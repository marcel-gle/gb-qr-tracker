"""
Google Cloud Function to migrate Google Docs to Templated.io
IMPROVED VERSION - Better formatting preservation and multi-page support
"""

import os
import json
import logging
import requests
import functions_framework
from flask import Request
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

# Environment variables
TEMPLATED_API_KEY = os.environ.get('TEMPLATED_IO_KEY')

# Constants
TEMPLATED_BASE_URL = 'https://api.templated.io/v1'
PAGE_WIDTH = 764  # 8.5 inches * 96 DPI
PAGE_HEIGHT = 1080  # 11 inches * 96 DPI
MARGIN = 30

# IMPROVED: More accurate heading size multipliers
HEADING_MULTIPLIERS = {
    'HEADING_1': 1.4,  # Changed from 1.8
    'HEADING_2': 1.2,  # Changed from 1.5
    'HEADING_3': 1.1,  # Changed from 1.2
    'HEADING_4': 1.05,
    'HEADING_5': 1.0,
    'HEADING_6': 1.0
}


def get_google_docs_service():
    """Initialize Google Docs API service using Application Default Credentials"""
    credentials, project = default(scopes=['https://www.googleapis.com/auth/documents.readonly'])
    logger.info(f"Using credentials for project: {project}")
    service = build('docs', 'v1', credentials=credentials, cache_discovery=False)
    return service


def extract_text_formatting(element):
    """Extract text and its formatting from a paragraph element"""
    if 'textRun' not in element:
        if 'pageBreak' in element:
            return {'text': '', 'is_page_break': True}
        if 'horizontalRule' in element:
            return {'text': '', 'is_rule': True}
        return None
    
    text_run = element['textRun']
    content = text_run.get('content', '')
    style = text_run.get('textStyle', {})
    
    # Get font family
    font_family = 'Arial'
    if 'weightedFontFamily' in style:
        font_family = style['weightedFontFamily'].get('fontFamily', 'Arial')
    elif 'fontFamily' in style:
        font_family = style['fontFamily']
    
    # Get font size
    font_size = 11
    if 'fontSize' in style:
        font_size_obj = style['fontSize']
        if isinstance(font_size_obj, dict):
            font_size = font_size_obj.get('magnitude', 11)
        elif isinstance(font_size_obj, (int, float)):
            font_size = font_size_obj
    
    return {
        'text': content,
        'bold': style.get('bold', False),
        'italic': style.get('italic', False),
        'underline': style.get('underline', False),
        'font_family': font_family,
        'font_size': font_size,
        'color': style.get('foregroundColor', {}).get('color', {}).get('rgbColor', {}),
        'background_color': style.get('backgroundColor', {}).get('color', {}).get('rgbColor', {})
    }


def rgb_to_hex(rgb_color):
    """Convert Google Docs RGB color to hex"""
    if not rgb_color:
        return '#000000'
    
    r = int(rgb_color.get('red', 0) * 255)
    g = int(rgb_color.get('green', 0) * 255)
    b = int(rgb_color.get('blue', 0) * 255)
    return f'#{r:02x}{g:02x}{b:02x}'


def estimate_text_width(text, font_size, bold=False):
    """
    IMPROVED: Better text width estimation
    Average character width varies by formatting
    """
    # More accurate multipliers based on common fonts
    if bold:
        avg_char_width = font_size * 0.65  # Bold is slightly wider
    else:
        avg_char_width = font_size * 0.55  # Regular text
    
    return len(text) * avg_char_width


def create_formatted_text_segments(paragraph_texts, named_style_type):
    """
    IMPROVED: Combine text runs with same formatting into segments
    This reduces the number of layers created
    """
    if not paragraph_texts:
        return []
    
    segments = []
    current_segment = None
    
    for text_run in paragraph_texts:
        # Skip empty text
        if not text_run.get('text') or not text_run['text'].strip():
            continue
        
        # Get formatting signature
        formatting = {
            'bold': text_run.get('bold', False),
            'italic': text_run.get('italic', False),
            'underline': text_run.get('underline', False),
            'font_family': text_run.get('font_family', 'Arial'),
            'font_size': text_run.get('font_size', 11),
            'color': text_run.get('color', {}),
            'background_color': text_run.get('background_color', {})
        }
        
        # Apply heading size multiplier
        base_font_size = formatting['font_size']
        for heading_type, multiplier in HEADING_MULTIPLIERS.items():
            if heading_type in named_style_type:
                formatting['font_size'] = int(base_font_size * multiplier)
                # Make headings bold by default if not already
                if not formatting['bold']:
                    formatting['bold'] = True
                break
        
        # Check if we can combine with current segment
        if current_segment and current_segment['formatting'] == formatting:
            # Same formatting - append text
            current_segment['text'] += text_run['text']
        else:
            # Different formatting - start new segment
            if current_segment:
                segments.append(current_segment)
            current_segment = {
                'text': text_run['text'],
                'formatting': formatting
            }
    
    # Add last segment
    if current_segment:
        segments.append(current_segment)
    
    return segments


def wrap_text_segments(segments, max_width, line_height_multiplier=1.5):
    """
    IMPROVED: Wrap text segments into lines while preserving formatting
    Returns array of lines, where each line has multiple formatted segments
    """
    lines = []
    current_line = []
    current_line_width = 0
    
    for segment in segments:
        text = segment['text']
        formatting = segment['formatting']
        font_size = formatting['font_size']
        
        # Split by explicit line breaks first
        text_parts = text.split('\n')
        
        for part_idx, part in enumerate(text_parts):
            if not part:
                # Empty part from line break - finish current line
                if current_line:
                    lines.append({
                        'segments': current_line,
                        'height': max([seg['formatting']['font_size'] for seg in current_line]) * line_height_multiplier
                    })
                    current_line = []
                    current_line_width = 0
                continue
            
            # Split into words
            words = part.split(' ')
            
            for word_idx, word in enumerate(words):
                # Add space before word (except first word)
                word_with_space = word if word_idx == 0 and not current_line else ' ' + word
                word_width = estimate_text_width(word_with_space, font_size, formatting.get('bold', False))
                
                # Check if word fits on current line
                if current_line_width + word_width <= max_width or not current_line:
                    # Fits - add to current line
                    if current_line and current_line[-1]['formatting'] == formatting:
                        # Same formatting as last segment - combine
                        current_line[-1]['text'] += word_with_space
                        current_line[-1]['width'] += word_width
                    else:
                        # Different formatting - new segment
                        current_line.append({
                            'text': word_with_space,
                            'formatting': formatting,
                            'width': word_width
                        })
                    current_line_width += word_width
                else:
                    # Doesn't fit - start new line
                    if current_line:
                        lines.append({
                            'segments': current_line,
                            'height': max([seg['formatting']['font_size'] for seg in current_line]) * line_height_multiplier
                        })
                    current_line = [{
                        'text': word.lstrip(),  # Remove leading space for new line
                        'formatting': formatting,
                        'width': estimate_text_width(word.lstrip(), font_size, formatting.get('bold', False))
                    }]
                    current_line_width = current_line[0]['width']
            
            # Handle explicit line break (if not last part)
            if part_idx < len(text_parts) - 1:
                if current_line:
                    lines.append({
                        'segments': current_line,
                        'height': max([seg['formatting']['font_size'] for seg in current_line]) * line_height_multiplier
                    })
                    current_line = []
                    current_line_width = 0
    
    # Add last line
    if current_line:
        lines.append({
            'segments': current_line,
            'height': max([seg['formatting']['font_size'] for seg in current_line]) * line_height_multiplier
        })
    
    return lines


def extract_document_content(doc):
    """Extract all content from Google Doc with formatting"""
    pages = []
    current_page_elements = []
    current_y = MARGIN
    
    body = doc.get('body', {})
    content = body.get('content', [])
    
    logger.info(f"Extracting content from document. Found {len(content)} top-level elements")
    
    if not content:
        logger.warning("Document body has no content elements")
        return pages
    
    for idx, struct_element in enumerate(content):
        # Handle paragraphs
        if 'paragraph' in struct_element:
            paragraph = struct_element['paragraph']
            elements = paragraph.get('elements', [])
            paragraph_style = paragraph.get('paragraphStyle', {})
            
            logger.info(f"--- Processing Element {idx + 1}/{len(content)}: PARAGRAPH ---")
            
            # Extract paragraph-level formatting
            alignment = paragraph_style.get('alignment', 'START').lower()
            line_spacing = paragraph_style.get('lineSpacing', {})
            space_before = paragraph_style.get('spaceAbove', {}).get('magnitude', 0) or 0
            space_after = paragraph_style.get('spaceBelow', {}).get('magnitude', 0) or 0
            indent_first_line = paragraph_style.get('indentFirstLine', {}).get('magnitude', 0) or 0
            indent_start = paragraph_style.get('indentStart', {}).get('magnitude', 0) or 0
            named_style_type = paragraph_style.get('namedStyleType', 'NORMAL_TEXT')
            
            # Extract all text runs
            paragraph_texts = []
            for elem in elements:
                formatting = extract_text_formatting(elem)
                if formatting:
                    if formatting.get('is_page_break'):
                        # Force page break
                        if current_page_elements:
                            pages.append(current_page_elements)
                            current_page_elements = []
                            current_y = MARGIN
                        continue
                    paragraph_texts.append(formatting)
            
            if not paragraph_texts:
                continue
            
            # IMPROVED: Combine text runs into segments
            segments = create_formatted_text_segments(paragraph_texts, named_style_type)
            
            if not segments:
                continue
            
            # Calculate line spacing
            line_height_multiplier = 1.5
            if line_spacing:
                if 'magnitude' in line_spacing:
                    # Line spacing is often in percentage (100 = single, 150 = 1.5x)
                    spacing_value = line_spacing.get('magnitude', 150)
                    if spacing_value > 10:  # Likely percentage
                        line_height_multiplier = spacing_value / 100
                    else:
                        line_height_multiplier = spacing_value
            
            # Calculate available text width
            text_width = PAGE_WIDTH - (2 * MARGIN) - int(indent_start)
            
            # Add spacing before paragraph
            if space_before > 0:
                current_y += int(space_before * 1.33)  # Convert pt to px (rough)
            
            # IMPROVED: Wrap text segments into lines
            lines = wrap_text_segments(segments, text_width, line_height_multiplier)
            
            logger.info(f"  Created {len(lines)} visual lines from {len(segments)} text segments")
            
            # Create layers for each line
            for line_idx, line in enumerate(lines):
                line_height = int(line['height'])
                
                # Check if we need a new page
                if current_y + line_height > PAGE_HEIGHT - MARGIN:
                    logger.info(f"  ⚠️  Page break at line {line_idx + 1}")
                    if current_page_elements:
                        pages.append(current_page_elements)
                    current_page_elements = []
                    current_y = MARGIN
                
                # Calculate x position based on alignment
                base_x = MARGIN + int(indent_start)
                if line_idx == 0:
                    base_x += int(indent_first_line)
                
                # For center/right alignment, we'd need to calculate total line width
                # For simplicity, keeping left alignment logic here
                current_x = base_x
                
                # Create a layer for each formatted segment in the line
                for seg_idx, segment in enumerate(line['segments']):
                    formatting = segment['formatting']
                    
                    layer_data = {
                        'type': 'text',
                        'text': segment['text'],
                        'x': int(current_x),
                        'y': int(current_y),
                        'width': int(segment['width']),
                        'height': line_height,
                        'color': rgb_to_hex(formatting.get('color')),
                        'font_family': formatting.get('font_family', 'Arial').replace(' ', ''),
                        'font_size': f"{int(formatting['font_size'])}px",
                        'bold': formatting.get('bold', False),
                        'italic': formatting.get('italic', False),
                        'underline': formatting.get('underline', False)
                    }
                    
                    if formatting.get('background_color'):
                        layer_data['background'] = rgb_to_hex(formatting['background_color'])
                    
                    current_page_elements.append(layer_data)
                    current_x += segment['width']
                
                current_y += line_height
            
            # Add spacing after paragraph
            if space_after > 0:
                current_y += int(space_after * 1.33)
            else:
                current_y += 5  # Reduced default spacing
            
            logger.info(f"  ✓ Paragraph processed at Y: {current_y}px")
        
        # Handle tables (simplified)
        elif 'table' in struct_element:
            logger.info(f"Found table at index {idx} - creating placeholder")
            if current_y + 40 > PAGE_HEIGHT - MARGIN:
                if current_page_elements:
                    pages.append(current_page_elements)
                current_page_elements = []
                current_y = MARGIN
            
            current_page_elements.append({
                'type': 'text',
                'text': '[Table content - not fully supported yet]',
                'x': MARGIN,
                'y': int(current_y),
                'width': PAGE_WIDTH - (2 * MARGIN),
                'height': 30,
                'color': '#666666',
                'font_size': '11px',
                'italic': True
            })
            current_y += 40
    
    # Add last page
    if current_page_elements:
        pages.append(current_page_elements)
    
    logger.info(f"=== Extraction Complete: {len(pages)} pages ===")
    
    return pages


def create_templated_template(doc_title, pages):
    """
    IMPROVED: Create template with first page, then create proper multi-page document
    """
    logger.info(f"=== Creating Template on Templated.io ===")
    
    if not TEMPLATED_API_KEY:
        raise ValueError('TEMPLATED_IO_KEY environment variable is not set')
    
    headers = {
        'Authorization': f'Bearer {TEMPLATED_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Create template with first page
    first_page_layers = []
    if pages and len(pages) > 0:
        for idx, element in enumerate(pages[0]):
            layer = {
                'layer': f'page1-element-{idx}',
                **element
            }
            first_page_layers.append(layer)
    
    template_data = {
        'name': doc_title or 'Migrated Google Doc',
        'width': PAGE_WIDTH,
        'height': PAGE_HEIGHT,
        'layers': first_page_layers
    }
    
    logger.info(f"Creating template: {template_data['name']} ({len(first_page_layers)} layers)")
    
    response = requests.post(
        f'{TEMPLATED_BASE_URL}/template',
        headers=headers,
        json=template_data
    )
    
    if response.status_code != 200:
        logger.error(f"Template creation failed: {response.text}")
        raise Exception(f'Failed to create template: {response.text}')
    
    template = response.json()
    template_id = template['id']
    
    logger.info(f"✓ Template created: {template_id}")
    
    return template_id, template


def create_multi_page_render(template_id, pages, doc_title):
    """
    IMPROVED: Create multi-page render that properly shows all pages
    """
    if len(pages) <= 1:
        return None
    
    logger.info(f"=== Creating Multi-Page Render ({len(pages)} pages) ===")
    
    headers = {
        'Authorization': f'Bearer {TEMPLATED_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Build pages array for render
    render_pages = []
    
    for page_idx, page_elements in enumerate(pages):
        logger.info(f"  Building page {page_idx + 1} with {len(page_elements)} elements")
        
        # Create a layers object for this page
        page_layers = {}
        
        for elem_idx, element in enumerate(page_elements):
            layer_name = f'page{page_idx + 1}-element-{elem_idx}'
            
            # Remove 'type' from layer data as it's not needed in modifications
            layer_data = {k: v for k, v in element.items() if k != 'type'}
            page_layers[layer_name] = layer_data
        
        render_pages.append({
            'layers': page_layers
        })
    
    render_data = {
        'template': template_id,
        'merge': True,  # Merge into single PDF
        'pages': render_pages
    }
    
    logger.info(f"Posting render request...")
    
    render_response = requests.post(
        f'{TEMPLATED_BASE_URL}/render',
        headers=headers,
        json=render_data
    )
    
    logger.info(f"Render response status: {render_response.status_code}")
    
    if render_response.status_code == 200:
        render = render_response.json()
        # Handle both single render and array response
        if isinstance(render, list):
            render_url = render[0].get('url') if render else None
        else:
            render_url = render.get('url')
        
        logger.info(f"✓ Multi-page render created: {render_url}")
        return render_url
    else:
        logger.error(f"Render failed: {render_response.text}")
        return None


def migrate_google_doc(document_id):
    """Main migration function"""
    
    if not TEMPLATED_API_KEY:
        raise ValueError('TEMPLATED_IO_KEY environment variable is not set')
    
    # Get Google Doc content
    service = get_google_docs_service()
    
    try:
        logger.info(f"=== Fetching Google Doc: {document_id} ===")
        doc = service.documents().get(documentId=document_id).execute()
    except HttpError as e:
        logger.error(f"Error fetching Google Doc: {e}")
        raise Exception(f'Error fetching Google Doc: {e}')
    
    doc_title = doc.get('title', 'Untitled Document')
    logger.info(f"Document title: {doc_title}")
    
    # Extract content by pages
    pages = extract_document_content(doc)
    
    if not pages:
        logger.warning("No content extracted")
        return {
            'success': False,
            'error': 'No content could be extracted from document'
        }
    
    # Create template
    template_id, template = create_templated_template(doc_title, pages)
    
    result = {
        'success': True,
        'template_id': template_id,
        'template_url': f'https://app.templated.io/editor/{template_id}',
        'pages_migrated': len(pages)
    }
    
    # Create multi-page render if needed
    if len(pages) > 1:
        render_url = create_multi_page_render(template_id, pages, doc_title)
        if render_url:
            result['render_url'] = render_url
            result['pdf_url'] = render_url  # Add explicit PDF URL
    
    logger.info(f"=== Migration Complete ===")
    logger.info(f"Result: {json.dumps(result, indent=2)}")
    
    return result


@functions_framework.http
def migrate_document(request: Request):
    """Google Cloud Function entry point"""
    
    request_json = request.get_json(silent=True)
    
    if not request_json or 'document_id' not in request_json:
        return {'error': 'Missing document_id in request'}, 400
    
    document_id = request_json['document_id']
    
    try:
        result = migrate_google_doc(document_id)
        return result, 200
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return {'error': str(e)}, 500


if __name__ == '__main__':
    test_doc_id = 'YOUR_GOOGLE_DOC_ID'
    result = migrate_google_doc(test_doc_id)
    print(json.dumps(result, indent=2))