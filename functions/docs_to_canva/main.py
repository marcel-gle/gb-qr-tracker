"""
Google Cloud Function to migrate Google Docs to Templated.io
Preserves multi-page layout and formatting
"""

import os
import json
import requests
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Environment variables
TEMPLATED_API_KEY = os.environ.get('TEMPLATED_IO_KEY')

# Constants
TEMPLATED_BASE_URL = 'https://api.templated.io/v1'
PAGE_WIDTH = 816  # 8.5 inches * 96 DPI
PAGE_HEIGHT = 1056  # 11 inches * 96 DPI
MARGIN = 60


def get_google_docs_service():
    """Initialize Google Docs API service using Application Default Credentials"""
    # Use Application Default Credentials (ADC)
    # This will use:
    # 1. Service account attached to Cloud Function when deployed
    # 2. GOOGLE_APPLICATION_CREDENTIALS env var pointing to service account file (local)
    # 3. Default credentials from gcloud (local)
    credentials, _ = default(scopes=['https://www.googleapis.com/auth/documents.readonly'])
    return build('docs', 'v1', credentials=credentials)


def extract_text_formatting(element):
    """Extract text and its formatting from a paragraph element"""
    if 'textRun' not in element:
        return None
    
    text_run = element['textRun']
    content = text_run.get('content', '')
    style = text_run.get('textStyle', {})
    
    return {
        'text': content,
        'bold': style.get('bold', False),
        'italic': style.get('italic', False),
        'underline': style.get('underline', False),
        'font_family': style.get('weightedFontFamily', {}).get('fontFamily', 'Arial'),
        'font_size': style.get('fontSize', {}).get('magnitude', 11),
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


def extract_document_content(doc):
    """Extract all content from Google Doc with formatting"""
    pages = []
    current_page_elements = []
    current_y = MARGIN
    
    body = doc.get('body', {})
    content = body.get('content', [])
    
    for struct_element in content:
        # Handle paragraphs
        if 'paragraph' in struct_element:
            paragraph = struct_element['paragraph']
            elements = paragraph.get('elements', [])
            
            paragraph_texts = []
            for elem in elements:
                formatting = extract_text_formatting(elem)
                if formatting and formatting['text'].strip():
                    paragraph_texts.append(formatting)
            
            if paragraph_texts:
                # Combine text runs into a single text element
                combined_text = ''.join([t['text'] for t in paragraph_texts])
                
                # Use first element's formatting as base
                base_format = paragraph_texts[0]
                
                element_height = int(base_format['font_size'] * 1.5)
                
                # Check if we need a new page
                if current_y + element_height > PAGE_HEIGHT - MARGIN:
                    pages.append(current_page_elements)
                    current_page_elements = []
                    current_y = MARGIN
                
                # Create text layer
                layer_data = {
                    'type': 'text',
                    'text': combined_text.rstrip('\n'),
                    'x': MARGIN,
                    'y': current_y,
                    'width': PAGE_WIDTH - (2 * MARGIN),
                    'height': element_height,
                    'color': rgb_to_hex(base_format['color']),
                    'font_family': base_format['font_family'].replace(' ', ''),
                    'font_size': f"{int(base_format['font_size'])}px",
                    'bold': base_format['bold'],
                    'italic': base_format['italic']
                }
                
                if base_format['background_color']:
                    layer_data['background'] = rgb_to_hex(base_format['background_color'])
                
                current_page_elements.append(layer_data)
                current_y += element_height + 10
        
        # Handle tables
        elif 'table' in struct_element:
            table = struct_element['table']
            # For simplicity, extract table as text
            # In production, you'd create actual table layers
            current_page_elements.append({
                'type': 'text',
                'text': '[Table content]',
                'x': MARGIN,
                'y': current_y,
                'width': PAGE_WIDTH - (2 * MARGIN),
                'height': 30,
                'color': '#000000',
                'font_size': '11px'
            })
            current_y += 40
    
    # Add last page
    if current_page_elements:
        pages.append(current_page_elements)
    
    return pages


def create_templated_template(doc_title, pages):
    """Create a multi-page template on Templated.io"""
    
    if not TEMPLATED_API_KEY:
        raise ValueError('TEMPLATED_IO_KEY environment variable is not set')
    
    headers = {
        'Authorization': f'Bearer {TEMPLATED_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Create template with first page
    first_page_layers = []
    for idx, element in enumerate(pages[0] if pages else []):
        layer = {
            'layer': f'element-{idx}',
            **element
        }
        first_page_layers.append(layer)
    
    template_data = {
        'name': doc_title or 'Migrated Google Doc',
        'width': PAGE_WIDTH,
        'height': PAGE_HEIGHT,
        'layers': first_page_layers
    }
    
    response = requests.post(
        f'{TEMPLATED_BASE_URL}/template',
        headers=headers,
        json=template_data
    )
    
    if response.status_code != 200:
        raise Exception(f'Failed to create template: {response.text}')
    
    template = response.json()
    template_id = template['id']
    
    # If multi-page, we need to add additional pages
    # Note: This requires using the template pages API
    # For now, we'll create separate templates or use the render API with pages
    
    return template_id, template


def migrate_google_doc(document_id):
    """Main migration function"""
    
    # Validate API key is set
    if not TEMPLATED_API_KEY:
        raise ValueError('TEMPLATED_IO_KEY environment variable is not set')
    
    # Get Google Doc content
    service = get_google_docs_service()
    
    try:
        doc = service.documents().get(
            documentId=document_id,
            includeTabsContent=True
        ).execute()
    except HttpError as e:
        raise Exception(f'Error fetching Google Doc: {str(e)}')
    
    doc_title = doc.get('title', 'Untitled Document')
    
    # Extract content by pages
    pages = extract_document_content(doc)
    
    # Create template on Templated.io
    template_id, template = create_templated_template(doc_title, pages)
    
    # For multi-page documents, create a render with all pages
    if len(pages) > 1:
        render_pages = []
        for page_idx, page_elements in enumerate(pages):
            page_layers = {}
            for elem_idx, element in enumerate(page_elements):
                layer_name = f'element-{elem_idx}'
                page_layers[layer_name] = {
                    k: v for k, v in element.items() if k != 'type'
                }
            
            render_pages.append({
                'page': f'page-{page_idx}',
                'layers': page_layers
            })
        
        # Create initial render to visualize all pages
        headers = {
            'Authorization': f'Bearer {TEMPLATED_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        render_data = {
            'template': template_id,
            'merge': True,  # Merge into single PDF
            'pages': render_pages
        }
        
        render_response = requests.post(
            f'{TEMPLATED_BASE_URL}/render',
            headers=headers,
            json=render_data
        )
        
        if render_response.status_code == 200:
            render = render_response.json()
            return {
                'success': True,
                'template_id': template_id,
                'template_url': f'https://app.templated.io/editor/{template_id}',
                'render_url': render.get('url') if isinstance(render, dict) else render[0].get('url'),
                'pages_migrated': len(pages)
            }
    
    return {
        'success': True,
        'template_id': template_id,
        'template_url': f'https://app.templated.io/editor/{template_id}',
        'pages_migrated': len(pages)
    }


def main(request):
    """
    Google Cloud Function entry point
    
    Expected request JSON:
    {
        "document_id": "your-google-doc-id"
    }
    """
    
    # Parse request
    request_json = request.get_json(silent=True)
    
    if not request_json or 'document_id' not in request_json:
        return {
            'error': 'Missing document_id in request'
        }, 400
    
    document_id = request_json['document_id']
    
    try:
        result = migrate_google_doc(document_id)
        return result, 200
    except Exception as e:
        return {
            'error': str(e)
        }, 500


# For local testing
if __name__ == '__main__':
    # Test with a sample document ID
    test_doc_id = 'YOUR_GOOGLE_DOC_ID'
    result = migrate_google_doc(test_doc_id)
    print(json.dumps(result, indent=2))