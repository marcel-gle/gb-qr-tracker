#!/usr/bin/env python3
"""
Generate print-ready letters (A4, DIN 5008 form B, window envelope DL) from a recipient CSV.

Each letter gets: letterhead, return line, address window, personal QR code + link,
the general copy from letter_template.md and the individual paragraph from the CSV.

Usage:
  python generate_letters.py recipients.csv                 # all letters -> output/letters_all.pdf
  python generate_letters.py recipients.csv --limit 5       # first 5 only (test print)
  python generate_letters.py recipients.csv --single        # additionally one PDF per letter
  python generate_letters.py recipients.csv --rows 3,17     # specific CSV rows (1-based, without header)

Outputs (in --out, default ./output):
  letters_all.pdf   combined PDF for printing (padded to even pages if duplex_padding)
  single/*.pdf      one file per letter (with --single)
  report.csv        one line per recipient: status, pages, font size, QR slug, reason if skipped
  qr_links.csv      slug -> URL mapping to create the tracking links in Rocket Letter
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

import yaml
from pypdf import PdfReader, PdfWriter
from reportlab.graphics import renderPDF
from reportlab.graphics.barcode.qr import QrCodeWidget
from reportlab.graphics.shapes import Drawing
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.lib.utils import simpleSplit
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (BaseDocTemplate, Flowable, Frame, Image, KeepTogether, ListFlowable, ListItem,
                                NextPageTemplate, PageTemplate, Paragraph, Spacer, Table, TableStyle)

BASE_DIR = Path(__file__).resolve().parent
PAGE_W, PAGE_H = A4


# ----------------------------------------------------------------------------
# Config, fonts, template
# ----------------------------------------------------------------------------
def load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def register_fonts(cfg: dict) -> dict[str, str]:
    """Register the brand TTFs. Returns role -> font name; falls back to Helvetica for missing files."""
    fallback = {"heading_light": "Helvetica", "heading_regular": "Helvetica", "heading_semibold": "Helvetica-Bold",
                "heading_bold": "Helvetica-Bold", "body_regular": "Helvetica", "body_bold": "Helvetica-Bold",
                "body_heavy": "Helvetica-Bold", "body_italic": "Helvetica-Oblique"}
    fonts = {}
    for role, rel in (cfg.get("brand", {}).get("fonts") or {}).items():
        path = Path(rel) if Path(rel).is_absolute() else BASE_DIR / rel
        if rel and path.exists():
            name = f"Brand-{role}"
            pdfmetrics.registerFont(TTFont(name, str(path)))
            fonts[role] = name
        else:
            fonts[role] = fallback.get(role, "Helvetica")
    for role, fb in fallback.items():
        fonts.setdefault(role, fb)
    pdfmetrics.registerFontFamily("BrandBody", normal=fonts["body_regular"], bold=fonts["body_bold"],
                                  italic=fonts["body_italic"], boldItalic=fonts["body_bold"])
    return fonts


@dataclass
class Template:
    headline: str
    greeting: str
    greeting_without_name: str
    body: str


def load_template(path: Path) -> Template:
    raw = path.read_text(encoding="utf-8")
    m = re.match(r"^---\n(.*?)\n---\n(.*)$", raw, re.S)
    if not m:
        sys.exit(f"{path}: expected a '---' header block followed by the letter body.")
    header = "\n".join(l for l in m.group(1).splitlines() if not l.lstrip().startswith("#"))
    meta = yaml.safe_load(header) or {}
    return Template(meta.get("headline") or "", meta.get("greeting", "Hallo {first_name},"),
                    meta.get("greeting_without_name", "Hallo zusammen,"), m.group(2).strip())


# ----------------------------------------------------------------------------
# Recipient data
# ----------------------------------------------------------------------------
@dataclass
class Recipient:
    row: int
    data: dict
    company: str = ""
    agency_name: str = ""
    salutation: str = ""
    first_name: str = ""
    last_name: str = ""
    street: str = ""
    postcode: str = ""
    city: str = ""
    co_line: str = ""
    paragraph: str = ""
    domain: str = ""
    slug: str = ""
    problems: list[str] = field(default_factory=list)


def pick(record: dict, columns: list[str]) -> str:
    for c in columns:
        v = (record.get(c) or "").strip()
        if v:
            return v
    return ""


def read_recipients(path: Path, cfg: dict) -> list[Recipient]:
    raw = path.read_text(encoding="utf-8-sig")
    dialect = csv.Sniffer().sniff(raw[:20000], delimiters=",;\t")
    cols = cfg["columns"]
    out = []
    for i, rec in enumerate(csv.DictReader(io.StringIO(raw), dialect=dialect), 1):
        r = Recipient(row=i, data=rec)
        # company line: first configured column that fits the window width
        candidates = [(rec.get(c) or "").strip() for c in cols["company"]]
        candidates = [c for c in candidates if c]
        r.company = next((c for c in candidates if len(c) <= cols["company_max_chars"]), candidates[0] if candidates else "")
        r.agency_name = pick(rec, cols["agency_name"])
        r.salutation = pick(rec, cols["salutation"])
        r.first_name = pick(rec, cols["first_name"])
        r.last_name = pick(rec, cols["last_name"])
        r.street = f"{pick(rec, cols['street'])} {pick(rec, cols['house_number'])}".strip()
        r.postcode = pick(rec, cols["postcode"])
        r.city = pick(rec, cols["city"])
        m = re.search(r"\bc/o\s+[^,·;]+", pick(rec, cols["full_address"]), re.I)
        r.co_line = re.sub(r"\s+", " ", m.group(0)).strip(" -:") if m else ""
        # drop the street if the c/o fragment runs into it ("c/o WeWork ... Kemperplatz 1")
        street_only = pick(rec, cols["street"])
        if r.co_line and street_only and street_only in r.co_line:
            r.co_line = r.co_line[: r.co_line.index(street_only)].strip(" -:,·")
        r.paragraph = re.sub(r"\s+", " ", pick(rec, cols["paragraph"]))
        r.domain = pick(rec, cols["domain"])
        if not (r.street and r.postcode and r.city):
            r.problems.append("address incomplete")
        if not re.fullmatch(r"\d{5}", r.postcode or ""):
            r.problems.append(f"postcode not German: '{r.postcode}'")
        if not r.company and not r.last_name:
            r.problems.append("no company and no name")
        out.append(r)
    return out


def slugify(text: str) -> str:
    text = text.lower().replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def assign_slugs(recipients: list[Recipient], cfg: dict) -> None:
    """Use slug column if present, else last name; disambiguate collisions with first name, then a counter."""
    col = cfg["qr"].get("slug_column")
    used: set[str] = set()
    base_counts: dict[str, int] = {}
    for r in recipients:
        base = slugify(r.last_name or r.agency_name or r.domain)
        base_counts[base] = base_counts.get(base, 0) + 1
    for r in recipients:
        given = (r.data.get(col) or "").strip() if col else ""
        if given:
            slug = slugify(given)
        else:
            base = slugify(r.last_name or r.agency_name or r.domain) or f"r{r.row}"
            slug = base if base_counts[base] == 1 else f"{base}-{slugify(r.first_name) or r.row}"
        candidate, n = slug, 2
        while candidate in used:
            candidate, n = f"{slug}-{n}", n + 1
        used.add(candidate)
        r.slug = candidate


# ----------------------------------------------------------------------------
# Rendering
# ----------------------------------------------------------------------------
def fill(text: str, r: Recipient, extra: dict | None = None) -> str:
    """Replace {placeholders} with XML-escaped recipient values; keep markup in the template."""
    values = {"first_name": r.first_name, "last_name": r.last_name, "salutation": r.salutation,
              "agency_name": r.agency_name or r.company, "paragraph": r.paragraph,
              "qr_slug": r.slug}
    values.update(extra or {})
    text = re.sub(r"\{(\w+)\}", lambda m: escape(values.get(m.group(1), m.group(0))), text)
    # keep numbers and units together: "60 %", "96.000 €", "7 bis 14 Tagen" -> no line break before % / €
    text = re.sub(r"(\d) (%|€|Mio\.|Tage)", "\\1\u00a0\\2", text)
    # keep phone numbers on one line: "0176 4554 0806"
    return re.sub(r"(?<=\d) (?=\d)", "\u00a0", text)


def md_inline(text: str, bold_font: str, size: float, highlight_font: str = "", highlight_color: str = "",
              label_scale: float = 0.9) -> str:
    """**bold** -> label in the heading font (Sora semibold), slightly smaller to match the serif x-height.
    ==highlight== -> body font in bold (700), plus a light accent background if highlight_color is set."""
    text = re.sub(r"\*\*(.+?)\*\*", rf'<font name="{bold_font}" size="{size * label_scale:.2f}">\1</font>', text)
    # *italic* -> <i> (resolved via the BrandBody font family registered in register_fonts)
    text = re.sub(r"(?<!\*)\*(?!\s)([^*]+?)\*(?!\*)", r"<i>\1</i>", text)
    attrs = (f' fontName="{highlight_font}"' if highlight_font else "") + \
            (f' backColor="{highlight_color}"' if highlight_color else "")
    return re.sub(r"==(.+?)==", rf"<span{attrs}>\1</span>" if attrs else r"\1", text)


def load_logo(path: Path):
    """Return a callable draw(canvas, x, y, height) for PNG/JPG/SVG logos, plus the aspect ratio."""
    if path.suffix.lower() == ".svg":
        from svglib.svglib import svg2rlg
        drawing = svg2rlg(str(path))
        ratio = drawing.width / drawing.height

        def draw(canv, x, y, h):
            scale = h / drawing.height
            canv.saveState()
            canv.translate(x, y)
            canv.scale(scale, scale)
            renderPDF.draw(drawing, canv, 0, 0)
            canv.restoreState()
        return draw, ratio
    img = Image(str(path))
    ratio = img.imageWidth / img.imageHeight

    def draw(canv, x, y, h):
        canv.drawImage(str(path), x, y, h * ratio, h, mask="auto")
    return draw, ratio


class AccentBar(Flowable):
    """Short rounded accent line above the headline (website style)."""
    def __init__(self, color, width=14 * mm, height=1.1 * mm, space_after=2.4 * mm):
        super().__init__()
        self.color, self.bar_w, self.bar_h, self.space_after = color, width, height, space_after

    def wrap(self, aw, ah):
        return aw, self.bar_h + self.space_after

    def draw(self):
        self.canv.setFillColor(self.color)
        self.canv.roundRect(0, self.space_after, self.bar_w, self.bar_h, self.bar_h / 2, stroke=0, fill=1)


class LinkBox(Flowable):
    """Rounded CTA box: bold question, short instruction, personal link in large bold type. Height follows content."""
    def __init__(self, title, label, link, fonts, colors_, width=82 * mm, max_size=13):
        super().__init__()
        self.title, self.label, self.link, self.f, self.c = title, label, link, fonts, colors_
        self.box_w, self.max_size = width, max_size
        self.pad_x, self.pad_top, self.pad_bottom = 5 * mm, 4.2 * mm, 4 * mm
        self.title_size, self.label_size = 8.6, 6.8
        self.title_lines = simpleSplit(title, fonts["heading_semibold"], self.title_size, self.box_w - 2 * self.pad_x)[:2]
        self.lead = self.title_size * 1.3
        self.link_size = max_size
        while pdfmetrics.stringWidth(link, fonts["heading_bold"], self.link_size) > self.box_w - 2 * self.pad_x \
                and self.link_size > 8:
            self.link_size -= 0.25
        # vertical rhythm, bottom-up
        self.y_link = self.pad_bottom
        self.y_label = self.y_link + self.link_size * 0.72 + 3.4 * mm
        self.y_title_last = self.y_label + self.label_size * 0.72 + 3.4 * mm
        self.box_h = self.y_title_last + (len(self.title_lines) - 1) * self.lead + self.title_size * 0.72 + self.pad_top

    def wrap(self, aw, ah):
        return self.box_w, self.box_h

    def draw(self):
        cv, px = self.canv, self.pad_x
        cv.setFillColor(self.c["light_bg"])
        cv.roundRect(0, 0, self.box_w, self.box_h, 3.5 * mm, stroke=0, fill=1)
        cv.setFillColor(self.c["accent"])
        cv.roundRect(0, 0, 1.5 * mm, self.box_h, 0.75 * mm, stroke=0, fill=1)
        cv.setFont(self.f["heading_semibold"], self.title_size)
        cv.setFillColor(self.c["heading"])
        for i, line in enumerate(self.title_lines):
            cv.drawString(px, self.y_title_last + (len(self.title_lines) - 1 - i) * self.lead, line)
        cv.setFont(self.f["heading_regular"], self.label_size)
        cv.setFillColor(self.c["muted"])
        cv.drawString(px, self.y_label, self.label)
        cv.setFont(self.f["heading_bold"], self.link_size)
        cv.setFillColor(self.c["accent_text"])
        cv.drawString(px, self.y_link, self.link)


class LetterBuilder:
    def __init__(self, cfg: dict, template: Template):
        self.cfg, self.tpl = cfg, template
        self.lay = cfg["layout"]
        self.f = register_fonts(cfg)
        c = cfg.get("brand", {}).get("colors", {})
        hx = lambda k, d: colors.HexColor(c.get(k, d))
        self.c_text, self.c_heading = hx("text", "#152842"), hx("heading", "#152842")
        self.c_accent, self.c_accent_text = hx("accent", "#00B0CA"), hx("accent_text", "#008198")
        self.c_muted, self.c_light, self.c_rule = hx("muted", "#62657C"), hx("light_bg", "#F5F8FB"), hx("rule", "#E9EDF1")
        logo = cfg["sender"].get("logo")
        self.logo = None
        if logo and (BASE_DIR / logo).exists():
            self.logo = load_logo(BASE_DIR / logo)

    # --- static page elements -------------------------------------------------
    def _letterhead(self, canv):
        s, lay = self.cfg["sender"], self.lay
        ml, mr = lay["margin_left"] * mm, PAGE_W - lay["margin_right"] * mm
        top = PAGE_H - 13 * mm
        h = s.get("logo_height_mm", 11) * mm
        if self.logo and s.get("logo_position", "left") == "right":
            # compact variant: icon + wordmark right-aligned, sender block below it
            draw, ratio = self.logo
            name_size = s.get("name_size", 11)
            sub_size = name_size * 0.62
            text_w = max(stringWidth(s.get("name", ""), self.f["heading_bold"], name_size),
                         stringWidth(s.get("subline", ""), self.f["heading_regular"], sub_size))
            x_icon = mr - text_w - 2.2 * mm - h * ratio
            draw(canv, x_icon, top - h, h)
            x_text, mid = x_icon + h * ratio + 2.2 * mm, top - h / 2
            canv.setFont(self.f["heading_bold"], name_size)
            canv.setFillColor(self.c_heading)
            canv.drawString(x_text, mid + 0.2 * mm, s.get("name", ""))
            canv.setFont(self.f["heading_regular"], sub_size)
            canv.setFillColor(self.c_muted)
            canv.drawString(x_text, mid - sub_size * 0.42 * mm, s.get("subline", ""))
            top -= h + 3.5 * mm
        elif self.logo:
            draw, ratio = self.logo
            draw(canv, ml, top - h, h)
            if s.get("logo_with_text", False):
                # icon-style logo: brand name + subline right of the icon, vertically centred
                x = ml + h * ratio + 3.2 * mm
                mid = top - h / 2
                name_size = s.get("name_size", 17)
                canv.setFont(self.f["heading_bold"], name_size)
                canv.setFillColor(self.c_heading)
                canv.drawString(x, mid + 0.4 * mm, s.get("name", ""))
                canv.setFont(self.f["heading_regular"], name_size * 0.44)
                canv.setFillColor(self.c_muted if s.get("subline_muted") else self.c_accent_text)
                canv.drawString(x, mid - name_size * 0.21 * mm, s.get("subline", ""))
        else:
            canv.setFont(self.f["heading_bold"], 17)
            canv.setFillColor(self.c_heading)
            canv.drawString(ml, top - 6.5 * mm, s.get("name", ""))
            canv.setFont(self.f["heading_regular"], 7.5)
            canv.setFillColor(self.c_accent_text)
            canv.drawString(ml, top - 10.5 * mm, s.get("subline", ""))
        # sender block top right
        y = top - 2.6 * mm
        block = s.get("block", [])
        bold_first = s.get("block_bold_first", True)
        for i, line in enumerate(block):
            first = i == 0 and bold_first
            canv.setFont(self.f["heading_semibold"] if first else self.f["heading_regular"], 7.5 if first else 7)
            canv.setFillColor(self.c_heading if first else self.c_muted)
            canv.drawRightString(mr, y, line)
            y -= 3.5 * mm

    def _address(self, canv, r: Recipient):
        s = self.cfg["sender"]
        shift = self.lay.get("address_shift_mm", 0) * mm   # positive = higher on the page
        x0, y_ret = 25 * mm, PAGE_H - 59.5 * mm + shift
        canv.setFillColor(self.c_muted)
        canv.setFont(self.f["heading_regular"], 6.3)
        canv.drawString(x0, y_ret, s.get("return_line", ""))
        canv.setStrokeColor(self.c_rule)
        canv.setLineWidth(0.5)
        canv.line(x0, y_ret - 1.3 * mm, x0 + 80 * mm, y_ret - 1.3 * mm)
        lines = [r.company]
        name = " ".join(x for x in (r.salutation, r.first_name, r.last_name) if x)
        if name:
            lines.append(name)
        if r.co_line:
            lines.append(r.co_line)
        lines += [r.street, f"{r.postcode} {r.city}"]
        lines = [l for l in lines if l][:6]
        size = 9.5 if len(lines) <= 5 else 8.8
        canv.setFillColor(self.c_text)
        y = PAGE_H - 62.7 * mm + shift - size * 1.05
        y_first = y
        font = self.f["heading_regular"]
        for line in lines:
            fs = size
            while pdfmetrics.stringWidth(line, font, fs) > 80 * mm and fs > 6.5:
                fs -= 0.25
            canv.setFont(font, fs)
            canv.drawString(x0, y, line)
            y -= size * 1.3
        note = s.get("address_note", "")   # e.g. "BITTE NICHT KNICKEN" below postcode + city
        if note:
            note_size = s.get("address_note_size", size)
            text = note.upper() if s.get("address_note_caps", True) else note
            # fixed position: measured from the first address line as if the address had 4 lines,
            # so a c/o line or a 6-line address never pushes the note out of the window
            ref_lines = s.get("address_note_ref_lines", 4)
            ny = y_first - ref_lines * 9.5 * 1.3 - 9.5 * 0.5 - s.get("address_note_offset_mm", 0) * mm
            ny = min(ny, y - size * 0.5)   # never overlap the last address line
            canv.setFont(self.f["heading_bold"], note_size)
            canv.drawString(x0, ny, text)
            if s.get("address_note_underline", False):
                canv.setStrokeColor(self.c_text)
                canv.setLineWidth(0.7)
                uy = ny - note_size * 0.18
                canv.line(x0, uy, x0 + pdfmetrics.stringWidth(text, self.f["heading_bold"], note_size), uy)

    def _qr_block(self, canv, r: Recipient):
        qr, lay = self.cfg["qr"], self.lay
        if qr.get("style", "box") == "plain":
            return self._qr_plain(canv, r)
        if qr.get("style") == "inline":   # drawn next to the signature instead
            return
        box_w, box_h = 46 * mm, 47 * mm
        bx = PAGE_W - lay["margin_right"] * mm - box_w
        by = PAGE_H - 41 * mm - box_h
        canv.setFillColor(self.c_light)
        canv.roundRect(bx, by, box_w, box_h, 3 * mm, stroke=0, fill=1)
        size_pt = qr.get("size_mm", 27) * mm
        url = qr["base_url"].rstrip("/") + "/" + r.slug
        widget = QrCodeWidget(url, barFillColor=self.c_heading, barBorder=2)
        b = widget.getBounds()
        d = Drawing(size_pt, size_pt, transform=[size_pt / (b[2] - b[0]), 0, 0, size_pt / (b[3] - b[1]), 0, 0])
        d.add(widget)
        qx, qy = bx + (box_w - size_pt) / 2, by + box_h - size_pt - 3.5 * mm
        canv.setFillColor(colors.white)
        canv.roundRect(qx - 1 * mm, qy - 1 * mm, size_pt + 2 * mm, size_pt + 2 * mm, 1.8 * mm, stroke=0, fill=1)
        renderPDF.draw(d, canv, qx, qy)
        cx = bx + box_w / 2
        canv.setFont(self.f["heading_semibold"], 7.6)
        canv.setFillColor(self.c_heading)
        canv.drawCentredString(cx, by + 8.6 * mm, qr.get("caption_title", ""))
        canv.setFont(self.f["heading_regular"], 6.8)
        canv.setFillColor(self.c_muted)
        canv.drawCentredString(cx, by + 4.6 * mm, qr.get("caption", ""))

    def _qr_plain(self, canv, r: Recipient):
        """Sober variant: bare QR code right-aligned, one grey caption line and the link below."""
        qr, lay = self.cfg["qr"], self.lay
        size_pt = qr.get("size_mm", 24) * mm
        right = PAGE_W - lay["margin_right"] * mm
        url = qr["base_url"].rstrip("/") + "/" + r.slug
        widget = QrCodeWidget(url, barFillColor=self.c_heading, barBorder=0)
        b = widget.getBounds()
        d = Drawing(size_pt, size_pt, transform=[size_pt / (b[2] - b[0]), 0, 0, size_pt / (b[3] - b[1]), 0, 0])
        d.add(widget)
        top = PAGE_H - qr.get("top_mm", 50) * mm
        renderPDF.draw(d, canv, right - size_pt, top - size_pt)
        y = top - size_pt - 4 * mm
        canv.setFont(self.f["heading_regular"], 6.8)
        canv.setFillColor(self.c_muted)
        if qr.get("caption"):
            canv.drawRightString(right, y, qr["caption"])
            y -= 3.3 * mm
        if qr.get("show_link", False):
            canv.setFillColor(self.c_heading)
            canv.drawRightString(right, y, qr.get("display_prefix", "") + r.slug)

    def _footer(self, canv):
        s, lay = self.cfg["sender"], self.lay
        ml, mr = lay["margin_left"] * mm, PAGE_W - lay["margin_right"] * mm
        canv.setStrokeColor(self.c_rule)
        canv.setLineWidth(0.5)
        canv.line(ml, 15.5 * mm, mr, 15.5 * mm)
        canv.setFont(self.f["heading_regular"], 6.2)
        canv.setFillColor(self.c_muted)
        fy = 12 * mm
        for line in s.get("footer", []):
            canv.drawString(ml, fy, line)
            fy -= 2.9 * mm

    def _marks(self, canv):
        if not self.lay.get("fold_marks", True):
            return
        canv.setLineWidth(0.4)
        canv.setStrokeColor(colors.HexColor("#AEB6C5"))
        for y_mm, length in ((105, 5), (210, 5), (148.5, 8)):
            canv.line(4 * mm, PAGE_H - y_mm * mm, (4 + length) * mm, PAGE_H - y_mm * mm)

    def _first_page(self, canv, doc):
        canv.saveState()
        self._letterhead(canv)
        self._address(canv, doc.recipient)
        self._qr_block(canv, doc.recipient)
        self._marks(canv)
        self._footer(canv)
        canv.restoreState()

    def _later_page(self, canv, doc):
        r: Recipient = doc.recipient
        canv.saveState()
        canv.setFont(self.f["heading_regular"], 7)
        canv.setFillColor(self.c_muted)
        canv.drawString(self.lay["margin_left"] * mm, PAGE_H - 12 * mm,
                        f"{r.agency_name or r.company} · Seite {canv.getPageNumber()}")
        self._footer(canv)
        canv.restoreState()

    # --- flowing content ---------------------------------------------------------
    def _body_uses_body_fonts(self) -> bool:
        """brand.body_font: "body" (the body_* fonts; "serif" kept as alias) or "heading" (Sora; alias "sans")."""
        return self.cfg.get("brand", {}).get("body_font", "body") in ("body", "serif")

    def _styles(self, size: float) -> dict:
        lay = self.lay
        lead = size * lay.get("leading_factor", 1.38)
        body_font = self.f["body_regular"] if self._body_uses_body_fonts() \
            else self.f["heading_regular"]
        base = ParagraphStyle("body", fontName=body_font, fontSize=size, leading=lead, alignment=TA_LEFT,
                              textColor=self.c_text, spaceAfter=lead * float(lay.get("paragraph_spacing", 0.45)))
        hs = lay.get("headline_size", 13)
        return {
            "body": base,
            "headline": ParagraphStyle("headline", parent=base, fontName=self.f["heading_semibold"], fontSize=hs,
                                       leading=hs * 1.28, textColor=self.c_heading, spaceAfter=lead * 0.75),
            "bullet": ParagraphStyle("bullet", parent=base,
                                     spaceAfter=lead * float(lay.get("bullet_spacing", 0.3))),
            "sig_name": ParagraphStyle("sig", parent=base, fontName=self.f["heading_semibold"], fontSize=size * 0.88,
                                       leading=size * 1.15, textColor=self.c_heading),
            "small": ParagraphStyle("small", parent=base, fontName=self.f["heading_regular"], fontSize=size * 0.75,
                                    leading=size * 1.0, textColor=self.c_muted),
        }

    def _qr_inline(self, r: Recipient, st: dict):
        """QR code with caption + personal link to its left, for the space next to the signature."""
        qr = self.cfg["qr"]
        size_pt = qr.get("size_mm", 20) * mm
        url = qr["base_url"].rstrip("/") + "/" + r.slug
        widget = QrCodeWidget(url, barFillColor=self.c_heading, barBorder=0)
        b = widget.getBounds()
        d = Drawing(size_pt, size_pt, transform=[size_pt / (b[2] - b[0]), 0, 0, size_pt / (b[3] - b[1]), 0, 0])
        d.add(widget)
        cap = ParagraphStyle("qrcap", fontName=self.f["heading_regular"], fontSize=6.8, leading=8.6,
                             textColor=self.c_muted, alignment=TA_RIGHT)
        link = ParagraphStyle("qrlink", parent=cap, fontName=self.f["heading_semibold"], fontSize=7.6,
                              textColor=self.c_heading)
        text_w = qr.get("caption_width_mm", 48) * mm
        title = ParagraphStyle("qrtitle", parent=cap, fontName=self.f["heading_semibold"], fontSize=7.8,
                               leading=9.6, textColor=self.c_heading)
        # keep the agency name on one line in the small caption
        keep = {"agency_name": (r.agency_name or r.company).replace(" ", "\u00a0")}
        cell = []
        if qr.get("inline_title"):
            cell += [Paragraph(fill(escape(qr["inline_title"]), r, keep), title), Spacer(1, 0.5 * mm)]
        # the personal link must stay on one line: shrink long slugs instead of wrapping them
        link_text = qr.get("display_prefix", "") + r.slug
        link_size = link.fontSize
        while stringWidth(link_text, link.fontName, link_size) > text_w - 3.2 * mm and link_size > 5.5:
            link_size -= 0.2
        link = ParagraphStyle("qrlink_fit", parent=link, fontSize=link_size, splitLongWords=0)
        cell += [Paragraph(fill(escape(qr.get("caption", "")), r, keep), cap), Spacer(1, 0.8 * mm),
                 Paragraph(escape(link_text), link)]
        t = Table([[cell, d]], colWidths=[text_w, size_pt])
        t.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (0, 0), 3 * mm),
                               ("RIGHTPADDING", (1, 0), (1, 0), 0),
                               ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                               ("VALIGN", (0, 0), (-1, -1), "BOTTOM")]))
        t._width = text_w + size_pt
        return t

    def _signatures(self, st: dict, r: Recipient) -> list:
        """Left: signature image(s) + one combined name line + caption. Right: personal tracking link."""
        sig, qr = self.cfg["signatures"], self.cfg["qr"]
        h = sig.get("image_height_mm", 12) * mm
        images = []
        for path in sig.get("images", []):
            if path and (BASE_DIR / path).exists():
                im = Image(str(BASE_DIR / path))
                im.drawWidth, im.drawHeight = im.imageWidth * h / im.imageHeight, h
                images.append(im)
        img_cell = Table([images], hAlign="LEFT") if images else Spacer(1, h)
        if images:
            img_cell.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 4 * mm),
                                          ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                                          ("VALIGN", (0, 0), (-1, -1), "BOTTOM")]))
        left = [img_cell, Spacer(1, 0.6 * mm), Paragraph(escape(sig.get("name", "")), st["sig_name"]),
                Spacer(1, 0.4 * mm), Paragraph(escape(sig.get("caption", "")), st["small"])]
        gap = self.lay.get("cta_gap_mm", 4) * mm
        if qr.get("style") == "inline":
            right = self._qr_inline(r, st)
            width = PAGE_W - (self.lay["margin_left"] + self.lay["margin_right"]) * mm
            t = Table([[left, right]], colWidths=[width - right._width, right._width], hAlign="LEFT")
            t.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                                   ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                                   ("VALIGN", (0, 0), (-1, -1), "BOTTOM")]))
            return [KeepTogether([Spacer(1, gap), t, Spacer(1, gap)])]
        if not sig.get("cta", {}).get("enabled", True):
            # plain letter: names only, no link box
            return [KeepTogether([Spacer(1, gap)] + left + [Spacer(1, gap)])]
        cols = {"light_bg": self.c_light, "accent": self.c_accent, "accent_text": self.c_accent_text, "muted": self.c_muted}
        link = qr.get("display_prefix", "") + r.slug
        cta = sig.get("cta", {})
        name = r.agency_name or r.company
        title = cta.get("title", "").replace("{agency_name}", name)
        # personalised title may wrap to two lines; only fall back for very long names
        if not name or len(simpleSplit(title, self.f["heading_semibold"], 8.6, 82 * mm - 10 * mm)) > 2:
            title = cta.get("title_fallback", "")
        cols["heading"] = self.c_heading
        right = LinkBox(title, cta.get("label", ""), link, self.f, cols)
        width = PAGE_W - (self.lay["margin_left"] + self.lay["margin_right"]) * mm
        t = Table([[left, right]], colWidths=[width - right.box_w, right.box_w], hAlign="LEFT")
        t.setStyle(TableStyle([("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                               ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                               ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
                               # signatures.align: "bottom" (names level with the link) or "top" (names level with the box top)
                               ("VALIGN", (0, 0), (0, 0), sig.get("align", "bottom").upper())]))
        gap = self.lay.get("cta_gap_mm", 4) * mm
        # the paragraph above adds its own spaceAfter; move half of it below so both visual gaps match
        # (-0.2 mm compensates the ink offset between descenders above and cap height below, measured)
        shift = max(st["body"].spaceAfter / 2 - 0.2 * mm, 0)
        return [KeepTogether([Spacer(1, max(gap - shift, 0)), t, Spacer(1, gap + shift)])]

    def _story(self, r: Recipient, size: float) -> list:
        st = self._styles(size)
        bold = self.f["heading_semibold"]
        # layout.highlight: "bold" (semibold only) | "marker" (semibold + light cyan background)
        hl_color = self.cfg.get("brand", {}).get("colors", {}).get("highlight", "") \
            if self.lay.get("highlight", "bold") == "marker" else ""
        hl_font = self.f.get("body_heavy", "") if self._body_uses_body_fonts() \
            else self.f.get("heading_semibold", "")

        def inline(text: str) -> str:
            return md_inline(fill(escape(text), r), bold, size, hl_font, hl_color,
                             float(self.lay.get("label_scale", 0.9)))

        story = [NextPageTemplate("later")]
        if self.lay.get("headline_accent", True):
            story.append(AccentBar(self.c_accent))
        if self.tpl.headline.strip():   # empty headline in the template -> letter starts with the greeting
            story.append(Paragraph(fill(escape(self.tpl.headline), r), st["headline"]))
        greeting = self.tpl.greeting if r.first_name else self.tpl.greeting_without_name
        story.append(Paragraph(fill(escape(greeting), r), st["body"]))

        for block in re.split(r"\n\s*\n", self.tpl.body):
            block = block.strip()
            if not block:
                continue
            if block == "{signatures}":
                story += self._signatures(st, r)
                continue
            lines = block.splitlines()
            if all(l.lstrip().startswith("- ") for l in lines):
                items = [ListItem(Paragraph(inline(l.lstrip()[2:]), st["bullet"]),
                                  leftIndent=5 * mm) for l in lines]
                plain = self.lay.get("bullet_style", "accent") == "plain"
                story.append(ListFlowable(items, bulletType="bullet", start="•", leftIndent=5 * mm,
                                          bulletFontName=self.f["body_regular"] if plain else self.f["heading_bold"],
                                          bulletFontSize=size if plain else size * 1.1,
                                          bulletColor=self.c_text if plain else self.c_accent,
                                          bulletOffsetY=0 if plain else size * 0.05))
                story.append(Spacer(1, st["body"].spaceAfter * 0.6))
            else:
                text = " ".join(l.strip() for l in lines)
                story.append(Paragraph(inline(text), st["body"]))
        return story

    def build(self, r: Recipient) -> tuple[bytes, int, float]:
        """Render one letter; shrink the font until it fits one page (or the minimum is reached)."""
        lay = self.lay
        size, min_size = float(lay.get("font_size", 10)), float(lay.get("font_size_min", 9))
        while True:
            buf = io.BytesIO()
            doc = BaseDocTemplate(buf, pagesize=A4, leftMargin=lay["margin_left"] * mm,
                                  rightMargin=lay["margin_right"] * mm, topMargin=20 * mm,
                                  bottomMargin=lay["body_bottom"] * mm, title=f"Brief {r.agency_name}",
                                  author=self.cfg["sender"].get("name", ""))
            doc.recipient = r
            width = PAGE_W - (lay["margin_left"] + lay["margin_right"]) * mm
            first = Frame(lay["margin_left"] * mm, lay["body_bottom"] * mm, width,
                          PAGE_H - lay["body_top"] * mm - lay["body_bottom"] * mm, id="first",
                          leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
            later = Frame(lay["margin_left"] * mm, lay["body_bottom"] * mm, width,
                          PAGE_H - 22 * mm - lay["body_bottom"] * mm, id="later",
                          leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
            doc.addPageTemplates([PageTemplate("first", [first], onPage=self._first_page),
                                  PageTemplate("later", [later], onPage=self._later_page)])
            doc.build(self._story(r, size))
            pages = len(PdfReader(io.BytesIO(buf.getvalue())).pages)
            if pages == 1 or size - 0.25 < min_size:
                return buf.getvalue(), pages, size
            size -= 0.25


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", help="recipient CSV (comma or semicolon), e.g. the data-merge file with paragraphs")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--template", default="letter_template.md")
    ap.add_argument("--out", default="output")
    ap.add_argument("--limit", type=int, help="only the first N printable letters")
    ap.add_argument("--rows", help="specific CSV rows, e.g. 1,5,9 (1-based, header not counted)")
    ap.add_argument("--single", action="store_true", help="also write one PDF per letter")
    args = ap.parse_args()

    cfg = load_config(BASE_DIR / args.config)
    tpl = load_template(BASE_DIR / args.template)
    recipients = read_recipients(Path(args.csv), cfg)
    assign_slugs(recipients, cfg)          # on the full list, so slugs stay stable for --limit/--rows
    if args.rows:
        wanted = {int(x) for x in args.rows.split(",")}
        recipients = [r for r in recipients if r.row in wanted]

    out_dir = Path(args.out)
    (out_dir / "single").mkdir(parents=True, exist_ok=True) if args.single else out_dir.mkdir(parents=True, exist_ok=True)
    builder = LetterBuilder(cfg, tpl)
    writer = PdfWriter()
    report, done = [], 0
    mode = cfg["options"].get("missing_paragraph", "skip")

    for r in recipients:
        if args.limit and done >= args.limit:
            break
        if not r.paragraph:
            if mode == "fallback":
                r.paragraph = re.sub(r"\s+", " ", cfg["options"]["fallback_paragraph"]).strip()
                r.problems.append("fallback paragraph used")
            else:
                report.append([r.row, r.domain, r.company, "skipped", 0, "", r.slug, "no paragraph"])
                continue
        blocking = [p for p in r.problems if p.startswith(("address", "postcode", "no company"))]
        if blocking:
            report.append([r.row, r.domain, r.company, "skipped", 0, "", r.slug, "; ".join(blocking)])
            continue
        try:
            pdf, pages, size = builder.build(r)
        except Exception as e:  # noqa: BLE001 - one broken record must not stop the batch
            report.append([r.row, r.domain, r.company, "error", 0, "", r.slug, str(e)])
            continue
        reader = PdfReader(io.BytesIO(pdf))
        for p in reader.pages:
            writer.add_page(p)
        if cfg["layout"].get("duplex_padding", True) and pages % 2:
            writer.add_blank_page(width=PAGE_W, height=PAGE_H)
        if args.single:
            (out_dir / "single" / f"{r.row:04d}_{r.slug}.pdf").write_bytes(pdf)
        report.append([r.row, r.domain, r.company, "ok", pages, size, r.slug, "; ".join(r.problems)])
        done += 1
        print(f"  [{done}] row {r.row} {r.domain}: {pages} page(s), {size} pt", file=sys.stderr)

    if done:
        with open(out_dir / "letters_all.pdf", "wb") as f:
            writer.write(f)
    with open(out_dir / "report.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["row", "domain", "company", "status", "pages", "font_size", "qr_slug", "note"])
        w.writerows(report)
    with open(out_dir / "qr_links.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["row", "domain", "company", "first_name", "last_name", "qr_slug", "url"])
        for r in recipients:
            w.writerow([r.row, r.domain, r.company, r.first_name, r.last_name, r.slug,
                        cfg["qr"]["base_url"].rstrip("/") + "/" + r.slug])

    skipped = sum(1 for x in report if x[3] != "ok")
    two_pages = sum(1 for x in report if x[3] == "ok" and x[4] > 1)
    print(f"\n{done} letters -> {out_dir / 'letters_all.pdf'} | {skipped} skipped/errors | "
          f"{two_pages} letters need 2 pages | see report.csv", file=sys.stderr)


if __name__ == "__main__":
    main()
