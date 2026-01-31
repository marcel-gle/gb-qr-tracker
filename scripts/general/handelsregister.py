#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GmbHs aus dem Handelsregister per PLZ ziehen und in CSV schreiben (Batch/Paging).

CSV-Spalten (genau wie gewünscht):
- rechtsform
- firmenname
- ort
- plz
- strasse_hausnr
- registernummer   (z.B. HRB 123456 oder HRA 98765)

Install:
  pip install requests beautifulsoup4 lxml

Run:
  python pull_gmbh_by_plz_fields.py 80331 out_80331.csv
"""

import csv
import re
import sys
import time
from typing import Dict, Optional, Tuple, List

import requests
from bs4 import BeautifulSoup

BASE = "https://www.handelsregister.de"
SEARCH_URL = f"{BASE}/rp_web/erweitertesuche/welcome.xhtml"


# konservativ, um typische Limits einzuhalten
SLEEP_BETWEEN_REQUESTS_SEC = 75

PLZ_ORT_RE = re.compile(r"\b(\d{5})\s+([A-Za-zÄÖÜäöüß\-\.\s]+)\b")
REGNO_RE = re.compile(r"\b(HRB|HRA|PR|GnR)\s*([0-9]+)\b", re.IGNORECASE)


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _extract_total_hits(html: str) -> Optional[int]:
    patterns = [
        r"Treffer\s*\(?\s*([0-9]{1,9})\s*\)?",
        r"Insgesamt\s*:\s*([0-9]{1,9})",
        r"([0-9]{1,9})\s+Treffer",
    ]
    for p in patterns:
        m = re.search(p, html, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
    return None


def _best_results_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    best, best_score = None, -1
    for t in tables:
        rows = t.find_all("tr")
        if len(rows) < 2:
            continue
        text = t.get_text(" ", strip=True)
        score = len(rows)
        if any(x in text for x in ["HRB", "HRA", "Register", "Firma", "Sitz", "Anschrift"]):
            score += 25
        if score > best_score:
            best, best_score = t, score
    return best


def _table_to_rows(table) -> Tuple[List[str], List[List[str]]]:
    trs = table.find_all("tr")
    if len(trs) < 2:
        return [], []

    header_cells = trs[0].find_all(["th", "td"])
    headers = [c.get_text(" ", strip=True) for c in header_cells]
    if not any(h.strip() for h in headers):
        headers = [f"col_{i+1}" for i in range(len(header_cells))]

    body = []
    for tr in trs[1:]:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue
        vals = [td.get_text(" ", strip=True) for td in tds]
        if any(v.strip() for v in vals):
            body.append(vals)

    return headers, body


def _extract_hidden_inputs(form) -> Dict[str, str]:
    data = {}
    for inp in form.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if name:
            data[name] = inp.get("value", "")
    return data


def _extract_form_field_name(soup: BeautifulSoup, field_type: str, label_hint: str = "") -> Optional[str]:
    """
    Extract the actual JSF form field name from the HTML.
    
    Args:
        soup: BeautifulSoup object of the form page
        field_type: Type of field to find (e.g., "postleitzahl", "rechtsform")
        label_hint: Optional hint text to find the field
        
    Returns:
        The JSF field name (e.g., "erweiterteSucheForm:postleitzahl") or None
    """
    # Try to find input fields by various attributes
    inputs = soup.find_all("input")
    selects = soup.find_all("select")
    
    for inp in inputs + selects:
        field_id = inp.get("id", "")
        field_name = inp.get("name", "")
        
        # Check if this field matches our search
        if field_type.lower() in field_id.lower() or field_type.lower() in field_name.lower():
            if field_name:
                return field_name
            elif field_id:
                # JSF often uses formId:componentId format
                form = inp.find_parent("form")
                if form:
                    form_id = form.get("id", "erweiterteSucheForm")
                    return f"{form_id}:{field_id}"
    
    return None


def _parse_jsfcljs_onclick(onclick: str) -> Optional[Dict[str, str]]:
    if not onclick:
        return None
    m = re.search(r"\{(.+?)\}", onclick)
    if not m:
        return None
    inner = m.group(1)
    pairs = re.findall(r"'([^']+)'\s*:\s*'([^']*)'", inner)
    if not pairs:
        return None
    return {k: v for k, v in pairs}


def _find_next_page_post(soup: BeautifulSoup) -> Optional[Tuple[str, Dict[str, str]]]:
    for el in soup.find_all(["a", "button", "input"]):
        text = (el.get_text(" ", strip=True) or "").lower()
        val = (el.get("value") or "").lower()
        title = (el.get("title") or "").lower()
        onclick = el.get("onclick") or ""

        is_next = any(x in text for x in ["weiter", "nächste", "naechste", "next", ">"]) \
                  or any(x in val for x in ["weiter", "nächste", "naechste", "next", ">"]) \
                  or any(x in title for x in ["weiter", "nächste", "naechste", "next"])
        if not is_next:
            continue

        data = _parse_jsfcljs_onclick(onclick)
        if data:
            return SEARCH_URL, data
    return None


def _pick_col(headers: List[str], keywords: List[str]) -> Optional[int]:
    hn = [_norm(h) for h in headers]
    for i, h in enumerate(hn):
        for kw in keywords:
            if kw in h:
                return i
    return None


def _guess_rechtsform_from_name(name: str) -> str:
    n = (name or "").strip()
    # grobe Heuristiken, falls Rechtsform nicht als eigene Spalte vorhanden ist
    if re.search(r"\bGmbH\b", n):
        return "GmbH"
    if re.search(r"\bUG\b", n) or re.search(r"UG\s*\(haftungsbeschränkt\)", n, re.IGNORECASE):
        return "UG"
    if re.search(r"\bAG\b", n):
        return "AG"
    if re.search(r"\be\.K\.\b", n) or re.search(r"\beK\b", n):
        return "e.K."
    if re.search(r"\bKG\b", n):
        return "KG"
    if re.search(r"\bGmbH\s*&\s*Co\.\s*KG\b", n):
        return "GmbH & Co. KG"
    return ""


def _extract_record(headers: List[str], row: List[str], fallback_plz: str) -> Dict[str, str]:
    """
    Extrahiert die gewünschten Felder aus einer Tabellenzeile.
    Robust: nutzt Header-Mapping + Regex-Fallback aus der Anschrift.
    """
    # mögliche Header
    idx_name = _pick_col(headers, ["firma", "name", "unternehmensname"])
    idx_ort = _pick_col(headers, ["ort", "sitz", "stadt", "gemeinde"])
    idx_plz = _pick_col(headers, ["plz", "postleitzahl"])
    idx_addr = _pick_col(headers, ["anschrift", "adresse", "sitz/anschrift", "sitz / anschrift", "straße", "strasse"])
    idx_reg = _pick_col(headers, ["registernummer", "register", "hrb", "hra", "aktenzeichen"])

    firmenname = row[idx_name].strip() if idx_name is not None and idx_name < len(row) else ""
    ort = row[idx_ort].strip() if idx_ort is not None and idx_ort < len(row) else ""
    plz = row[idx_plz].strip() if idx_plz is not None and idx_plz < len(row) else ""
    anschrift = row[idx_addr].strip() if idx_addr is not None and idx_addr < len(row) else ""

    # fallback: nimm längste Zelle als "Anschrift", wenn nicht klar
    if not anschrift:
        anschrift = max(row, key=lambda x: len((x or "").strip())).strip()

    # Registernummer: aus Spalte oder via Regex irgendwo aus der Zeile
    registernummer = ""
    if idx_reg is not None and idx_reg < len(row):
        registernummer = row[idx_reg].strip()

    if not registernummer:
        # Suche in der gesamten Zeile nach HRB/HRA/...
        joined = " | ".join([c for c in row if c])
        m = REGNO_RE.search(joined)
        if m:
            registernummer = f"{m.group(1).upper()} {m.group(2)}"

    # PLZ/Ort aus Anschrift extrahieren, wenn nicht als Spalte vorhanden
    if (not plz or not ort) and anschrift:
        m = PLZ_ORT_RE.search(anschrift)
        if m:
            plz = plz or m.group(1)
            # Ort normalisieren (nicht perfekt, aber ok)
            ort = ort or re.sub(r"\s+", " ", m.group(2)).strip()

    plz = (plz or fallback_plz).strip()

    # Straße+Hausnr: aus Anschrift alles entfernen, was PLZ+Ort ist
    strasse_hausnr = anschrift
    if anschrift:
        # entferne registernummer aus anschrift falls drin
        strasse_hausnr = REGNO_RE.sub("", strasse_hausnr).strip(" ,")
        if ort:
            # entferne "PLZ Ort"
            strasse_hausnr = re.sub(
                r"\b" + re.escape(plz) + r"\s+" + re.escape(ort) + r"\b",
                "",
                strasse_hausnr,
                flags=re.IGNORECASE,
            ).strip(" ,")

    rechtsform = _guess_rechtsform_from_name(firmenname)

    return {
        "rechtsform": rechtsform,
        "firmenname": firmenname,
        "ort": ort,
        "plz": plz,
        "strasse_hausnr": strasse_hausnr,
        "registernummer": registernummer,
    }


def fetch_gmbh_by_plz(plz: str, out_csv: str) -> None:
    s = requests.Session()
    # Set proper browser-like headers
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    })

    # First, GET the form page to obtain JSF viewState
    print(f"[INFO] Loading search form from {SEARCH_URL}...")
    try:
        resp = s.get(SEARCH_URL, timeout=60)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to load search form: {e}")
        if 'resp' in locals():
            print(f"[INFO] Response status: {resp.status_code}")
            print(f"[INFO] Response headers: {dict(resp.headers)}")
        raise
    
    form_html = resp.text
    
    # Extract JSESSIONID from cookies
    jsessionid = None
    if 'JSESSIONID' in s.cookies:
        jsessionid = s.cookies['JSESSIONID']
        print(f"[DEBUG] Found JSESSIONID in cookies: {jsessionid[:20]}...")
    
    # Extract viewState from JSF form immediately (no delay to avoid session expiry)
    soup = BeautifulSoup(form_html, "lxml")
    viewstate_input = soup.find("input", {"name": "javax.faces.ViewState"})
    viewstate = viewstate_input.get("value", "") if viewstate_input else ""
    
    if not viewstate:
        # Try alternative viewState name
        viewstate_input = soup.find("input", {"name": re.compile(r".*ViewState.*", re.I)})
        viewstate = viewstate_input.get("value", "") if viewstate_input else ""
    
    if not viewstate:
        print("[WARN] Could not find JSF viewState. Trying without it...")
        # Save HTML for debugging
        with open("debug_form.html", "w", encoding="utf-8") as f:
            f.write(form_html)
        print("[DEBUG] Saved form HTML to debug_form.html")
    
    # Find the correct search form (not headerForm)
    forms = soup.find_all("form")
    search_form = None
    form_id = "form"  # default based on field names like "form:postleitzahl"
    
    # First, try to find form with id="form" (common JSF pattern)
    for f in forms:
        if f.get("id") == "form":
            search_form = f
            form_id = "form"
            break
    
    # If not found, look for search form by keywords
    if not search_form:
        for f in forms:
            form_id_attr = f.get("id", "").lower()
            # Look for search form (not header form)
            if "erweiterte" in form_id_attr or "suche" in form_id_attr or "search" in form_id_attr:
                if "header" not in form_id_attr:
                    search_form = f
                    form_id = f.get("id", form_id)
                    break
    
    # If no search form found, try to find form with postleitzahl field
    if not search_form:
        for f in forms:
            if f.find("input", {"id": re.compile(r".*postleitzahl.*", re.I)}) or \
               f.find("input", {"name": re.compile(r".*postleitzahl.*", re.I)}):
                search_form = f
                form_id = f.get("id", form_id)
                break
    
    # Fallback to first form if still not found
    if not search_form and forms:
        search_form = forms[0]
        form_id = search_form.get("id", form_id)
    
    if search_form:
        form_action = search_form.get("action", "")
        print(f"[DEBUG] Search Form ID: {form_id}, Action: {form_action}")
        
        # Extract jsessionid from form action if present and not already found
        if not jsessionid and ";jsessionid=" in form_action:
            jsessionid_from_action = form_action.split(";jsessionid=")[1].split("?")[0].split("/")[0]
            if jsessionid_from_action:
                jsessionid = jsessionid_from_action
                print(f"[DEBUG] Extracted JSESSIONID from form action: {jsessionid[:20]}...")
    else:
        print("[WARN] Could not find search form!")
    
    # Extract actual field names (not _focus fields)
    def find_field_name(soup, field_type, form_id):
        """Find actual input/select field name, not focus field"""
        # Try to find input or select with the field type in id or name
        patterns = [
            f".*{field_type}.*",
            f".*{field_type.replace('_', '')}.*",
        ]
        
        for pattern in patterns:
            # Try by id first
            field = soup.find("input", {"id": re.compile(pattern, re.I)})
            if not field:
                field = soup.find("select", {"id": re.compile(pattern, re.I)})
            if not field:
                field = soup.find("input", {"name": re.compile(pattern, re.I)})
            if not field:
                field = soup.find("select", {"name": re.compile(pattern, re.I)})
            
            if field:
                name = field.get("name")
                if name and "_focus" not in name:
                    return name
                # If name has _focus, try to construct the real field name
                if name and "_focus" in name:
                    return name.replace("_focus", "")
        
        # Fallback to form_id:field_type
        return f"{form_id}:{field_type}"
    
    postleitzahl_field = find_field_name(soup, "postleitzahl", form_id)
    rechtsform_field = find_field_name(soup, "rechtsform", form_id)
    ergebnisse_field = find_field_name(soup, "ergebnisseProSeite", form_id)
    
    # Find the search button
    btn_suche = soup.find("input", {"type": "submit", "value": re.compile(r".*suchen.*", re.I)}) or \
                soup.find("button", string=re.compile(r".*suchen.*", re.I)) or \
                soup.find("input", {"id": re.compile(r".*btnSuche.*", re.I)}) or \
                soup.find("button", {"id": re.compile(r".*btnSuche.*", re.I)})
    
    if btn_suche:
        btn_suche_field = btn_suche.get("name") or btn_suche.get("id", f"{form_id}:btnSuche")
    else:
        btn_suche_field = f"{form_id}:btnSuche"
    
    print(f"[DEBUG] Field names - PLZ: {postleitzahl_field}, Rechtsform: {rechtsform_field}, Ergebnisse: {ergebnisse_field}, Button: {btn_suche_field}")
    
    # Extract all form IDs (JSF may require all forms to be included)
    all_form_ids = {}
    for f in forms:
        fid = f.get("id")
        if fid:
            all_form_ids[fid] = fid
    
    # Prepare payload with JSF parameters
    payload = {
        "javax.faces.ViewState": viewstate,
    }
    
    # Add all form IDs
    payload.update(all_form_ids)
    
    # Add search parameters
    payload.update({
        rechtsform_field: "8",  # GmbH
        postleitzahl_field: plz,
        ergebnisse_field: "100",
        btn_suche_field: "Suchen",
    })
    
    print(f"[DEBUG] Including {len(all_form_ids)} form(s) in payload: {list(all_form_ids.keys())}")
    
    # Update headers for POST request
    s.headers.update({
        "Referer": SEARCH_URL,
        "Origin": BASE,
        "Content-Type": "application/x-www-form-urlencoded",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    })

    # Build POST URL with jsessionid if available
    post_url = SEARCH_URL
    if jsessionid:
        # Insert jsessionid into URL (before query string if present)
        if "?" in post_url:
            base_url, query_string = post_url.split("?", 1)
            post_url = f"{base_url};jsessionid={jsessionid}?{query_string}"
        else:
            post_url = f"{post_url};jsessionid={jsessionid}"
        print(f"[DEBUG] Using POST URL with JSESSIONID: {post_url[:100]}...")
    else:
        print("[WARN] No JSESSIONID found - session may expire")
    
    print(f"[INFO] Searching for GmbH companies in postcode {plz}...")
    print(f"[DEBUG] Payload keys: {list(payload.keys())}")
    
    try:
        resp = s.post(post_url, data=payload, timeout=60, allow_redirects=True)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        print(f"[ERROR] Connection error: {e}")
        print("[INFO] The server may be blocking automated requests.")
        print("[INFO] Try:")
        print("  1. Check if the website is accessible in a browser")
        print("  2. Add a delay between requests")
        print("  3. Check if cookies/JSF session is required")
        raise
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"[INFO] Response status: {e.response.status_code}")
            print(f"[INFO] Response headers: {dict(e.response.headers)}")
            print(f"[INFO] Response text (first 500 chars): {e.response.text[:500]}")
        raise
    
    html = resp.text
    
    # Save response HTML for debugging
    with open("debug_response.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[DEBUG] Saved response HTML to debug_response.html")
    
    # Check for error messages in response
    soup_check = BeautifulSoup(html, "lxml")
    error_msgs = soup_check.find_all(string=re.compile(r".*fehler.*|.*error.*|.*session.*abgelaufen.*", re.I))
    if error_msgs:
        print(f"[WARN] Possible error messages found in response:")
        for msg in error_msgs[:3]:  # Show first 3
            print(f"  - {msg.strip()[:100]}")

    total = _extract_total_hits(html)
    if total is not None:
        print(f"[INFO] Treffer gesamt (best effort): {total}")
    else:
        print("[INFO] Treffer gesamt: nicht sicher aus HTML erkennbar (ok).")

    fieldnames = [
        "rechtsform",
        "firmenname",
        "ort",
        "plz",
        "strasse_hausnr",
        "registernummer",
    ]

    written = 0
    page = 1

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            soup = BeautifulSoup(html, "lxml")
            table = _best_results_table(soup)
            if not table:
                print("[WARN] Keine Ergebnis-Tabelle gefunden. Abbruch.")
                # Debug: show what tables were found
                all_tables = soup.find_all("table")
                print(f"[DEBUG] Found {len(all_tables)} table(s) in response")
                if all_tables:
                    for i, t in enumerate(all_tables[:3], 1):  # Show first 3
                        rows = t.find_all("tr")
                        text_preview = t.get_text(" ", strip=True)[:200]
                        print(f"  Table {i}: {len(rows)} rows, preview: {text_preview}...")
                # Check for common error/empty result indicators
                page_text = soup.get_text(" ", strip=True).lower()
                if "keine treffer" in page_text or "no results" in page_text:
                    print("[INFO] Page indicates no results found")
                elif "session" in page_text and "abgelaufen" in page_text:
                    print("[WARN] Session may have expired")
                break

            headers, body = _table_to_rows(table)
            if not body:
                print("[INFO] Keine weiteren Trefferzeilen gefunden. Fertig.")
                break

            batch_count = 0
            for row in body:
                rec = _extract_record(headers, row, fallback_plz=plz)

                # Sicherstellen: wirklich GmbH (Filter ist gesetzt, aber doppelt hält besser)
                if rec["rechtsform"] != "GmbH" and "GmbH" not in rec["firmenname"]:
                    continue

                # Skip leere Namen
                if not rec["firmenname"]:
                    continue

                writer.writerow(rec)
                batch_count += 1
                written += 1

            f.flush()
            print(f"[INFO] Seite {page}: {batch_count} GmbHs geschrieben (gesamt: {written})")

            nxt = _find_next_page_post(soup)
            if not nxt:
                print("[INFO] Keine nächste Seite gefunden. Fertig.")
                break

            action_url, jsf_data = nxt

            form = soup.find("form")
            if not form:
                print("[WARN] Kein <form> für Pagination gefunden. Abbruch.")
                break

            # Use form action if available, otherwise use the action_url from pagination
            form_action = form.get("action")
            if form_action:
                # Handle relative URLs
                if form_action.startswith("/"):
                    form_action = f"{BASE}{form_action}"
                elif not form_action.startswith("http"):
                    form_action = f"{BASE}/rp_web/erweitertesuche/{form_action}"
                action_url = form_action

            hidden = _extract_hidden_inputs(form)
            post_data = {}
            post_data.update(hidden)
            post_data.update(jsf_data)

            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            resp = s.post(action_url, data=post_data, timeout=60)
            resp.raise_for_status()
            html = resp.text
            page += 1

    print(f"[DONE] CSV geschrieben: {out_csv}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python pull_gmbh_by_plz_fields.py <PLZ> <output.csv>")
        sys.exit(1)

    plz_arg = sys.argv[1].strip()
    out_arg = sys.argv[2].strip()

    if not re.fullmatch(r"\d{5}", plz_arg):
        print("Bitte eine 5-stellige PLZ angeben, z.B. 80331")
        sys.exit(2)

    fetch_gmbh_by_plz(plz_arg, out_arg)
