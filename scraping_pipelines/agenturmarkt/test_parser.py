"""Offline test of parse_detail/extract_slugs against a fixture that mirrors
the structure observed on agenturmarkt.de detail/listing pages."""
import json
from agenturmarkt_scraper import (
    parse_detail, extract_slugs, extract_total,
    decode_masked_email, parse_listing_cards,
)

detail_html = """
<html><body>
<h1>NETPROFIT</h1>
<p>Wir machen Kunden online erfolgreich!</p>
<div>Agentur-Score: 4,9 &bull; <a href="/agentur/netprofit-passau/erfahrung">160 Bewertungen</a></div>
<span>Verifiziertes Profil</span>
<h2>Wer ist NETPROFIT?</h2>
<p>NETPROFIT ist eine inhabergef&uuml;hrte Webagentur aus Passau mit &uuml;ber 20 Jahren Erfahrung.</p>
<p>Besonders hervorzuheben sind die exzellenten Angebote im Bereich SEO.</p>
<h3>Dienstleistungen</h3>
<ul><li>Webdesign</li><li>SEO</li><li>Online-Marketing</li></ul>
<h3>Branchen</h3>
<ul><li>E-Commerce</li><li>Dienstleister</li></ul>
<h2>Aehnliche Werbeagenturen:</h2>
<a href="/agenturen/werbeagentur">Werbeagentur</a>
<a href="https://netprofit.de/?utm_source=agenturmarkt&utm_medium=referral&utm_campaign=profile">netprofit.de</a>
<div>Passau, Bayern</div>
<div>Gegr&uuml;ndet 2001</div>
<div>9 Mitarbeiter</div>
<div>Adresse: NETPROFIT Nibelungenplatz 2 94032 Passau Deutschland</div>
</body></html>
"""

d = parse_detail(detail_html, "netprofit-passau")
expect = {
    "name": "NETPROFIT", "score": 4.9, "review_count": 160,
    "founded": 2001, "employees": "9", "verified": 1,
    "city": "Passau", "state": "Bayern", "website": "https://netprofit.de/",
}
ok = True
for k, v in expect.items():
    got = d.get(k)
    status = "OK " if got == v else "FAIL"
    if got != v: ok = False
    print(f"{status} {k}: {got!r} (expected {v!r})")
print("OK  services:", json.loads(d["services"]))
print("OK  industries:", json.loads(d["industries"]))
print("OK  categories:", json.loads(d["categories"]))
print("desc:", (d.get("description") or "")[:60], "...")
print("addr:", d.get("address"))
print("slogan:", d.get("slogan"))

listing_html = """
<html><body>
<a href="/agentur/netprofit-passau">Profil</a>
<a href="https://www.agenturmarkt.de/agentur/maximiliandesign-nuertingen">x</a>
<a href="/agentur/netprofit-passau/kontaktieren">skip-me</a>
<a href="/agentur/netprofit-passau">dupe</a>
<a href="/agenturen/seo-agentur">not an agency</a>
<div>Ergebnisse 11 - 20 von 40.480</div>
</body></html>
"""
slugs = extract_slugs(listing_html)
print("slugs:", slugs)
assert slugs == ["netprofit-passau", "maximiliandesign-nuertingen"], slugs
assert extract_total(listing_html) == 40480
print("LISTING_OK, total=40480")

assert decode_masked_email(".de<!ATstammgast-personal#.info>") == "info@stammgast-personal.de"
assert decode_masked_email(".de&lt;!ATstammgast-personal#.info&gt;") == "info@stammgast-personal.de"

card_html = """
<html><body>
<div class="company-list-item claimed">
  <h3 class="name">Stammgast Personal GmbH</h3>
  <p class="catchphrase">qualifizierte Ingenieure</p>
  <div class="rating" data-rating="4.91783" data-reviews="135"></div>
  <div class="attribute" wire:key="location-1"><span>Berlin, Berlin</span></div>
  <div class="attribute" wire:key="founding-year-1"><span>2023</span></div>
  <a href="https://stammgast-personal.de/?utm_source=agenturmarkt&amp;utm_medium=referral&amp;utm_campaign=list">stammgast-personal.de</a>
  <div x-data="{ email: '.de&lt;!ATstammgast-personal#.info&gt;' }">
    <a href="#">mask</a>
  </div>
  <a href="https://www.agenturmarkt.de/agentur/stammgast-personal-gmbh-berlin">Zum Profil</a>
  <div class="item-category"><span>Recruiting-Agentur</span></div>
</div>
</body></html>
"""
cards = parse_listing_cards(card_html)
assert len(cards) == 1, cards
c = cards[0]
assert c["slug"] == "stammgast-personal-gmbh-berlin"
assert c["name"] == "Stammgast Personal GmbH"
assert c["email"] == "info@stammgast-personal.de", c.get("email")
assert c["website"] == "https://stammgast-personal.de/"
assert c["city"] == "Berlin"
assert c["founded"] == 2023
assert c["score"] == 4.92
print("CARD_OK", c["email"])
print("ALL_PASS" if ok else "SOME_FAILURES")
