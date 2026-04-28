"""
W6 — Price-drop notification mailer.
Runs after W2 (full scrape).

Logic:
  1. Check Israeli time window — exit cleanly if outside hours.
  2. Load all active, non-expired observations (including stored target_price).
  3. Per-store query: for each (item_code, city), find every store with an
     active promo — including that store's regular price.
  4. In-memory: for each observation, collect qualifying stores where
     promo_price <= target_price (or fall back to pct-based check).
  5. Group by user → fetch user emails + language preferences.
  6. Build one HTML email per user — one card per product, multi-store rows.
  7. Send via Resend.
  8. Bulk-update observations (last_notified_price, last_notified_at, promo_expires_at).
"""

import os
import re
import sys
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRESQL_URL")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
RESEND_FROM = os.environ.get("RESEND_FROM_EMAIL", "Agali Alerts <alerts@agali.live>")
SITE_URL = os.environ.get("SITE_URL", "https://agali.live")
CLOUDINARY_CLOUD = "dprve5nst"

ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")

# ── Chain slug map (mirrors chainLogos.ts) ────────────────────────────────────

CHAIN_SLUG_MAP: dict[str, str] = {
    'BE': 'be',
    'Dabach': 'dabach',
    'Dor Alon': 'dor-alon',
    'אושר עד': 'osher-ad',
    'ג.מ. מעיין אלפיים (07) בע"מ': 'maayan-alpayim',
    'גוד מרקט': 'good-market',
    'גוד פארם בע"מ': 'good-pharm',
    'וולט מרקט': 'wolt-market',
    'זול ובגדול בע"מ': 'zol-vebegadol',
    'טיב טעם': 'tiv-taam',
    'יוניברס': 'universe',
    'יש': 'yesh',
    'יש חסד': 'yesh-hesed',
    'מ. יוחננוף ובניו': 'yochananof',
    'משנת יוסף - קיי טי יבוא ושיווק בע"מ': 'mishnat-yosef',
    'נתיב החסד- סופר חסד בע"מ': 'nativ-hahesed',
    'סופר ברקת קמעונאות בע"מ': 'bareket',
    'סופר יודה': 'super-yuda',
    'סופר ספיר בע"מ': 'super-sapir',
    'סטופמרקט': 'stop-market',
    'סיטי מרקט': 'city-market',
    'סיטי צפריר בע"מ': 'city-tzafrir',
    'פוליצר': 'politzer',
    'פז קמעונאות ואנרגיה בע"מ': 'paz',
    'פרש מרקט': 'fresh-market',
    'קי טי יבוא ושווק בע"מ': 'kt-import',
    'רמי לוי בשכונה': 'rami-levy-bashchuna',
    'רמי לוי שיווק השקמה': 'rami-levy',
    'שופרסל': 'shufersal',
    'שופרסל ONLINE': 'shufersal-online',
    'שופרסל אקספרס': 'shufersal-express',
    'שופרסל דיל': 'shufersal-deal',
    'שופרסל שלי': 'shufersal-sheli',
    'שוק העיר (ט.ע.מ.ס.) בע"מ': 'shuk-hair',
    'שפע ברכת השם בע"מ': 'shefa-barakat',
}
CHAIN_ID_SLUG_MAP: dict[str, str] = {
    '7290058140886': 'rami-levy',
    '7290103152017': 'osher-ad',
    '7290027600007': 'shufersal-sheli',
    '7290492000005': 'dor-alon',
    '7290526500006': 'dabach',
    '7290639000004': 'stop-market',
    '7290803800003': 'yochananof',
    '7290873255550': 'tiv-taam',
    '7290875100001': 'bareket',
    '7290876100000': 'fresh-market',
    '7291056200008': 'rami-levy-bashchuna',
    '7291059100008': 'politzer',
    '7290058249350': 'wolt-market',
    '7290058197699': 'good-pharm',
    '7290000000003': 'city-market',
    '7290058173198': 'zol-vebegadol',
    '7290058159628': 'maayan-alpayim',
    '7290058156016': 'super-sapir',
    '7290058177776': 'super-yuda',
    '7290058134977': 'shefa-barakat',
    '7290058148776': 'shuk-hair',
    '7290058160839': 'nativ-hahesed',
    '7290058158628': 'yesh',
    '7290058266241': 'city-tzafrir',
    '7290058289400': 'kt-import',
    '7290644700005': 'paz',
    '5144744100002': 'mishnat-yosef',
}


def _normalize(s: str) -> str:
    s = s.lower().replace('"', '').replace("'", '')
    s = re.sub(r'[()\[\].,:;\-_/\\]+', ' ', s)
    s = re.sub(r'\bבע\s*מ\b', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


_NORM_MAP = {_normalize(k): v for k, v in CHAIN_SLUG_MAP.items()}


def chain_logo_url(chain_name: str, chain_id: str = '') -> str | None:
    if chain_name in CHAIN_SLUG_MAP:
        return f"{SITE_URL}/images/stores/{CHAIN_SLUG_MAP[chain_name]}.jpg"
    if chain_id and chain_id in CHAIN_ID_SLUG_MAP:
        return f"{SITE_URL}/images/stores/{CHAIN_ID_SLUG_MAP[chain_id]}.jpg"
    slug = _NORM_MAP.get(_normalize(chain_name))
    # keyword fallback
    if not slug:
        n = chain_name
        if 'שופרסל' in n:
            if 'אקספרס' in n: slug = 'shufersal-express'
            elif 'דיל' in n or 'אקסטרה' in n: slug = 'shufersal-deal'
            elif 'שלי' in n: slug = 'shufersal-sheli'
            elif 'online' in n.lower() or 'אונליין' in n: slug = 'shufersal-online'
            else: slug = 'shufersal'
        elif 'רמי לוי' in n:
            slug = 'rami-levy-bashchuna' if 'בשכונה' in n else 'rami-levy'
        elif 'יש חסד' in n: slug = 'yesh-hesed'
        elif 'יש' in n: slug = 'yesh'
        elif 'יוחננוף' in n: slug = 'yochananof'
        elif 'אושר עד' in n: slug = 'osher-ad'
        elif 'וולט' in n: slug = 'wolt-market'
        elif 'פרש' in n: slug = 'fresh-market'
    return f"{SITE_URL}/images/stores/{slug}.jpg" if slug else None


# ── i18n strings ──────────────────────────────────────────────────────────────

STRINGS = {
    "he": {
        "subject_single": "🔔 ירידת מחיר על {name}!",
        "subject_multi":  "🔔 ירידות מחיר על {n} מוצרים!",
        "greeting": "שלום {name}!",
        "intro": "מצאנו ירידות מחיר על המוצרים שעקבת אחריהם:",
        "base": "מחיר רגיל",
        "promo": "מחיר מבצע",
        "discount": "הנחה",
        "view": "צפה במוצר",
        "footer_unsub": "קיבלת מייל זה כי נרשמת להתראות מחיר ב-Agali.",
        "footer_link": "לניהול ההתראות שלך",
        "at_store": "ב",
    },
    "en": {
        "subject_single": "🔔 Price drop on {name}!",
        "subject_multi":  "🔔 Price drops on {n} items!",
        "greeting": "Hi {name}!",
        "intro": "We found price drops on products you're watching:",
        "base": "Regular price",
        "promo": "Sale price",
        "discount": "Discount",
        "view": "View product",
        "footer_unsub": "You received this email because you subscribed to price alerts on Agali.",
        "footer_link": "Manage your alerts",
        "at_store": "at",
    },
    "fr": {
        "subject_single": "🔔 Baisse de prix sur {name} !",
        "subject_multi":  "🔔 Baisses de prix sur {n} articles !",
        "greeting": "Bonjour {name} !",
        "intro": "Nous avons trouvé des baisses de prix sur les produits que vous suivez :",
        "base": "Prix habituel",
        "promo": "Prix promotionnel",
        "discount": "Réduction",
        "view": "Voir le produit",
        "footer_unsub": "Vous avez reçu cet email car vous êtes abonné·e aux alertes de prix Agali.",
        "footer_link": "Gérer vos alertes",
        "at_store": "chez",
    },
}


# ── Time window check ─────────────────────────────────────────────────────────

def is_within_notification_window() -> bool:
    now = datetime.now(ISRAEL_TZ)
    iso_day = now.isoweekday()
    hour = now.hour
    if iso_day == 6:
        log.info("🚫 Shabbat — no notifications sent.")
        return False
    if iso_day == 5:
        if 9 <= hour < 13:
            return True
        log.info(f"🚫 Friday outside window (hour={hour}).")
        return False
    if 9 <= hour < 23:
        return True
    log.info(f"🚫 Outside notification window (iso_day={iso_day}, hour={hour}).")
    return False


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


# ── Cloudinary URL ────────────────────────────────────────────────────────────

def cloudinary_url(item_code: str) -> str:
    return (
        f"https://res.cloudinary.com/{CLOUDINARY_CLOUD}"
        f"/image/upload/f_auto,q_auto,w_200/products/{item_code}.jpg"
    )


# ── HTML email builder ────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    """Minimal HTML escape for user-supplied text in email."""
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def build_html_email(user_name: str, lang: str, products: list[dict]) -> str:
    s = STRINGS.get(lang, STRINGS["he"])
    # text direction for global layout (product name, greeting, etc.)
    text_dir = 'rtl' if lang == 'he' else 'ltr'
    text_align = 'right' if lang == 'he' else 'left'

    product_cards = ""
    for p in products:
        product_url = f"{SITE_URL}/product/{p['item_code']}"
        img_url = cloudinary_url(p["item_code"])

        # ── Group stores by chain — keep best (lowest) promo per chain ──────
        from collections import defaultdict as _dd
        chains_map: dict = _dd(lambda: {"promo_price": float("inf"), "store": None})
        for store in p["stores"]:
            key = store.get("chain_id") or store["chain_name"]
            if store["promo_price"] < chains_map[key]["promo_price"]:
                chains_map[key] = {"promo_price": store["promo_price"], "store": store}
        # Sort cheapest first
        chain_entries = sorted(chains_map.values(), key=lambda x: x["promo_price"])

        store_rows = ""
        for entry in chain_entries:
            store = entry["store"]
            base = store["store_base_price"]
            promo = store["promo_price"]
            pct = round(((base - promo) / base) * 100) if base > 0 else 0
            logo = chain_logo_url(store["chain_name"], store.get("chain_id", ""))
            promo_desc = _esc(store.get("promo_desc", "")[:80]) if store.get("promo_desc") else ""

            # Logo cell — always on the right (RTL)
            if logo:
                logo_cell = (
                    f'<td width="44" style="width:44px;padding:0 0 0 8px;vertical-align:middle;">'
                    f'<img src="{logo}" width="40" height="40" alt="{_esc(store["chain_name"])}" '
                    f'style="display:block;width:40px;height:40px;object-fit:contain;'
                    f'border-radius:8px;background-color:#1a1a4a;border:1px solid rgba(255,255,255,0.07);" />'
                    f'</td>'
                )
            else:
                initials = _esc(store["chain_name"][:2])
                logo_cell = (
                    f'<td width="44" style="width:44px;padding:0 0 0 8px;vertical-align:middle;">'
                    f'<div class="logo-bg" style="width:40px;height:40px;background:#1a1a4a;border-radius:8px;'
                    f'text-align:center;line-height:40px;font-size:13px;font-weight:700;'
                    f'color:#a5b4fc;border:1px solid rgba(255,255,255,0.07);">{initials}</div>'
                    f'</td>'
                )

            # Price cell — always on the left (RTL = right side for LTR readers = visual left)
            price_cell = (
                f'<td style="text-align:left;vertical-align:middle;padding:0 0 0 4px;white-space:nowrap;">'
                f'<div>'
                f'<span class="promo-badge" style="display:inline-block;font-size:10px;font-weight:800;'
                f'color:#6366f1;background:rgba(99,102,241,0.18);border-radius:8px;padding:1px 6px;'
                f'letter-spacing:0.3px;">&#8722;{pct}%</span>'
                f'</div>'
                f'<div style="margin-top:2px;">'
                f'<span class="price-promo" style="font-size:17px;font-weight:900;color:#34d399;font-variant-numeric:tabular-nums;">&#8362;{promo:.2f}</span>'
                f'</div>'
                f'<div style="margin-top:1px;">'
                f'<span class="price-base" style="font-size:11px;color:#666;text-decoration:line-through;font-variant-numeric:tabular-nums;">&#8362;{base:.2f}</span>'
                f'</div>'
                f'</td>'
            )

            # Name cell — takes remaining space, truncate long names
            chain_display = _esc(store["chain_name"])
            name_cell = (
                f'<td style="vertical-align:middle;padding:0 8px;overflow:hidden;">'
                f'<div class="chain-name" style="font-size:13px;font-weight:700;color:#d4d4f0;'
                f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px;">'
                f'{chain_display}</div>'
                + (f'<div style="font-size:10px;color:#6866a0;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px;">{promo_desc}</div>' if promo_desc else '')
                + f'</td>'
            )

            # Row wraps in a link (entire row clickable, no underline, no color change)
            row_link_start = (
                f'<a href="{product_url}" style="display:block;text-decoration:none;color:inherit;" '
                f'class="store-row-link">'
            )
            row_link_end = '</a>'

            store_rows += f"""
              <tr class="store-sep" style="border-bottom:1px solid rgba(255,255,255,0.04);">
                <td style="padding:0;">
                  {row_link_start}
                  <table width="100%" cellpadding="0" cellspacing="0" dir="rtl"
                    style="padding:8px 0;">
                    <tr>
                      {logo_cell}
                      {name_cell}
                      {price_cell}
                    </tr>
                  </table>
                  {row_link_end}
                </td>
              </tr>"""

        product_cards += f"""
        <tr>
          <td style="padding:0 0 16px 0;">
            <table width="100%" cellpadding="0" cellspacing="0"
              class="product-card"
              style="background-color:#10103a;border-radius:16px;overflow:hidden;
                     border:1px solid rgba(99,102,241,0.2);">
              <!-- Product header row: image + name -->
              <tr>
                <td style="padding:0;width:80px;vertical-align:top;">
                  <a href="{product_url}" style="display:block;text-decoration:none;">
                    <img src="{img_url}" width="80" height="80" alt="{_esc(p['item_name'])}"
                      style="display:block;width:80px;height:80px;object-fit:contain;background:#1a1a4a;"
                      onerror="this.style.display='none'" />
                  </a>
                </td>
                <td style="padding:12px 14px 12px 6px;vertical-align:middle;
                  direction:{text_dir};text-align:{text_align};">
                  <a href="{product_url}" style="text-decoration:none;">
                    <div class="product-name" style="font-size:13px;font-weight:700;
                      color:#ffffff;line-height:1.35;">{_esc(p['item_name'])}</div>
                  </a>
                </td>
              </tr>
              <!-- Store rows separator -->
              <tr>
                <td colspan="2" style="padding:0;">
                  <div class="card-sep" style="height:1px;background:rgba(99,102,241,0.15);margin:0 12px;"></div>
                </td>
              </tr>
              <!-- Store list -->
              <tr>
                <td colspan="2" style="padding:0 8px 4px;">
                  <table width="100%" cellpadding="0" cellspacing="0">
                    {store_rows}
                  </table>
                </td>
              </tr>
              <!-- View product button -->
              <tr>
                <td colspan="2" style="padding:8px 12px 12px;text-align:center;">
                  <a href="{product_url}"
                    style="display:inline-block;background:linear-gradient(135deg,#6366f1,#818cf8);
                      color:#ffffff;font-size:12px;font-weight:700;padding:7px 22px;
                      border-radius:20px;text-decoration:none;letter-spacing:0.2px;">
                    {s['view']} &#8594;
                  </a>
                </td>
              </tr>
            </table>
          </td>
        </tr>"""

    alerts_url = f"{SITE_URL}/product"
    greeting = s["greeting"].format(name=_esc(user_name))
    agali_logo = f"{SITE_URL}/logo.png"

    # ── Logo column — on the right in HE, on the left in LTR ─────────────────
    logo_col = f"""<td width="72" style="width:72px;padding:0;vertical-align:middle;text-align:center;">
              <a href="{SITE_URL}" style="display:inline-block;text-decoration:none;">
                <img src="{agali_logo}" width="48" height="48" alt="Agali"
                  style="display:block;margin:0 auto 4px;border-radius:12px;
                    box-shadow:0 0 18px rgba(99,102,241,0.55);" />
                <div class="agali-name"
                  style="font-size:13px;font-weight:900;letter-spacing:-0.2px;color:#ffffff;">Agali</div>
              </a>
            </td>"""

    greeting_col = f"""<td style="padding:0 {'0 0 14px' if lang == 'he' else '14px 0 0'};
              vertical-align:middle;direction:{text_dir};text-align:{text_align};">
              <h2 class="greeting-text"
                style="margin:0 0 4px;font-size:18px;font-weight:800;color:#ffffff;">{greeting}</h2>
              <p class="intro-text"
                style="margin:0;font-size:13px;line-height:1.55;color:#8888b8;">{s['intro']}</p>
            </td>"""

    # Hebrew: logo right → comes first in RTL table; LTR: logo left → comes first
    if lang == 'he':
        header_row_cells = logo_col + greeting_col
    else:
        header_row_cells = greeting_col + logo_col

    return f"""<!DOCTYPE html>
<html lang="{lang}" dir="{text_dir}">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <meta name="color-scheme" content="dark light" />
  <meta name="supported-color-schemes" content="dark light" />
  <style>
    /* ── Light mode override (dark is already set via inline styles) ─ */
    @media (prefers-color-scheme: light) {{
      body, .email-bg {{ background-color: #f0f0f7 !important; }}
      .main-card {{ background-color: #ffffff !important; }}
      .footer-card {{ background-color: #ebebf5 !important; border-top: 1px solid rgba(0,0,0,0.06) !important; }}
      .product-card {{ background-color: #f5f5ff !important; border-color: rgba(99,102,241,0.15) !important; }}
      .product-name {{ color: #111133 !important; }}
      .chain-name {{ color: #222250 !important; }}
      .greeting-text {{ color: #111133 !important; }}
      .intro-text {{ color: #555577 !important; }}
      .footer-text {{ color: #888899 !important; }}
      .price-promo {{ color: #059669 !important; }}
      .price-base {{ color: #9999aa !important; }}
      .promo-badge {{ color: #4f46e5 !important; background: rgba(99,102,241,0.1) !important; }}
      .agali-name {{ color: #111133 !important; }}
      .store-row-bg {{ background-color: transparent !important; }}
      .store-sep {{ border-bottom-color: rgba(0,0,0,0.05) !important; }}
      .logo-bg {{ background-color: #e8e8f8 !important; border-color: rgba(0,0,80,0.08) !important; }}
      .card-sep {{ background: rgba(99,102,241,0.1) !important; }}
    }}
    @media only screen and (max-width: 600px) {{
      .outer-table {{ width: 100% !important; }}
    }}
    a {{ color: inherit !important; text-decoration: none !important; }}
  </style>
</head>
<body class="email-bg" style="margin:0;padding:0;background-color:#06061a;
  font-family:-apple-system,'Helvetica Neue',Arial,sans-serif;">
  <table class="email-bg" width="100%" cellpadding="0" cellspacing="0"
    style="background-color:#06061a;padding:12px 0 0 0;">
    <tr>
      <td align="center">
        <table class="outer-table" width="560" cellpadding="0" cellspacing="0"
          style="max-width:560px;width:100%;">

          <!-- HEADER + GREETING (combined, logo inline) -->
          <tr>
            <td class="main-card"
              style="background-color:#0d0d2b;border-radius:28px 28px 0 0;
                padding:20px 22px 14px 22px;">
              <table width="100%" cellpadding="0" cellspacing="0" dir="{text_dir}">
                <tr>
                  {header_row_cells}
                </tr>
              </table>
            </td>
          </tr>

          <!-- accent line below header -->
          <tr>
            <td class="main-card" style="background-color:#0d0d2b;padding:0 22px 4px;">
              <div style="height:1px;background:linear-gradient(to {'left' if lang == 'he' else 'right'},#6366f1,#a855f7,transparent);
                border-radius:2px;"></div>
            </td>
          </tr>

          <!-- PRODUCT CARDS -->
          <tr>
            <td class="main-card" style="background-color:#0d0d2b;padding:14px 22px 22px;">
              <table width="100%" cellpadding="0" cellspacing="0">{product_cards}</table>
            </td>
          </tr>

          <!-- FOOTER -->
          <tr>
            <td class="footer-card"
              style="background-color:#0a0a24;border-radius:0 0 28px 28px;
                padding:14px 22px;text-align:center;
                border-top:1px solid rgba(255,255,255,0.06);">
              <p class="footer-text"
                style="margin:0 0 5px;font-size:11px;color:#44446a;line-height:1.5;">{s['footer_unsub']}</p>
              <a href="{alerts_url}" class="footer-link"
                style="font-size:11px;color:#6366f1;text-decoration:none;">{s['footer_link']}</a>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# ── Resend sender ─────────────────────────────────────────────────────────────

def send_email(to: str, subject: str, html: str) -> bool:
    if not RESEND_API_KEY:
        log.warning("RESEND_API_KEY not set — skipping email send.")
        return False
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"from": RESEND_FROM, "to": [to], "subject": subject, "html": html},
            timeout=15,
            verify=os.environ.get("SKIP_TLS_VERIFY", "").lower() != "true",
        )
        if resp.status_code in (200, 201):
            return True
        log.error(f"Resend error {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as exc:
        log.error(f"Resend request failed: {exc}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not DATABASE_URL:
        log.error("DATABASE_URL / POSTGRESQL_URL not set.")
        sys.exit(1)

    if not is_within_notification_window():
        sys.exit(0)

    conn = get_connection()
    try:
        with conn.cursor() as cur:

            # ── 1. Load active observations ───────────────────────────────────
            cur.execute("""
                SELECT
                    o.id,
                    o.user_id,
                    o.item_code,
                    o.city,
                    o.min_discount_pct,
                    o.base_price,
                    o.target_price,
                    o.last_notified_price,
                    o.last_notified_at,
                    o.promo_expires_at
                FROM observations o
                WHERE o.status = 'active'
                  AND o.expires_at > NOW()
            """)
            observations = cur.fetchall()

        if not observations:
            log.info("No active observations — nothing to do.")
            return

        log.info(f"Loaded {len(observations)} active observation(s).")

        # ── 2. Unique (item_code, city) pairs — use string interpolation for tuple IN ──
        pairs = list({(o["item_code"], o["city"]) for o in observations})
        pair_values = ", ".join(f"('{ic}', '{ct}')" for ic, ct in pairs)

        with conn.cursor() as cur:
            # ── 3. Per-store promo query ──────────────────────────────────────
            # Returns every store with an active promo, including that store's
            # regular (non-promo) price for the same product.
            cur.execute(f"""
                SELECT
                    p.item_code,
                    s.city,
                    s.chain_id,
                    s.chain_name,
                    s.store_name,
                    pp.price            AS store_base_price,
                    psi.promo_price,
                    pr.promotion_end_date AS promo_end,
                    pr.promotion_description
                FROM promotion_store_items psi
                JOIN products p      ON p.id = psi.product_id
                JOIN stores s        ON s.id = psi.store_id
                JOIN product_prices pp
                     ON pp.product_id = psi.product_id
                    AND pp.store_id   = psi.store_id
                JOIN promotions pr
                     ON pr.chain_id      = psi.chain_id
                    AND pr.promotion_id  = psi.promotion_id
                WHERE (p.item_code, s.city) IN ({pair_values})
                  AND pr.promotion_end_date > NOW()
                  AND psi.promo_price IS NOT NULL
                  AND psi.promo_price > 0
                  AND psi.promo_price < pp.price
                ORDER BY p.item_code, s.city, psi.promo_price ASC
            """)
            store_rows = cur.fetchall()

            # ── 4. Product names ──────────────────────────────────────────────
            item_codes = list({o["item_code"] for o in observations})
            ph = ", ".join(["%s"] * len(item_codes))
            cur.execute(
                f"SELECT item_code, item_name FROM products WHERE item_code IN ({ph})",
                item_codes,
            )
            name_map = {r["item_code"]: r["item_name"] for r in cur.fetchall()}

        # Build per-(item_code, city) list of store deals
        from collections import defaultdict
        deals_map: dict[tuple, list[dict]] = defaultdict(list)
        for row in store_rows:
            deals_map[(row["item_code"], row["city"])].append({
                "chain_id":        row["chain_id"],
                "chain_name":      row["chain_name"],
                "store_name":      row["store_name"],
                "store_base_price": float(row["store_base_price"]),
                "promo_price":     float(row["promo_price"]),
                "promo_end":       row["promo_end"],
                "promo_desc":      row["promotion_description"] or "",
            })

        # ── 5. Decide which observations to notify ────────────────────────────
        to_notify: list[dict] = []
        now = datetime.now(ISRAEL_TZ)

        for obs in observations:
            key = (obs["item_code"], obs["city"])
            all_deals = deals_map.get(key, [])

            if not all_deals:
                log.info(f"  SKIP: no active promos for {key}")
                continue

            # Determine target_price: prefer stored value, fall back to pct-based
            target_price = float(obs["target_price"]) if obs["target_price"] is not None else None
            if target_price is None:
                # Legacy: use min store_base_price * (1 - pct/100) as fallback
                min_base = min(d["store_base_price"] for d in all_deals)
                target_price = min_base * (1 - float(obs["min_discount_pct"]) / 100)

            qualifying = [d for d in all_deals if d["promo_price"] <= target_price]

            if not qualifying:
                best = min(d["promo_price"] for d in all_deals)
                log.info(
                    f"  SKIP: best promo ₪{best:.2f} > target ₪{target_price:.2f} "
                    f"for {obs['item_code']} in {obs['city']}"
                )
                continue

            best_promo_price = min(d["promo_price"] for d in qualifying)
            best_promo_end   = max(d["promo_end"] for d in qualifying)

            last_price       = obs["last_notified_price"]
            promo_expires_at = obs["promo_expires_at"]

            promo_expired = False
            if promo_expires_at is not None:
                expires = promo_expires_at
                if expires.tzinfo is None:
                    expires = expires.replace(tzinfo=ISRAEL_TZ)
                promo_expired = expires < now

            should_send = (
                last_price is None
                or promo_expired
                or best_promo_price < float(last_price)
            )

            if not should_send:
                log.info(
                    f"  SKIP: already notified at ₪{float(last_price):.2f}, "
                    f"best now ₪{best_promo_price:.2f}"
                )
                continue

            log.info(
                f"  NOTIFY: {obs['item_code']} in {obs['city']} — "
                f"{len(qualifying)} store(s), best ₪{best_promo_price:.2f} ≤ target ₪{target_price:.2f}"
            )

            to_notify.append({
                "obs_id":          obs["id"],
                "user_id":         obs["user_id"],
                "item_code":       obs["item_code"],
                "item_name":       name_map.get(obs["item_code"], obs["item_code"]),
                "stores":          qualifying,
                "best_promo_price": best_promo_price,
                "best_promo_end":  best_promo_end,
                "city":            obs["city"],
            })

        if not to_notify:
            log.info("No qualifying price drops — no emails to send.")
            return

        log.info(f"{len(to_notify)} notification(s) to send across {len({n['user_id'] for n in to_notify})} user(s).")

        # ── 6. Group by user ──────────────────────────────────────────────────
        user_ids = list({n["user_id"] for n in to_notify})
        with conn.cursor() as cur:
            ph = ", ".join(["%s"] * len(user_ids))
            cur.execute(
                f"SELECT id, email, display_name, name, preferences FROM users WHERE id IN ({ph})",
                user_ids,
            )
            user_rows_db = cur.fetchall()

        user_map = {
            r["id"]: {
                "email": r["email"],
                "name":  r.get("display_name") or r.get("name") or r["email"].split("@")[0],
                "lang":  (r.get("preferences") or {}).get("language", "he"),
            }
            for r in user_rows_db
        }

        grouped: dict[str, list[dict]] = defaultdict(list)
        for n in to_notify:
            grouped[n["user_id"]].append(n)

        # ── 7. Send one email per user ────────────────────────────────────────
        sent_obs: list[tuple] = []  # (obs_id, best_promo_price, best_promo_end)
        emails_sent = 0

        for user_id, notifs in grouped.items():
            udata = user_map.get(user_id)
            if not udata:
                continue

            lang = udata["lang"] if udata["lang"] in STRINGS else "he"
            s = STRINGS[lang]

            # Dynamic subject
            if len(notifs) == 1:
                subject = s["subject_single"].format(name=notifs[0]["item_name"][:30])
            else:
                subject = s["subject_multi"].format(n=len(notifs))

            html = build_html_email(udata["name"], lang, notifs)
            ok = send_email(udata["email"], subject, html)

            if ok:
                emails_sent += 1
                log.info(f"✅ Email sent to {udata['email']} ({len(notifs)} product(s))")
                for n in notifs:
                    sent_obs.append((n["obs_id"], n["best_promo_price"], n["best_promo_end"]))
            else:
                log.warning(f"⚠️  Failed to send to {udata['email']}")

        # ── 8. Bulk-update sent observations ──────────────────────────────────
        if sent_obs:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """UPDATE observations
                       SET last_notified_price = %s,
                           last_notified_at    = NOW(),
                           promo_expires_at    = %s,
                           updated_at          = NOW()
                       WHERE id = %s""",
                    [(price, end, oid) for oid, price, end in sent_obs],
                )
            conn.commit()
            log.info(f"Updated {len(sent_obs)} observation row(s).")

        log.info(f"Done — {emails_sent} email(s) sent.")

    except Exception as exc:
        conn.rollback()
        log.error(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()


