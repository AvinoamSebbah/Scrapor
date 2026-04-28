"""
Standalone test script — sends a test price-drop email without hitting the DB.
Usage:
  python send_test_email.py [he|en|fr]
  TEST_EMAIL=foo@bar.com python send_test_email.py fr
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

from notify_price_drops import build_html_email, send_email, STRINGS

lang = sys.argv[1] if len(sys.argv) > 1 else "he"
to   = os.environ.get("TEST_EMAIL", "avinoam.sebbah@gmail.com")

# ── Fake products — 2 products, stores from different chains + 2 stores of same chain ──
products = [
    {
        "item_code": "7290000066775",
        "item_name": "קפה עלית טורקי 200 גרם",
        "stores": [
            # Two Shufersal branches → should merge to ONE chain row (cheapest)
            {
                "chain_id":        "7290027600007",
                "chain_name":      "שופרסל",
                "store_name":      "שופרסל דיל רמת גן",
                "store_base_price": 24.90,
                "promo_price":     16.90,
                "promo_end":       None,
                "promo_desc":      "2 ב-34 ₪",
            },
            {
                "chain_id":        "7290027600007",
                "chain_name":      "שופרסל",
                "store_name":      "שופרסל שלי תל אביב",
                "store_base_price": 24.90,
                "promo_price":     17.90,   # more expensive → should NOT appear
                "promo_end":       None,
                "promo_desc":      "מחיר חבר",
            },
            {
                "chain_id":        "7290058140886",
                "chain_name":      "רמי לוי שיווק השקמה",
                "store_name":      "רמי לוי נתניה",
                "store_base_price": 23.50,
                "promo_price":     15.50,
                "promo_end":       None,
                "promo_desc":      "מבצע שבועי",
            },
            {
                "chain_id":        "7290103152017",
                "chain_name":      "אושר עד",
                "store_name":      "אושר עד רחובות",
                "store_base_price": 25.00,
                "promo_price":     18.00,
                "promo_end":       None,
                "promo_desc":      "",
            },
        ],
    },
    {
        "item_code": "7290000117509",
        "item_name": "שמן זית כתית מעולה 750 מ\"ל",
        "stores": [
            {
                "chain_id":        "7290873255550",
                "chain_name":      "טיב טעם",
                "store_name":      "טיב טעם גבעתיים",
                "store_base_price": 42.90,
                "promo_price":     29.90,
                "promo_end":       None,
                "promo_desc":      "חבר קלאב",
            },
            {
                "chain_id":        "7290058140886",
                "chain_name":      "רמי לוי שיווק השקמה",
                "store_name":      "רמי לוי חולון",
                "store_base_price": 44.00,
                "promo_price":     31.50,
                "promo_end":       None,
                "promo_desc":      "",
            },
            # Two Yochananof → should merge to ONE row
            {
                "chain_id":        "7290803800003",
                "chain_name":      "מ. יוחננוף ובניו",
                "store_name":      "יוחננוף ירושלים",
                "store_base_price": 43.00,
                "promo_price":     32.00,
                "promo_end":       None,
                "promo_desc":      "מחיר מיוחד",
            },
            {
                "chain_id":        "7290803800003",
                "chain_name":      "מ. יוחננוף ובניו",
                "store_name":      "יוחננוף פתח תקווה",
                "store_base_price": 43.00,
                "promo_price":     30.00,   # cheaper → this one should show
                "promo_end":       None,
                "promo_desc":      "מחיר מיוחד",
            },
        ],
    },
]

html = build_html_email("אבינועם", lang, products)
ok   = send_email(to, f"[TEST {lang.upper()}] ירידות מחיר — Agali", html)
if ok:
    print(f"✅ Email sent successfully!")
else:
    print(f"❌ Failed to send email.")
