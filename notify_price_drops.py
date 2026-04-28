"""
W6 — Price-drop notification mailer.
Runs after W5 (which already waits for all W3 uploads to finish).

Logic:
  1. Check Israeli time window — exit cleanly if outside hours.
  2. Load all active, non-expired observations (single query).
  3. Bulk-fetch base prices for all watched (item_code, city) pairs.
  4. Bulk-fetch best current promo prices for all watched pairs.
  5. In-memory decision: which observations warrant an email?
  6. Group by user → fetch user emails + language preferences.
  7. Build one HTML email per user and send via Resend.
  8. Bulk-update observations (last_notified_price, last_notified_at, promo_expires_at).
"""

import os
import sys
import json
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
RESEND_FROM = os.environ.get("RESEND_FROM_EMAIL", "alerts@agali.live")
SITE_URL = os.environ.get("SITE_URL", "https://agali.live")
CLOUDINARY_CLOUD = "dprve5nst"

ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")

# ── i18n strings ──────────────────────────────────────────────────────────────

STRINGS = {
    "he": {
        "subject": "🔔 ירידת מחיר ב-Agali!",
        "greeting": "שלום {name}!",
        "intro": "מצאנו ירידות מחיר על המוצרים שעקבת אחריהם:",
        "base": "מחיר רגיל",
        "promo": "מחיר מבצע",
        "discount": "הנחה",
        "view": "צפה במוצר",
        "footer_unsub": "קיבלת מייל זה כי נרשמת להתראות מחיר ב-Agali.",
        "footer_link": "לניהול ההתראות שלך",
    },
    "en": {
        "subject": "🔔 Price drops on Agali!",
        "greeting": "Hi {name}!",
        "intro": "We found price drops on products you're watching:",
        "base": "Regular price",
        "promo": "Sale price",
        "discount": "Discount",
        "view": "View product",
        "footer_unsub": "You received this email because you subscribed to price alerts on Agali.",
        "footer_link": "Manage your alerts",
    },
    "fr": {
        "subject": "🔔 Baisses de prix sur Agali !",
        "greeting": "Bonjour {name} !",
        "intro": "Nous avons trouvé des baisses de prix sur les produits que vous suivez :",
        "base": "Prix habituel",
        "promo": "Prix promotionnel",
        "discount": "Réduction",
        "view": "Voir le produit",
        "footer_unsub": "Vous avez reçu cet email car vous êtes abonné·e aux alertes de prix Agali.",
        "footer_link": "Gérer vos alertes",
    },
}


# ── Time window check ─────────────────────────────────────────────────────────

def is_within_notification_window() -> bool:
    """Return True only if current Israel time is within the allowed sending window."""
    now = datetime.now(ISRAEL_TZ)
    weekday = now.weekday()  # Monday=0 … Saturday=6 (Sunday=6 in Python isoweekday, but here Mon=0)
    # isoweekday: Mon=1 … Sun=7
    iso_day = now.isoweekday()  # 1=Mon … 5=Fri, 6=Sat, 7=Sun
    hour = now.hour

    if iso_day == 6:  # Saturday — never
        log.info("🚫 Shabbat — no notifications sent.")
        return False

    if iso_day == 5:  # Friday — 09:00–13:00 only
        if 9 <= hour < 13:
            return True
        log.info(f"🚫 Friday outside window (hour={hour}) — no notifications sent.")
        return False

    # Sunday (iso=7) and Mon–Thu (iso=1..4): 09:00–23:00
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

def build_html_email(user_name: str, lang: str, products: list[dict]) -> str:
    s = STRINGS.get(lang, STRINGS["he"])
    dir_attr = 'rtl' if lang == 'he' else 'ltr'
    align = 'right' if lang == 'he' else 'left'

    product_cards = ""
    for p in products:
        base = p["base_price"]
        promo = p["promo_price"]
        pct = round(((base - promo) / base) * 100) if base > 0 else 0
        product_url = f"{SITE_URL}/product/{p['item_code']}"
        img_url = cloudinary_url(p["item_code"])

        product_cards += f"""
        <tr>
          <td style="padding: 0 0 16px 0;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#13133a;border-radius:16px;overflow:hidden;">
              <tr>
                <td style="padding:0;width:90px;vertical-align:top;">
                  <a href="{product_url}">
                    <img src="{img_url}" width="90" height="90"
                      alt="{p['item_name']}"
                      style="display:block;width:90px;height:90px;object-fit:contain;border-radius:12px 0 0 12px;background:#1a1a4a;"
                      onerror="this.style.display='none'" />
                  </a>
                </td>
                <td style="padding:12px 16px;vertical-align:top;direction:{dir_attr};text-align:{align};">
                  <div style="font-size:14px;font-weight:700;color:#ffffff;margin-bottom:6px;line-height:1.3;">
                    {p['item_name']}
                  </div>
                  <div style="display:inline-block;background:rgba(99,102,241,0.18);border:1px solid rgba(99,102,241,0.35);
                    border-radius:20px;padding:3px 10px;font-size:12px;font-weight:800;color:#a5b4fc;margin-bottom:8px;">
                    -{pct}% {s['discount']}
                  </div>
                  <table cellpadding="0" cellspacing="0">
                    <tr>
                      <td style="padding-{'left' if lang != 'he' else 'right'}:0;">
                        <span style="font-size:11px;color:#6b6b9e;">{s['base']}: </span>
                        <span style="font-size:11px;color:#888;text-decoration:line-through;">₪{base:.2f}</span>
                      </td>
                    </tr>
                    <tr>
                      <td style="padding-top:2px;">
                        <span style="font-size:11px;color:#6b6b9e;">{s['promo']}: </span>
                        <span style="font-size:18px;font-weight:900;color:#34d399;">₪{promo:.2f}</span>
                      </td>
                    </tr>
                  </table>
                  <div style="margin-top:8px;">
                    <a href="{product_url}"
                      style="display:inline-block;background:linear-gradient(135deg,#6366f1,#818cf8);
                        color:#ffffff;font-size:11px;font-weight:700;padding:6px 14px;
                        border-radius:20px;text-decoration:none;">
                      {s['view']} →
                    </a>
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    alerts_url = f"{SITE_URL}/product"
    greeting = s["greeting"].format(name=user_name)

    return f"""<!DOCTYPE html>
<html lang="{lang}" dir="{dir_attr}">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{s['subject']}</title>
</head>
<body style="margin:0;padding:0;background:#06061a;font-family:'Helvetica Neue',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#06061a;padding:24px 0;">
    <tr>
      <td align="center">
        <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">

          <!-- HEADER -->
          <tr>
            <td style="padding:0 0 24px 0;text-align:center;">
              <!-- Logo mark -->
              <div style="display:inline-block;width:48px;height:48px;border-radius:14px;
                background:linear-gradient(135deg,#6366f1,#a855f7);
                box-shadow:0 0 24px rgba(99,102,241,0.4);
                line-height:48px;font-size:22px;font-weight:900;color:#fff;
                text-align:center;margin-bottom:8px;">A</div>
              <div style="font-size:24px;font-weight:900;color:#ffffff;letter-spacing:-0.5px;">Agali</div>
              <div style="width:40px;height:2px;background:linear-gradient(90deg,#6366f1,#a855f7);
                margin:8px auto 0;border-radius:2px;"></div>
            </td>
          </tr>

          <!-- GREETING -->
          <tr>
            <td style="background:#0d0d2b;border-radius:20px 20px 0 0;
              padding:24px 24px 8px 24px;direction:{dir_attr};text-align:{align};">
              <h2 style="margin:0 0 6px;font-size:20px;font-weight:800;color:#ffffff;">
                {greeting}
              </h2>
              <p style="margin:0 0 16px;font-size:14px;color:#8888b8;line-height:1.5;">
                {s['intro']}
              </p>
            </td>
          </tr>

          <!-- PRODUCT CARDS -->
          <tr>
            <td style="background:#0d0d2b;padding:0 24px 24px;">
              <table width="100%" cellpadding="0" cellspacing="0">
                {product_cards}
              </table>
            </td>
          </tr>

          <!-- FOOTER -->
          <tr>
            <td style="background:#0a0a24;border-radius:0 0 20px 20px;
              padding:16px 24px;text-align:center;border-top:1px solid rgba(255,255,255,0.06);">
              <p style="margin:0 0 6px;font-size:11px;color:#44446a;line-height:1.5;">
                {s['footer_unsub']}
              </p>
              <a href="{alerts_url}" style="font-size:11px;color:#6366f1;text-decoration:none;">
                {s['footer_link']}
              </a>
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

            # ── 1. Load all active, non-expired observations ──────────────────
            cur.execute("""
                SELECT
                    o.id,
                    o.user_id,
                    o.item_code,
                    o.city,
                    o.min_discount_pct,
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

        # ── 2. Collect unique (item_code, city) pairs ─────────────────────────
        pairs = list({(o["item_code"], o["city"]) for o in observations})

        with conn.cursor() as cur:
            # ── 3. Bulk-fetch base prices ─────────────────────────────────────
            # MIN(price) per (item_code, city) from product_prices
            pair_values = ", ".join(
                f"('{ic}', '{ct}')" for ic, ct in pairs
            )
            cur.execute(f"""
                SELECT
                    p.item_code,
                    s.city,
                    MIN(pp.price) AS base_price
                FROM product_prices pp
                JOIN products p ON p.id = pp.product_id
                JOIN stores s ON s.id = pp.store_id
                WHERE (p.item_code, s.city) IN ({pair_values})
                GROUP BY p.item_code, s.city
            """)
            base_rows = cur.fetchall()
            base_price_map = {
                (r["item_code"], r["city"]): float(r["base_price"])
                for r in base_rows
            }

            # ── 4. Bulk-fetch best current promo prices ───────────────────────
            cur.execute(f"""
                SELECT
                    p.item_code,
                    s.city,
                    MIN(psi.promo_price)     AS promo_price,
                    MAX(pr.promotion_end_date) AS promotion_end_date
                FROM promotion_store_items psi
                JOIN products p ON p.id = psi.product_id
                JOIN stores s ON s.id = psi.store_id
                JOIN promotions pr
                     ON pr.chain_id = psi.chain_id
                    AND pr.promotion_id = psi.promotion_id
                WHERE (p.item_code, s.city) IN ({pair_values})
                  AND pr.promotion_end_date > NOW()
                  AND psi.promo_price IS NOT NULL
                  AND psi.promo_price > 0
                GROUP BY p.item_code, s.city
            """)
            promo_rows = cur.fetchall()
            promo_map = {
                (r["item_code"], r["city"]): {
                    "promo_price": float(r["promo_price"]),
                    "promo_end": r["promotion_end_date"],
                }
                for r in promo_rows
            }

            # ── 5. Fetch product names for email ──────────────────────────────
            item_codes = list({o["item_code"] for o in observations})
            placeholders = ", ".join(f"${i+1}" for i in range(len(item_codes)))
            cur.execute(
                f"SELECT item_code, item_name FROM products WHERE item_code IN ({placeholders})",
                item_codes,
            )
            name_map = {r["item_code"]: r["item_name"] for r in cur.fetchall()}

        # ── 6. Decide which observations to notify ────────────────────────────
        to_notify: list[dict] = []   # {obs, promo_price, promo_end, base_price}

        for obs in observations:
            key = (obs["item_code"], obs["city"])
            base_price = base_price_map.get(key)
            promo_info = promo_map.get(key)

            if base_price is None or promo_info is None:
                continue  # no promo active for this pair

            promo_price = promo_info["promo_price"]
            promo_end = promo_info["promo_end"]

            # Must exceed user's minimum threshold
            actual_pct = ((base_price - promo_price) / base_price) * 100
            if actual_pct < float(obs["min_discount_pct"]):
                continue

            last_price = obs["last_notified_price"]
            promo_expires_at = obs["promo_expires_at"]
            now = datetime.now(ISRAEL_TZ)

            promo_expired = (
                promo_expires_at is not None
                and promo_expires_at.replace(tzinfo=ISRAEL_TZ if promo_expires_at.tzinfo is None else None) < now
            )

            should_send = (
                last_price is None                         # first notification ever
                or promo_expired                           # previous promo expired → fresh evaluation
                or promo_price < float(last_price)        # price dropped further
            )

            if not should_send:
                continue

            to_notify.append({
                "obs_id": obs["id"],
                "user_id": obs["user_id"],
                "item_code": obs["item_code"],
                "item_name": name_map.get(obs["item_code"], obs["item_code"]),
                "base_price": base_price,
                "promo_price": promo_price,
                "promo_end": promo_end,
                "city": obs["city"],
            })

        if not to_notify:
            log.info("No qualifying price drops — no emails to send.")
            return

        log.info(f"{len(to_notify)} notification(s) to send across {len({n['user_id'] for n in to_notify})} user(s).")

        # ── 7. Group by user ──────────────────────────────────────────────────
        user_ids = list({n["user_id"] for n in to_notify})

        with conn.cursor() as cur:
            placeholders = ", ".join(f"${i+1}" for i in range(len(user_ids)))
            cur.execute(
                f"SELECT id, email, display_name, name, preferences FROM users WHERE id IN ({placeholders})",
                user_ids,
            )
            user_rows = cur.fetchall()

        user_map = {
            r["id"]: {
                "email": r["email"],
                "name": r.get("display_name") or r.get("name") or r["email"].split("@")[0],
                "lang": (r.get("preferences") or {}).get("language", "he"),
            }
            for r in user_rows
        }

        from collections import defaultdict
        grouped: dict[str, list[dict]] = defaultdict(list)
        for n in to_notify:
            grouped[n["user_id"]].append(n)

        # ── 8. Send one email per user ────────────────────────────────────────
        sent_obs_ids: list[tuple] = []   # (obs_id, promo_price, promo_end)
        emails_sent = 0

        for user_id, notifs in grouped.items():
            udata = user_map.get(user_id)
            if not udata:
                continue

            lang = udata["lang"] if udata["lang"] in STRINGS else "he"
            s = STRINGS[lang]
            subject = s["subject"]
            html = build_html_email(udata["name"], lang, notifs)

            ok = send_email(udata["email"], subject, html)
            if ok:
                emails_sent += 1
                log.info(f"✅ Email sent to {udata['email']} ({len(notifs)} product(s))")
                for n in notifs:
                    sent_obs_ids.append((n["obs_id"], n["promo_price"], n["promo_end"]))
            else:
                log.warning(f"⚠️  Failed to send to {udata['email']}")

        # ── 9. Bulk-update sent observations ──────────────────────────────────
        if sent_obs_ids:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """UPDATE observations
                       SET last_notified_price = %s,
                           last_notified_at    = NOW(),
                           promo_expires_at    = %s,
                           updated_at          = NOW()
                       WHERE id = %s""",
                    [(promo_price, promo_end, obs_id) for obs_id, promo_price, promo_end in sent_obs_ids],
                )
            conn.commit()
            log.info(f"Updated {len(sent_obs_ids)} observation row(s).")

        log.info(f"Done — {emails_sent} email(s) sent.")

    except Exception as exc:
        conn.rollback()
        log.error(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
