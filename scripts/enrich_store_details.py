"""Refresh store phone numbers and opening hours from official retailer pages.

The command is read-only by default. Pass --apply to persist strictly matched data.
Failed or ambiguous matches never erase existing values.
"""

from __future__ import annotations

import argparse
import html
import os
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import psycopg2
import requests
from bs4 import BeautifulSoup


RAMI_CHAIN_IDS = ("7290058140886", "7291056200008")
SUPER_PHARM_CHAIN_ID = "7290172900007"
RAMI_URL = "https://www.rami-levy.co.il/he/stores"
SUPER_PHARM_URL = "https://shop.super-pharm.co.il/branches/branchPage?code={}"
HEADERS = {"User-Agent": "AgaliStoreDetails/1.0 (+https://agali.co.il)"}


@dataclass
class OfficialStore:
    address: str
    phone: str | None
    hours: str | None
    source_url: str
    latitude: float | None = None
    longitude: float | None = None


def clean_text(value: str | None) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<br\s*/?>|</p>", "\n", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    lines = [re.sub(r"\s+", " ", line).strip(" \t:-") for line in value.splitlines()]
    return "\n".join(line for line in lines if line)


def normalized(value: str | None) -> str:
    value = clean_text(value).lower().replace("־", "-")
    return re.sub(r"[^0-9a-zא-ת]+", "", value)


def house_number(value: str | None) -> str | None:
    match = re.search(r"(?:^|\D)(\d{1,4})(?:\D|$)", value or "")
    return match.group(1) if match else None


def strict_address_match(db_address: str, db_city: str, official_address: str) -> bool:
    wanted_number = house_number(db_address)
    actual_number = house_number(official_address)
    if wanted_number and wanted_number != actual_number:
        return False
    compact_official = normalized(official_address)
    if db_city and normalized(db_city) not in compact_official:
        return False
    words = [normalized(word) for word in re.split(r"[\s,]+", db_address) if len(normalized(word)) >= 3]
    return bool(words) and any(word in compact_official for word in words)


def database_url() -> str:
    value = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("POSTGRESQL_URL or DATABASE_URL is required")
    if os.getenv("AGALI_LOCAL_DB_TUNNEL") == "1":
        value = value.replace("@db:", "@127.0.0.1:")
    return value


def fetch(url: str) -> str:
    # Local corporate proxies can replace certificates. This opt-in exists only for
    # workstation smoke tests; the scheduled workflow keeps TLS verification on.
    verify_tls = os.getenv("AGALI_ALLOW_INSECURE_TLS") != "1"
    response = requests.get(url, headers=HEADERS, timeout=30, verify=verify_tls)
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text


def extract_super_pharm(page: str, source_url: str) -> OfficialStore | None:
    address = re.search(r'class="branch-address"[^>]*>(.*?)</span>', page, re.I | re.S)
    phone = re.search(r'data-tel="([^"]+)"', page, re.I)
    soup = BeautifulSoup(page, 'html.parser')
    hour_blocks = []
    for wrapper in soup.select('.wrapper-opening-hours'):
        title = wrapper.find_previous_sibling()
        title_text = clean_text(str(title)) if title else ''
        if 'Erroca' in title_text:
            continue
        content = clean_text(str(wrapper))
        if content and (not hour_blocks or 'חג' in title_text):
            hour_blocks.append(content)
    coordinates = re.search(r'data-latitude="([\d.]+)"\s+data-longitude="([\d.]+)"', page, re.I)
    if not address:
        return None
    return OfficialStore(
        address=clean_text(address.group(1)),
        phone=clean_text(phone.group(1)) if phone else None,
        hours='\n'.join(hour_blocks) or None,
        source_url=source_url,
        latitude=float(coordinates.group(1)) if coordinates else None,
        longitude=float(coordinates.group(2)) if coordinates else None,
    )


def extract_rami_levy(page: str) -> list[OfficialStore]:
    stores: list[OfficialStore] = []
    chunks = re.split(r'<button[^>]+class="[^"]*store-btn[^"]*"', page, flags=re.I)[1:]
    for chunk in chunks:
        address = re.search(r'<small[^>]*>(.*?)</small>', chunk, re.I | re.S)
        hours = re.search(r'>\s*שעות פתיחה\s*</h4>\s*<div[^>]*>(.*?)</div>', chunk, re.I | re.S)
        phone = re.search(r'>\s*טלפון\s*</h4>\s*<div[^>]*>(.*?)</div>', chunk, re.I | re.S)
        if address:
            stores.append(OfficialStore(
                address=clean_text(address.group(1)),
                phone=clean_text(phone.group(1)) if phone else None,
                hours=clean_text(hours.group(1)) if hours else None,
                source_url=RAMI_URL,
            ))
    return stores


def valid_israel_coordinates(official: OfficialStore) -> bool:
    return bool(
        official.latitude is not None and official.longitude is not None
        and 29.4 <= official.latitude <= 33.4 and 34.1 <= official.longitude <= 35.95
    )


def persist(
    cursor,
    db_id: int,
    official: OfficialStore,
    source: str,
    apply: bool,
    geocode_reason: str = "strict_official_store_match",
) -> bool:
    if not apply:
        return valid_israel_coordinates(official)
    has_coordinates = valid_israel_coordinates(official)
    cursor.execute(
        """
        UPDATE stores
           SET phone = COALESCE(%s, phone),
               website_url = %s,
               opening_hours_text = COALESCE(%s, opening_hours_text),
               opening_hours_updated_at = CASE WHEN %s IS NOT NULL THEN NOW() ELSE opening_hours_updated_at END,
               store_details_source = %s,
               store_details_source_url = %s,
               store_details_updated_at = NOW(),
               store_details_checked_at = NOW(),
               latitude = CASE WHEN geocode_status <> 'verified' AND %s THEN %s ELSE latitude END,
               longitude = CASE WHEN geocode_status <> 'verified' AND %s THEN %s ELSE longitude END,
               geocode_status = CASE WHEN geocode_status <> 'verified' AND %s THEN 'verified' ELSE geocode_status END,
               geocode_source = CASE WHEN geocode_status <> 'verified' AND %s THEN %s ELSE geocode_source END,
               geocode_precision = CASE WHEN geocode_status <> 'verified' AND %s THEN 'official_store' ELSE geocode_precision END,
               geocode_reason = CASE WHEN geocode_status <> 'verified' AND %s THEN %s ELSE geocode_reason END,
               geocode_matched_address = CASE WHEN geocode_status <> 'verified' AND %s THEN %s ELSE geocode_matched_address END,
               geocode_verified_at = CASE WHEN geocode_status <> 'verified' AND %s THEN NOW() ELSE geocode_verified_at END
         WHERE id = %s
        """,
        (
            official.phone, official.source_url, official.hours, official.hours, source, official.source_url,
            has_coordinates, official.latitude, has_coordinates, official.longitude,
            has_coordinates, has_coordinates, source, has_coordinates, has_coordinates, geocode_reason,
            has_coordinates, official.address, has_coordinates, db_id,
        ),
    )
    return has_coordinates


def mark_checked(cursor, db_id: int, apply: bool) -> None:
    if apply:
        cursor.execute("UPDATE stores SET store_details_checked_at = NOW() WHERE id = %s", (db_id,))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Limit each source for smoke tests")
    args = parser.parse_args()

    connection = psycopg2.connect(database_url())
    connection.autocommit = False
    cursor = connection.cursor()
    counts = {"super_pharm_matched": 0, "super_pharm_rejected": 0, "rami_matched": 0, "rami_rejected": 0, "coordinates_verified": 0}

    cursor.execute(
        """SELECT id, store_id, address, city FROM stores
             WHERE chain_id = %s
               AND (geocode_status = 'review_required'
                    OR store_details_checked_at IS NULL
                    OR store_details_checked_at < NOW() - INTERVAL '7 days')
             ORDER BY store_details_checked_at NULLS FIRST, id""",
        (SUPER_PHARM_CHAIN_ID,),
    )
    super_pharm_rows = cursor.fetchall()[: args.limit or None]
    def load_super_pharm(row):
        db_id, store_id, address, city = row
        url = SUPER_PHARM_URL.format(store_id)
        try:
            return row, extract_super_pharm(fetch(url), url), False
        except requests.RequestException:
            return row, None, True

    # A small pool keeps the weekly refresh quick without aggressively hitting the site.
    with ThreadPoolExecutor(max_workers=2) as pool:
        loaded_super_pharm = list(pool.map(load_super_pharm, super_pharm_rows))
    for (db_id, store_id, address, city), official, fetch_failed in loaded_super_pharm:
        if fetch_failed:
            counts["super_pharm_rejected"] += 1
            continue
        address_matches = bool(official and strict_address_match(address or "", city or "", official.address))
        # store_id is Super-Pharm's own branch code and addresses this exact
        # official page. It is more reliable than stale street text.
        official_id_matches = bool(store_id and official and valid_israel_coordinates(official))
        if official and (address_matches or official_id_matches):
            reason = "strict_official_store_match" if address_matches else "official_store_id_match"
            if persist(cursor, db_id, official, "super-pharm-official", args.apply, reason):
                counts["coordinates_verified"] += 1
            counts["super_pharm_matched"] += 1
        else:
            counts["super_pharm_rejected"] += 1
            mark_checked(cursor, db_id, args.apply)

    try:
        rami_official = extract_rami_levy(fetch(RAMI_URL))
    except requests.RequestException:
        rami_official = []
    cursor.execute(
        """SELECT id, address, city FROM stores
             WHERE chain_id = ANY(%s)
               AND (store_details_checked_at IS NULL OR store_details_checked_at < NOW() - INTERVAL '7 days')
             ORDER BY store_details_checked_at NULLS FIRST, id""",
        (list(RAMI_CHAIN_IDS),),
    )
    rami_rows = cursor.fetchall()[: args.limit or None]
    used: set[int] = set()
    for db_id, address, city in rami_rows:
        matches = [
            (index, store) for index, store in enumerate(rami_official)
            if index not in used and strict_address_match(address or "", city or "", store.address)
        ]
        if len(matches) == 1:
            index, official = matches[0]
            used.add(index)
            persist(cursor, db_id, official, "rami-levy-official", args.apply)
            counts["rami_matched"] += 1
        else:
            counts["rami_rejected"] += 1
            if rami_official:
                mark_checked(cursor, db_id, args.apply)

    if args.apply:
        connection.commit()
    else:
        connection.rollback()
    cursor.close()
    connection.close()
    print({"mode": "apply" if args.apply else "dry-run", **counts})


if __name__ == "__main__":
    main()
