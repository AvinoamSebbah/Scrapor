"""Fill missing store details from OpenStreetMap with strict spatial/name matching."""

from __future__ import annotations

import argparse
import math
import os
import re
import time

import psycopg2
import requests

OVERPASS_URLS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
OVERPASS_QUERY = """
[out:json][timeout:60];
(
  nwr["shop"~"^(supermarket|convenience|chemist)$"]({bbox});
  nwr["amenity"="pharmacy"]({bbox});
);
out center tags;
"""
ISRAEL_TILES = (
    (29.4, 34.1, 31.4, 35.0), (29.4, 35.0, 31.4, 35.95),
    (31.4, 34.1, 32.4, 35.0), (31.4, 35.0, 32.4, 35.95),
    (32.4, 34.1, 33.4, 35.0), (32.4, 35.0, 33.4, 35.95),
)


def database_url() -> str:
    value = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("POSTGRESQL_URL or DATABASE_URL is required")
    if os.getenv("AGALI_LOCAL_DB_TUNNEL") == "1":
        value = value.replace("@db:", "@127.0.0.1:")
    return value


def normalized(value: str | None) -> str:
    value = (value or "").lower().replace("־", "-")
    return re.sub(r"[^0-9a-zא-ת]+", "", value)


def words(value: str | None) -> set[str]:
    ignored = {"ישראל", "שיווק", "רשת", "בעמ", "סניף", "store", "israel"}
    return {token for token in re.split(r"[^0-9a-zא-ת]+", (value or "").lower()) if len(token) >= 2 and token not in ignored}


def one_number(value: str | None) -> str | None:
    values = re.findall(r"(?:^|\D)(\d{1,4})(?=\D|$)", value or "")
    return values[0] if len(values) == 1 else None


def canonical_city(value: str | None) -> str:
    return normalized(value).replace('קריית', 'קרית').replace('תלאביביפו', 'תלאביב')


def exact_osm_address_matches(store: dict, element: dict) -> bool:
    tags = element.get('tags', {})
    wanted_number = one_number(store.get('address'))
    if not wanted_number or wanted_number != one_number(tags.get('addr:housenumber')):
        return False
    if canonical_city(store.get('city')) != canonical_city(tags.get('addr:city')):
        return False
    street = normalized(tags.get('addr:street'))
    ignored = words(store.get('city')) | {'רחוב', 'קניון', 'מרכז'}
    street_words = [word for word in words(store.get('address')) if word not in ignored and not word.isdigit()]
    return bool(street and street_words) and all(normalized(word) in street for word in street_words)


def distance_meters(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    north = (a_lat - b_lat) * 111_320
    east = (a_lon - b_lon) * 111_320 * math.cos(math.radians((a_lat + b_lat) / 2))
    return math.hypot(north, east)


def candidate_point(element: dict) -> tuple[float, float] | None:
    lat = element.get("lat") or element.get("center", {}).get("lat")
    lon = element.get("lon") or element.get("center", {}).get("lon")
    return (float(lat), float(lon)) if lat is not None and lon is not None else None


def name_matches(store: dict, element: dict) -> bool:
    tags = element.get("tags", {})
    candidate = " ".join(filter(None, [tags.get("name"), tags.get("brand"), tags.get("operator")]))
    if not candidate:
        return False
    candidate_compact = normalized(candidate)
    references = [store["chain_name"], store["store_name"]]
    if any(candidate_compact and (candidate_compact in normalized(ref) or normalized(ref) in candidate_compact) for ref in references if ref):
        return True
    reference_words = words(" ".join(filter(None, references)))
    return len(reference_words & words(candidate)) >= 2


def fetch_elements() -> list[dict]:
    verify_tls = os.getenv("AGALI_ALLOW_INSECURE_TLS") != "1"
    all_elements = {}
    for tile in ISRAEL_TILES:
        last_error = None
        query = OVERPASS_QUERY.format(bbox=','.join(str(value) for value in tile))
        for attempt in range(3):
            url = OVERPASS_URLS[attempt % len(OVERPASS_URLS)]
            try:
                response = requests.post(
                    url, data={"data": query},
                    headers={"User-Agent": "AgaliStoreDetails/1.0 (+https://agali.co.il)"},
                    timeout=90, verify=verify_tls,
                )
                response.raise_for_status()
                for element in response.json().get("elements", []):
                    all_elements[(element.get('type'), element.get('id'))] = element
                break
            except requests.RequestException as error:
                last_error = error
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
        else:
            print({'warning': 'overpass_tile_unavailable', 'tile': tile, 'error': str(last_error)})
    if not all_elements:
        raise RuntimeError('Every Overpass tile request failed')
    return list(all_elements.values())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    elements = fetch_elements()
    points = [(element, candidate_point(element)) for element in elements]
    points = [(element, point) for element, point in points if point]

    connection = psycopg2.connect(database_url())
    cursor = connection.cursor()
    cursor.execute("""SELECT id,chain_name,store_name,address,city FROM stores
                       WHERE geocode_status='review_required' AND address IS NOT NULL AND city IS NOT NULL""")
    review_columns = [item.name for item in cursor.description]
    review_stores = [dict(zip(review_columns, row)) for row in cursor.fetchall()]
    geocoded = 0
    for store in review_stores:
        candidates = [(element, point) for element, point in points
                      if name_matches(store, element) and exact_osm_address_matches(store, element)]
        if len(candidates) != 1:
            continue
        element, point = candidates[0]
        geocoded += 1
        if args.apply:
            cursor.execute("""UPDATE stores SET latitude=%s,longitude=%s,geocode_status='verified',
                geocode_source='openstreetmap-store-poi',geocode_precision='official_poi',
                geocode_reason='exact_osm_chain_and_address_match',geocode_matched_address=%s,
                geocode_verified_at=NOW() WHERE id=%s AND geocode_status='review_required'""",
                (point[0],point[1],element.get('tags',{}).get('name'),store['id']))
    cursor.execute(
        """
        SELECT id, chain_name, store_name, latitude, longitude
          FROM stores
         WHERE geocode_status = 'verified'
           AND (opening_hours_text IS NULL OR phone IS NULL OR website_url IS NULL)
        """
    )
    columns = [item.name for item in cursor.description]
    stores = [dict(zip(columns, row)) for row in cursor.fetchall()]
    matched = 0
    for store in stores:
        nearby = []
        for element, point in points:
            distance = distance_meters(store["latitude"], store["longitude"], point[0], point[1])
            if distance <= 80 and name_matches(store, element):
                nearby.append((distance, element))
        nearby.sort(key=lambda item: item[0])
        if not nearby or (len(nearby) > 1 and nearby[1][0] - nearby[0][0] < 20):
            continue
        distance, element = nearby[0]
        tags = element.get("tags", {})
        phone = tags.get("contact:phone") or tags.get("phone")
        website = tags.get("contact:website") or tags.get("website")
        hours = tags.get("opening_hours")
        if not any((phone, website, hours)):
            continue
        matched += 1
        if args.apply:
            osm_type = element.get("type", "node")
            osm_id = element.get("id")
            source_url = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"
            cursor.execute(
                """
                UPDATE stores
                   SET phone = COALESCE(phone, %s),
                       website_url = COALESCE(website_url, %s),
                       opening_hours_text = COALESCE(opening_hours_text, %s),
                       opening_hours_updated_at = CASE WHEN opening_hours_text IS NULL AND %s IS NOT NULL THEN NOW() ELSE opening_hours_updated_at END,
                       store_details_source = COALESCE(store_details_source, 'openstreetmap'),
                       store_details_source_url = COALESCE(store_details_source_url, %s),
                       store_details_updated_at = NOW()
                 WHERE id = %s
                """,
                (phone, website, hours, hours, source_url, store["id"]),
            )
    if args.apply:
        connection.commit()
    else:
        connection.rollback()
    cursor.close()
    connection.close()
    print({"mode": "apply" if args.apply else "dry-run", "osm_elements": len(elements),
           "review_stores": len(review_stores), "geocoded": geocoded, "stores": len(stores), "matched": matched})


if __name__ == "__main__":
    main()
