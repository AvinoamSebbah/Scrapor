"""One-time, cached recovery of exact street addresses with OSM Nominatim.

This is intentionally not part of the recurring workflow. The public service's
policy permits small one-time single-threaded batches, requires at most one
request per second, and requires caching. Successful coordinates and failed
exact-match attempts are cached in PostgreSQL so an address is never queried
twice. Ambiguous or incomplete addresses remain ``review_required``.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import time

import psycopg2
import requests


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "AgaliStoreLocationRecovery/1.0 (+https://agali.co.il)"
ISRAEL_BOUNDS = (29.4, 33.4, 34.1, 35.95)
ATTEMPT_MARKER = "nominatim_checked_no_exact_match"
IGNORED_WORDS = {
    "רחוב", "רח", "דרך", "שדרות", "שד", "קניון", "מרכז", "מסחרי",
    "אזור", "תעשיה", "התעשיה", "ישראל", "קומה", "פינת", "street", "road",
}


def database_url() -> str:
    value = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("POSTGRESQL_URL or DATABASE_URL is required")
    if os.getenv("AGALI_LOCAL_DB_TUNNEL") == "1":
        value = value.replace("@db:", "@127.0.0.1:")
    return value


def normalized(value: str | None) -> str:
    value = (value or "").lower().replace("־", "-")
    value = value.replace("קריית", "קרית").replace("תל אביב-יפו", "תל אביב")
    return re.sub(r"[^0-9a-zא-ת]+", "", value)


def words(value: str | None) -> set[str]:
    return {
        normalized(token) for token in re.split(r"[^0-9a-zא-ת]+", value or "")
        if len(normalized(token)) >= 2 and normalized(token) not in IGNORED_WORDS
    }


def one_house_number(value: str | None) -> str | None:
    numbers = re.findall(r"(?<!\d)(\d{1,4})(?!\d)", value or "")
    return numbers[0] if len(numbers) == 1 else None


def distance_meters(first: tuple[float, float], second: tuple[float, float]) -> float:
    north = (first[0] - second[0]) * 111_320
    east = (first[1] - second[1]) * 111_320 * math.cos(math.radians((first[0] + second[0]) / 2))
    return math.hypot(north, east)


def exact_candidates(address: str, city: str, results: list[dict]) -> list[dict]:
    wanted_number = one_house_number(address)
    if not wanted_number:
        return []
    city_tokens = words(city)
    street_tokens = {
        token for token in words(address)
        if token not in city_tokens and not token.isdigit()
    }
    if not street_tokens:
        return []
    matches = []
    for result in results:
        details = result.get("address") or {}
        if one_house_number(str(details.get("house_number") or "")) != wanted_number:
            continue
        display = " ".join(str(value) for value in [result.get("display_name"), *details.values()] if value)
        if normalized(city) not in normalized(display):
            continue
        if not all(token in normalized(display) for token in street_tokens):
            continue
        try:
            latitude, longitude = float(result["lat"]), float(result["lon"])
        except (KeyError, TypeError, ValueError):
            continue
        south, north, west, east = ISRAEL_BOUNDS
        if south <= latitude <= north and west <= longitude <= east:
            matches.append(result)
    return matches


def unique_point(candidates: list[dict]) -> tuple[float, float] | None:
    if not candidates:
        return None
    points = [(float(item["lat"]), float(item["lon"])) for item in candidates]
    anchor = points[0]
    if any(distance_meters(anchor, point) > 40 for point in points[1:]):
        return None
    return anchor


def fetch_address(session: requests.Session, address: str, city: str) -> list[dict]:
    verify_tls = os.getenv("AGALI_ALLOW_INSECURE_TLS") != "1"
    response = session.get(
        NOMINATIM_URL,
        params={
            "q": f"{address}, {city}, ישראל",
            "format": "jsonv2",
            "addressdetails": 1,
            "countrycodes": "il",
            "limit": 5,
        },
        headers={"User-Agent": USER_AGENT, "Referer": "https://agali.co.il"},
        timeout=30,
        verify=verify_tls,
    )
    response.raise_for_status()
    value = response.json()
    return value if isinstance(value, list) else []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    connection = psycopg2.connect(database_url())
    cursor = connection.cursor()
    cursor.execute(
        """SELECT id,store_name,address,city FROM stores
             WHERE geocode_status='review_required'
               AND address IS NOT NULL AND city IS NOT NULL
               AND COALESCE(geocode_reason,'') NOT LIKE %s
             ORDER BY id""",
        (f"%{ATTEMPT_MARKER}%",),
    )
    rows = cursor.fetchall()[: args.limit or None]
    session = requests.Session()
    confirmed = ambiguous = incomplete = failed = 0
    last_request_at = 0.0
    for db_id, store_name, address, city in rows:
        compact_name = normalized(store_name)
        if any(marker in compact_name for marker in ("אונליין", "משלוחים", "online", "וולט")):
            incomplete += 1
            results = []
        elif not one_house_number(address):
            incomplete += 1
            results = []
        else:
            # Stay below the OSMF absolute maximum of one request per second.
            wait = 1.05 - (time.monotonic() - last_request_at)
            if wait > 0:
                time.sleep(wait)
            try:
                results = fetch_address(session, address, city)
                last_request_at = time.monotonic()
            except requests.RequestException:
                # A service failure is not a cached negative result; a later
                # manual run may retry it.
                failed += 1
                continue
        candidates = exact_candidates(address, city, results)
        point = unique_point(candidates)
        if point:
            confirmed += 1
            if args.apply:
                cursor.execute(
                    """UPDATE stores SET latitude=%s,longitude=%s,geocode_status='verified',
                        geocode_source='openstreetmap-nominatim',geocode_precision='exact_address',
                        geocode_reason='exact_osm_city_street_house_number',
                        geocode_matched_address=%s,geocode_verified_at=NOW()
                        WHERE id=%s AND geocode_status='review_required'""",
                    (point[0], point[1], candidates[0].get("display_name"), db_id),
                )
        else:
            ambiguous += int(bool(candidates))
            if args.apply:
                cursor.execute(
                    """UPDATE stores
                          SET geocode_reason=concat_ws(';',NULLIF(geocode_reason,''),%s)
                        WHERE id=%s AND geocode_status='review_required'""",
                    (ATTEMPT_MARKER, db_id),
                )
    if args.apply:
        connection.commit()
    else:
        connection.rollback()
    cursor.close()
    connection.close()
    print({
        "mode": "apply" if args.apply else "dry-run",
        "due": len(rows),
        "confirmed": confirmed,
        "ambiguous": ambiguous,
        "incomplete_or_virtual": incomplete,
        "request_failures": failed,
    })


if __name__ == "__main__":
    main()
