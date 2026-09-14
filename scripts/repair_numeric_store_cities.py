"""Replace numeric locality placeholders with exact official CBS Hebrew names."""
import argparse
import re
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import requests

from enrich_store_details import database_url


def resolve(code):
    response = requests.get(
        f'https://api.cbs.gov.il/Dictionary/Geo/localities/{code}',
        headers={'User-Agent': 'AgaliStoreCityRepair/1.0 (+https://agali.co.il)'}, timeout=15,
    )
    response.raise_for_status()
    items = response.json()['dictionary']['data']['localities']['items']
    locality = items['localities'] if isinstance(items, dict) else items[0]['localities']
    return locality.get('name_heb')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    with psycopg2.connect(database_url()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT city FROM stores WHERE city ~ '^[0-9]+(\\.0)?$' AND city !~ '^0(\\.0)?$'")
            raw_codes = [row[0] for row in cursor.fetchall()]
            def load(raw):
                code = re.sub(r'\.0$', '', raw)
                try:
                    name = resolve(code)
                except (requests.RequestException, KeyError, IndexError, TypeError):
                    name = None
                return raw, name
            with ThreadPoolExecutor(max_workers=6) as pool:
                resolved = {raw: name for raw, name in pool.map(load, raw_codes) if name}
            if args.apply:
                for raw, name in resolved.items():
                    cursor.execute("""UPDATE stores SET city=%s,
                        geocode_status=CASE WHEN geocode_status='verified' THEN geocode_status ELSE 'review_required' END,
                        geocode_reason=CASE WHEN geocode_status='verified' THEN geocode_reason ELSE 'city_resolved_from_cbs_code' END,
                        store_details_checked_at=NULL
                        WHERE city=%s AND city ~ '^[0-9]+(\\.0)?$'""", (name, raw))
            else:
                connection.rollback()
            print({'mode':'apply' if args.apply else 'dry-run','codes':len(raw_codes),
                   'resolved_codes':len(resolved),'unresolved_codes':len(raw_codes)-len(resolved)})


if __name__ == '__main__':
    main()
