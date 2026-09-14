"""Fetch whole official branch catalogs; retain existing details on failed refreshes."""
import argparse
import ast
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from urllib.parse import urljoin
from urllib.parse import parse_qs, urlparse

import psycopg2
import requests
from bs4 import BeautifulSoup
from enrich_store_details import HEADERS, OfficialStore, clean_text, database_url, fetch, normalized, house_number, persist

GOODPHARM_URL = 'https://goodpharm.co.il/allstores'
CARREFOUR_ACCESSIBILITY_URL = 'https://negishut.carrefour.co.il/'
DOR_ALON_URL = 'https://www.doralon.co.il/station/'
BE_BRANCHES_URL = 'https://www.bestore.co.il/online/he/branch-info/beBranches'
SHUFERSAL_BRANCHES_URL = 'https://www.shufersal.co.il/corp/branches'
SHUFERSAL_CHAIN_ID = '7290027600007'
YOCHANANOF_BRANCHES_URL = 'https://yochananof.co.il/branches'
YOCHANANOF_CHAIN_ID = '7290803800003'
RAMI_LEVY_STORES_API = 'https://www.rami-levy.co.il/api/stores'
RAMI_LEVY_STORES_URL = 'https://www.rami-levy.co.il/he/stores'
RAMI_LEVY_CHAIN_IDS = ('7290058140886', '7291056200008')
RAMI_LEVY_NEIGHBORHOOD_CHAIN_ID = '7291056200008'
# Stable branch codes from the chain's mandatory price files, paired with the
# exact current label in Rami Levy's own public branch API. Four price-feed
# rows are deliberately absent because the API has no unambiguous equivalent.
RAMI_LEVY_NEIGHBORHOOD_STORE_LABELS = {
    '299': 'פרדסיה (בשכונה)',
    '302': 'שטמפפר פתח תקווה (בשכונה)',
    '306': 'הרצל חדרה (בשכונה)',
    '307': 'אבנת פתח תקווה (בשכונה)',
    '309': 'כפר סבא (בשכונה)',
    '311': 'קריון (בשכונה)',
    '312': 'עפולה (בשכונה)',
    '314': 'הנשיא חדרה (בשכונה)',
    '316': 'רמי לוי בשכונה- אלי כהן אשקלון',
    '317': 'רמי לוי בשכונה- אשדוד רובע י"ב',
    '319': 'רמי לוי בשכונה - אור יהודה',
    '320': 'ראש העין (בשכונה)',
    '321': 'גבעת שמואל (בשכונה)',
    '323': 'הרטום נתניה (בשכונה)',
    '324': 'רשל"צ  (בשכונה)',
    '325': 'רגר באר שבע (בשכונה)',
    '329': 'בן יהודה 23  (בשכונה)',
    '330': 'אסתר המלכה תל אביב(בשכונה)',
    '331': 'רוטשילד פתח תקווה (בשכונה)',
    '332': 'רעננה (בשכונה)',
    '334': 'הוד השרון (בשכונה)',
    '335': 'בן יהודה 174 (בשכונה)',
    '336': 'הרצליה (בשכונה)',
    '337': 'גבעתיים (בשכונה)',
    '338': 'החשמונאים (בשכונה)',
    '339': 'מ.ישראל רשל"צ (בשכונה)',
    '340': 'שוקן (בשכונה)',
    '401': 'חולון (בשכונה)',
    '403': 'ויצמן נתניה (בשכונה)',
    '404': 'רמת גן (בשכונה)',
}
RAMI_LEVY_MAIN_STORE_LABELS = {
    # Main-chain rows whose published label/address changed slightly.
    '1': 'אומן',
    '7': 'קניון מבשרת רמי לוי החדש',
    '9': 'קסטינה',
    '16': 'איילון בני ברק',
    '20': 'זכרון',
    '25': 'מודיעין ישפרו',
    '26': 'אשדוד',
    '37': 'נתניה האורזים',
    '54': 'ביג קריית גת',
    '62': 'חיפה',
    '66': 'מגדל העמק',
    '22': 'גבעת שאול מהדרין',
    # Sub-brand rows emitted under the main chain in the price files. These
    # are duplicate physical branches of the audited neighborhood catalog.
    '708': 'רמי לוי בשכונה - אור יהודה',
    '709': 'רמי לוי בשכונה- אשדוד רובע י"ב',
    '710': 'רמי לוי בשכונה- אלי כהן אשקלון',
    '711': 'רגר באר שבע (בשכונה)',
    '712': 'גבעת שמואל (בשכונה)',
    '713': 'גבעתיים (בשכונה)',
    '714': 'הוד השרון (בשכונה)',
    '715': 'הרצליה (בשכונה)',
    '716': 'הרצל חדרה (בשכונה)',
    '717': 'הנשיא חדרה (בשכונה)',
    '718': 'חולון (בשכונה)',
    '719': 'כפר סבא (בשכונה)',
    '720': 'הרטום נתניה (בשכונה)',
    '721': 'ויצמן נתניה (בשכונה)',
    '722': 'עפולה (בשכונה)',
    '723': 'פרדסיה (בשכונה)',
    '724': 'אבנת פתח תקווה (בשכונה)',
    '725': 'שטמפפר פתח תקווה (בשכונה)',
    '726': 'רוטשילד פתח תקווה (בשכונה)',
    '727': 'קריון (בשכונה)',
    '728': 'ראש העין (בשכונה)',
    '729': 'רשל"צ  (בשכונה)',
    '730': 'מ.ישראל רשל"צ (בשכונה)',
    '731': 'רמת גן (בשכונה)',
    '732': 'רעננה (בשכונה)',
    '733': 'בן יהודה 23  (בשכונה)',
    '734': 'אסתר המלכה תל אביב(בשכונה)',
    '735': 'החשמונאים (בשכונה)',
    '736': 'שוקן (בשכונה)',
    '737': 'בן יהודה 174 (בשכונה)',
}
SAPIR_BRANCHES_URL = 'https://sapir-group.co.il/%D7%A1%D7%A0%D7%99%D7%A4%D7%99%D7%9D/'
SAPIR_CHAIN_ID = '7290058156016'
CITY_MARKET_URL = 'https://city-market.co.il/'
CITY_MARKET_CHAIN_ID = '7290000000003'
WIX_CODE_APP_ID = '675bbcef-18d8-41f5-800e-131ec9e08762'


def extract_goodpharm(page):
    return [item['store'] for item in extract_goodpharm_catalog(page)]


def extract_goodpharm_catalog(page):
    soup = BeautifulSoup(page, 'html.parser')
    result = []
    for title in soup.select('h1 a[href*="pharm_storelocation"]'):
        card = title.find_parent('div', class_='elementor-widget-wrap')
        address = card.select_one('.elementor-widget-text-editor .elementor-widget-container')
        if not address:
            continue
        phone = card.select_one('a[href^="tel:"]')
        hours = [item.get_text(' ', strip=True) for item in card.select('li') if item.select_one('.fa-clock')]
        waze = card.select_one('a[href*="waze.com/"]')
        result.append({
            'name': clean_text(title.get_text(' ', strip=True)),
            'waze_url': waze['href'] if waze else None,
            'store': OfficialStore(
                address.get_text(' ', strip=True),
                phone['href'][4:] if phone else None,
                '\n'.join(hours) or None,
                title['href'],
            ),
        })
    return result


def extract_waze_destination(page):
    """Read the public destination point behind an official retailer link."""
    match = re.search(
        r'"routing":\{"to":.*?"latLng":\{"lat":(-?\d+(?:\.\d+)?),"lng":(-?\d+(?:\.\d+)?)\}',
        page,
        re.S,
    )
    if not match:
        return None
    latitude, longitude = float(match.group(1)), float(match.group(2))
    if not (29.4 <= latitude <= 33.4 and 34.1 <= longitude <= 35.95):
        return None
    return latitude, longitude


def fetch_waze_destination(url):
    direct = extract_waze_url_point(url)
    if direct:
        return direct
    verify_tls = os.getenv('AGALI_ALLOW_INSECURE_TLS') != '1'
    response = requests.get(url, headers=HEADERS, timeout=30, verify=verify_tls)
    response.raise_for_status()
    final_point = extract_waze_url_point(response.url)
    if final_point:
        return final_point
    response.encoding = 'utf-8'
    return extract_waze_destination(response.text)


def extract_waze_url_point(url):
    """Read a literal ``ll=lat,lon`` point from an official Waze URL."""
    values = parse_qs(urlparse(url or '').query).get('ll', [])
    raw_point = values[0] if len(values) == 1 else None
    if raw_point is None:
        to_values = parse_qs(urlparse(url or '').query).get('to', [])
        if len(to_values) == 1 and to_values[0].startswith('ll.'):
            raw_point = to_values[0][3:]
    if raw_point is None:
        return None
    match = re.fullmatch(r'\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*', raw_point)
    if not match:
        return None
    latitude, longitude = float(match.group(1)), float(match.group(2))
    if not (29.4 <= latitude <= 33.4 and 34.1 <= longitude <= 35.95):
        return None
    return latitude, longitude


def jquery_form_pairs(value, prefix=''):
    """Serialize nested values the same way jQuery does for JetEngine AJAX."""
    if isinstance(value, dict):
        for key, item in value.items():
            child = f'{prefix}[{key}]' if prefix else key
            yield from jquery_form_pairs(item, child)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from jquery_form_pairs(item, f'{prefix}[]')
    else:
        yield prefix, str(value).lower() if isinstance(value, bool) else str(value)


def extract_sapir_cards(page):
    """Parse branch cards rendered by Super Sapir's official locator."""
    soup = BeautifulSoup(page, 'html.parser')
    result = []
    for card in soup.select('.jet-listing-grid__item[data-post-id]'):
        city_node = card.select_one('.jet-listing-dynamic-terms__link')
        home_icon = card.select_one('.jet-listing-dynamic-field__icon.fa-home')
        address_node = (home_icon.find_next_sibling(class_='jet-listing-dynamic-field__content')
                        if home_icon else None)
        if not city_node or not address_node:
            continue
        hours = []
        for heading in card.select('p.elementor-heading-title'):
            day = clean_text(heading.get_text(' ', strip=True))
            if not (day.startswith('יום ') or day == 'מוצאי שבת'):
                continue
            section = heading.find_parent('section')
            values = [clean_text(node.get_text(' ', strip=True))
                      for node in section.select('.jet-listing-dynamic-field__content')]
            values = [value.strip(' -') for value in values if value.strip(' -')]
            hours.append(f"{day}: {'-'.join(values) if values else 'סגור'}")
        phone = None
        for link in card.select('a[href]'):
            label = clean_text(link.get_text(' ', strip=True))
            digits = re.sub(r'\D', '', label)
            if 8 <= len(digits) <= 10 and re.fullmatch(r'[\d.\-\s]+', label):
                phone = label.replace('.', '-')
                break
        waze = card.select_one('a[href*="waze.com/"]')
        result.append({
            'post_id': card.get('data-post-id'),
            'city': clean_text(city_node.get_text(' ', strip=True)),
            'waze_url': urljoin(SAPIR_BRANCHES_URL, waze['href']) if waze else None,
            'store': OfficialStore(
                address=clean_text(address_node.get_text(' ', strip=True)),
                phone=phone,
                hours='\n'.join(hours) or None,
                source_url=SAPIR_BRANCHES_URL,
            ),
        })
    return result


def sapir_listing_nav():
    """Known public JetEngine listing contract; live page metadata overrides it."""
    return {
        'query': {
            'post_status': ['publish'], 'post_type': 'branches', 'posts_per_page': '19',
            'paged': '1', 'ignore_sticky_posts': '1', 'suppress_filters': False,
            'jet_smart_filters': 'jet-engine/branches-listing',
        },
        'widget_settings': {
            'lisitng_id': 704, 'posts_num': 15, 'columns': 3, 'columns_tablet': 2,
            'columns_mobile': 1, 'column_min_width': 240, 'column_min_width_tablet': 240,
            'column_min_width_mobile': 240, 'inline_columns_css': False,
            'is_archive_template': '', 'post_status': ['publish'],
            'use_random_posts_num': 'yes', 'max_posts_num': 30,
            'not_found_message': 'No data was found', 'is_masonry': False,
            'equal_columns_height': 'yes', 'use_load_more': 'yes', 'load_more_id': '',
            'load_more_type': 'scroll',
            'load_more_offset': {'unit': 'px', 'size': 0, 'sizes': []},
            'use_custom_post_types': '', 'custom_post_types': [], 'hide_widget_if': '',
            'carousel_enabled': '', 'slides_to_scroll': '1', 'arrows': 'true',
            'arrow_icon': 'fa fa-angle-left', 'dots': 'true', 'autoplay': 'true',
            'pause_on_hover': 'true', 'autoplay_speed': 5000, 'infinite': 'true',
            'center_mode': '', 'effect': 'slide', 'speed': 500,
            'inject_alternative_items': '', 'injection_items': [],
            'scroll_slider_enabled': '', 'scroll_slider_on': ['desktop', 'tablet', 'mobile'],
            'custom_query': False, 'custom_query_id': '', '_element_id': 'branches-listing',
            'collapse_first_last_gap': False, 'list_tag_selection': '',
            'list_items_wrapper_tag': 'div', 'list_item_tag': 'div',
            'empty_items_wrapper_tag': 'div',
        },
    }


def fetch_sapir_catalog():
    """Load all JetEngine pages used by Super Sapir's public branch locator."""
    verify_tls = os.getenv('AGALI_ALLOW_INSECURE_TLS') != '1'
    session = requests.Session()
    response = session.get(SAPIR_BRANCHES_URL, headers=HEADERS, timeout=30, verify=verify_tls)
    response.raise_for_status()
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    container = soup.select_one('.jet-listing-grid__items[data-nav][data-pages]')
    nav = json.loads(container['data-nav']) if container else sapir_listing_nav()
    fragments = [response.text] if container else []
    pages = int(container.get('data-pages') or 3) if container else 3
    post_id = ((container.get('data-queried-id') if container else None)
               or '588|WP_Post').split('|', 1)[0]
    element = container.find_parent(attrs={'data-id': True}) if container else None
    element_id = element.get('data-id') if element else 'be031da'
    first_ajax_page = 2 if container else 1
    for page_number in range(first_ajax_page, pages + 1):
        request_data = {
            'action': 'jet_engine_ajax',
            'handler': 'listing_load_more',
            'query': nav['query'],
            'widget_settings': nav['widget_settings'],
            'page_settings': {
                'post_id': post_id,
                'queried_id': (container.get('data-queried-id') if container else None) or False,
                'element_id': element_id,
                'page': page_number,
            },
            'listing_type': False,
            'isEditMode': False,
            'addedPostCSS': ['704'],
        }
        page_response = session.post(
            f'{SAPIR_BRANCHES_URL}?nocache={int(time.time())}',
            headers={**HEADERS, 'X-Requested-With': 'XMLHttpRequest', 'Referer': SAPIR_BRANCHES_URL},
            data=list(jquery_form_pairs(request_data)), timeout=30, verify=verify_tls,
        )
        page_response.raise_for_status()
        payload = page_response.json()
        if not payload.get('success') or not payload.get('data', {}).get('html'):
            raise RuntimeError(f'Super Sapir locator page {page_number} returned no branches')
        fragments.append(payload['data']['html'])
    stores = []
    seen = set()
    for fragment in fragments:
        for item in extract_sapir_cards(fragment):
            if item['post_id'] not in seen:
                stores.append(item)
                seen.add(item['post_id'])
    if len(stores) < 20:
        raise RuntimeError('Super Sapir locator format changed: too few branches parsed')
    return stores


def extract_city_market_catalog(page):
    """Read the address labels and Waze links published by City Market."""
    soup = BeautifulSoup(page, 'html.parser')
    result = []
    for link in soup.select('a[href]'):
        url = link.get('href') or ''
        label = clean_text(link.get_text(' ', strip=True))
        if not label or ('waze.com/' not in url and 'to=place.' not in url):
            continue
        result.append({
            'address': label,
            'waze_url': url if url.startswith('https://') else None,
        })
    return result


def city_market_catalog_matches(store_name, official):
    """The mandatory feed embeds the full address in this chain's store name."""
    official_address = normalized(official['address'])
    return bool(
        len(official_address) >= 6
        and house_number(official['address'])
        and official_address in normalized(store_name)
    )


def extract_carrefour_phones(page):
    soup = BeautifulSoup(page, 'html.parser')
    result = {}
    for cells in ([cell.get_text(' ', strip=True) for cell in row.select('td')] for row in soup.select('tr')):
        if len(cells) >= 6 and cells[1].isdigit() and re.search(r'\d', cells[5]):
            result[cells[1]] = {'address': cells[3], 'phone': cells[5]}
    return result


def extract_dor_alon(page):
    soup = BeautifulSoup(page, 'html.parser')
    result = []
    for card in soup.select('.sl__item[data-lat][data-lng]'):
        title = card.select_one('.sl__item-title')
        address = card.select_one('.sl__items-address')
        if not title or not address:
            continue
        try:
            lat, lon = float(card['data-lat']), float(card['data-lng'])
        except (KeyError, TypeError, ValueError):
            continue
        phone = card.select_one('.sl__item-phone')
        hours = card.select_one('.sl__item-hours')
        result.append({
            'name': title.get_text(' ', strip=True),
            'store': OfficialStore(
                address.get_text(' ', strip=True),
                phone.get_text(' ', strip=True) if phone else None,
                hours.get_text(' ', strip=True) if hours else None,
                DOR_ALON_URL,
                lat,
                lon,
            ),
        })
    return result


def format_be_hours(opening_hours):
    """Keep the retailer's day-level schedule in a frontend-friendly text form."""
    day_labels = {
        'א': "יום א'", 'ב': "יום ב'", 'ג': "יום ג'", 'ד': "יום ד'",
        'ה': "יום ה'", 'ו': "יום ו'", 'ש': 'שבת',
    }

    def clock(value):
        value = str(value or '').strip()
        if not value or not re.search(r'\d', value):
            return value
        value = value.zfill(4)
        if value == '2400':
            return '00:00'
        return f'{value[:2]}:{value[2:4]}'

    lines = []
    for period in opening_hours or []:
        day = clean_text(period.get('day'))
        opened = clock(period.get('openHour'))
        closed = clock(period.get('closeHour'))
        if day and opened and closed:
            lines.append(f"{day_labels.get(day, day)}: {opened}-{closed}")
    return '\n'.join(lines) or None


def extract_be_branches(page):
    """Parse the JSON endpoint used by Be's official branch locator."""
    branches = json.loads(page)
    result = {}
    for branch in branches:
        code = clean_text(str(branch.get('code') or ''))
        try:
            latitude = float(branch.get('latitude'))
            longitude = float(branch.get('longitude'))
        except (TypeError, ValueError):
            latitude = longitude = None
        if not code:
            continue
        result[code] = OfficialStore(
            address=', '.join(filter(None, [clean_text(branch.get('address')), clean_text(branch.get('city'))])),
            phone=clean_text(branch.get('phone')) or None,
            hours=format_be_hours(branch.get('openingHours')),
            source_url=BE_BRANCHES_URL,
            latitude=latitude,
            longitude=longitude,
        )
    return result


def format_shufersal_hours(opening_hours):
    day_labels = {
        'א': "יום א'", 'ב': "יום ב'", 'ג': "יום ג'", 'ד': "יום ד'",
        'ה': "יום ה'", 'ו': "יום ו'", 'ש': 'שבת',
    }
    lines = []
    for period in opening_hours or []:
        day = clean_text(period.get('day'))
        opened = clean_text(period.get('open'))
        closed = clean_text(period.get('close'))
        if day and opened:
            lines.append(f"{day_labels.get(day, day)}: {opened}{f'-{closed}' if closed else ''}")
    return '\n'.join(lines) or None


def fetch_shufersal_catalog():
    """Call the public web method used by Shufersal's own branch locator.

    The Wix visitor instance and dispatcher metadata are discovered from the
    live page on every run; no private API key or expiring token is stored.
    """
    verify_tls = os.getenv('AGALI_ALLOW_INSECURE_TLS') != '1'
    session = requests.Session()
    page_response = session.get(
        SHUFERSAL_BRANCHES_URL, headers=HEADERS, timeout=30, verify=verify_tls,
    )
    page_response.raise_for_status()
    soup = BeautifulSoup(page_response.text, 'html.parser')
    model_tag = soup.select_one('#wix-viewer-model')
    if not model_tag:
        raise RuntimeError('Shufersal locator format changed: Wix model missing')
    model = json.loads(model_tag.get_text())
    config = model['siteFeaturesConfigs']['elementorySupportWixCodeSdk']
    token_response = session.get(
        model['accessTokensUrl'], headers=HEADERS, timeout=30, verify=verify_tls,
    )
    token_response.raise_for_status()
    instance = token_response.json()['apps'][WIX_CODE_APP_ID]['instance']
    method_url = urljoin(
        config['baseUrl'].rstrip('/') + '/',
        '_webMethods/backend/storelocator.jsw/SearchBranches.ajax',
    )
    params = {
        'gridAppId': config['gridAppId'],
        'viewMode': config['viewMode'],
        'instance': instance,
    }
    request_headers = {
        **HEADERS,
        'Content-Type': 'application/json',
        'X-XSRF-TOKEN': '',
        'x-wix-site-revision': str(config['siteRevision']),
        'x-wix-app-instance': instance,
        'Authorization': instance,
        'Referer': SHUFERSAL_BRANCHES_URL,
    }
    stores = {}
    # Be (network 5) has its own richer official endpoint above.
    for network_id in (1, 2, 3, 4, 6, 7, 8):
        response = session.post(
            method_url,
            params=params,
            headers=request_headers,
            data=json.dumps(['0', str(network_id)]),
            timeout=60,
            verify=verify_tls,
        )
        response.raise_for_status()
        items = response.json().get('result', {}).get('items', [])
        for item in items:
            store_id = clean_text(str(item.get('branchId') or ''))
            try:
                latitude = float(item.get('latitude'))
                longitude = float(item.get('longitude'))
            except (TypeError, ValueError):
                continue
            if not store_id or not (29.4 <= latitude <= 33.4 and 34.1 <= longitude <= 35.95):
                continue
            official = OfficialStore(
                address=', '.join(filter(None, [
                    clean_text(item.get('branchAddress')), clean_text(item.get('city')),
                ])),
                phone=clean_text(item.get('branchPhone')) or None,
                hours=format_shufersal_hours(item.get('OpeningHours')),
                source_url=SHUFERSAL_BRANCHES_URL,
                latitude=latitude,
                longitude=longitude,
            )
            if store_id in stores and (
                stores[store_id].latitude, stores[store_id].longitude
            ) != (official.latitude, official.longitude):
                # A reused identifier is not safe enough for automatic approval.
                stores[store_id] = None
            elif store_id not in stores:
                stores[store_id] = official
    return {store_id: store for store_id, store in stores.items() if store is not None}


def extract_yochananof_branches(javascript):
    """Read the official fallback catalog shipped with Yohananof's locator.

    The branch code is the retailer's stable code from the mandatory price
    files. Coordinates come from the map URL published for that same branch,
    so no address geocoding or fuzzy matching is involved.
    """
    quoted = r'(?:(?:"(?:\\.|[^"\\])*")|(?:\'(?:\\.|[^\'\\])*\'))'
    branch_pattern = re.compile(
        r'\{name:(?P<name>' + quoted + r'),address:(?P<address>' + quoted +
        r'),openingTimes:\{(?P<hours>.*?)\},srcForIframe:(?P<map>' + quoted +
        r'),code:(?P<code>' + quoted + r')\}',
        re.S,
    )
    hour_pattern = re.compile(r'(?P<key>' + quoted + r'|[^,:{}]+):(?P<value>' + quoted + r')')
    stores = {}
    ambiguous = set()
    for match in branch_pattern.finditer(javascript):
        try:
            code = clean_text(ast.literal_eval(match.group('code')))
            address = clean_text(ast.literal_eval(match.group('address')))
            map_url = ast.literal_eval(match.group('map'))
        except (SyntaxError, ValueError, TypeError):
            continue
        point = re.search(r'!2d(-?\d+(?:\.\d+)?)!3d(-?\d+(?:\.\d+)?)', map_url)
        if not code or not point:
            continue
        hours = []
        for period in hour_pattern.finditer(match.group('hours')):
            raw_key = period.group('key').strip()
            try:
                key = ast.literal_eval(raw_key) if raw_key[:1] in ('"', "'") else raw_key
                value = ast.literal_eval(period.group('value'))
            except (SyntaxError, ValueError):
                continue
            if clean_text(key) and clean_text(value):
                hours.append(f'{clean_text(key)}: {clean_text(value)}')
        store = OfficialStore(
            address=address,
            phone=None,
            hours='\n'.join(hours) or None,
            source_url=YOCHANANOF_BRANCHES_URL,
            latitude=float(point.group(2)),
            longitude=float(point.group(1)),
        )
        if code in stores and (
            stores[code].latitude, stores[code].longitude
        ) != (store.latitude, store.longitude):
            ambiguous.add(code)
        else:
            stores[code] = store
    return {code: store for code, store in stores.items() if code not in ambiguous}


def fetch_yochananof_catalog():
    """Discover and parse the current Next.js catalog without a fixed asset hash."""
    page = fetch(YOCHANANOF_BRANCHES_URL)
    chunk_paths = list(dict.fromkeys(re.findall(
        r'(?:https://yochananof\.co\.il)?(/_next/static/chunks/[^"\']+\.js)', page,
    )))
    # The current catalog is in this numbered shared chunk. Try it first, but
    # retain a full discovery fallback if a future deployment moves the module.
    chunk_paths.sort(key=lambda path: (not re.search(r'/5791-[^/]+\.js$', path), path))
    for path in chunk_paths:
        javascript = fetch(urljoin(YOCHANANOF_BRANCHES_URL, path))
        if 'srcForIframe' not in javascript or 'openingTimes' not in javascript:
            continue
        stores = extract_yochananof_branches(javascript)
        if stores:
            return stores
    raise RuntimeError('Yohananof locator format changed: official branch catalog missing')


def extract_rami_levy_api(page):
    """Parse the public JSON used by Rami Levy's own branch page.

    The endpoint publishes address, telephone and weekly hours, but no GPS.
    Consequently this source enriches details only; it never verifies a map
    point by itself.
    """
    payload = json.loads(page)
    result = []
    for branch in payload.get('stores', {}).get('data', []):
        street = clean_text(branch.get('street'))
        number = clean_text(str(branch.get('home_number') or ''))
        city = clean_text(branch.get('city'))
        address = ', '.join(filter(None, [' '.join(filter(None, [street, number])), city]))
        name = clean_text(branch.get('name'))
        if not name:
            continue
        hours = clean_text((branch.get('meta') or {}).get('open_hours_text')) or None
        # Do not publish malformed retailer typos (currently one branch says
        # 22:300). 24:00 is retained as a legitimate midnight notation.
        if hours and any(
            len(minutes) != 2 or int(minutes) > 59 or int(hour) > 24
            or (int(hour) == 24 and int(minutes) != 0)
            for hour, minutes in re.findall(r'(?<!\d)(\d{1,2}):(\d{2,3})(?!\d)', hours)
        ):
            hours = None
        result.append({
            'name': name,
            'store': OfficialStore(
                address=address,
                phone=clean_text(branch.get('tel')) or None,
                hours=hours,
                source_url=RAMI_LEVY_STORES_URL,
            ),
        })
    return result


def rami_levy_address_matches(db_address, db_city, official_address):
    """Require exact house number and every meaningful DB street token.

    Some mandatory price feeds put a numeric locality code in ``city``. A
    textual city is required to match; a numeric one is ignored, while the
    unique official-catalog candidate rule remains mandatory at the caller.
    """
    wanted_number = house_number(db_address)
    if not wanted_number or wanted_number != house_number(official_address):
        return False
    city_text = clean_text(db_city)
    if city_text and not re.fullmatch(r'[\d.]+', city_text):
        if normalized(city_text) not in normalized(official_address):
            return False
    city_words = {normalized(word) for word in re.split(r'[\s,]+', city_text)}
    ignored = {'רחוב', 'רח', 'שדרות', 'שד', 'קניון', 'מרכז', 'מסחרי'} | city_words
    wanted_words = {
        normalized(word) for word in re.split(r'[\s,]+', clean_text(db_address))
        if len(normalized(word)) >= 3 and not word.isdigit() and normalized(word) not in ignored
    }
    candidate = normalized(official_address)
    return bool(wanted_words) and all(word in candidate for word in wanted_words)


def normalized_rami_levy_branch(value):
    value = re.sub(r'\([^)]*\)', ' ', clean_text(value))
    for ignored in ('רמי לוי', 'בשכונה', 'שיווק השקמה', 'סניף'):
        value = value.replace(ignored, ' ')
    return normalized(value)


def rami_levy_catalog_matches(db_name, db_address, db_city, official):
    """Accept an exact unique branch label, or exact address plus name proof."""
    wanted_name = normalized_rami_levy_branch(db_name)
    official_name = normalized_rami_levy_branch(official['name'])
    if len(wanted_name) >= 3 and wanted_name == official_name:
        wanted_number = house_number(db_address)
        official_number = house_number(official['store'].address)
        # A matching label must never override contradictory street numbers.
        # Missing official/DB addresses are common for neighborhood branches,
        # where the unique exact retailer label is still deterministic.
        return not (wanted_number and official_number and wanted_number != official_number)
    if not rami_levy_address_matches(db_address, db_city, official['store'].address):
        return False
    wanted_words = {normalized(word) for word in re.split(r'[\s,]+', clean_text(db_name))
                    if len(normalized(word)) >= 3}
    official_words = {normalized(word) for word in re.split(r'[\s,]+', clean_text(official['name']))
                      if len(normalized(word)) >= 3}
    ignored = {'רמי', 'לוי', 'שיווק', 'השקמה', 'בשכונה', 'סניף'}
    return bool((wanted_words - ignored) & (official_words - ignored))


def rami_levy_catalog_candidates(chain_id, store_id, db_name, db_address, db_city, catalog):
    """Return only deterministic candidates for one price-feed branch.

    Neighborhood API records usually omit their address and telephone. Their
    price-feed branch code is therefore joined through an audited exact-label
    table. Main-chain rows are never allowed to match a neighborhood record,
    which prevents same-city false positives such as Afula.
    """
    if clean_text(chain_id) == RAMI_LEVY_NEIGHBORHOOD_CHAIN_ID:
        expected = RAMI_LEVY_NEIGHBORHOOD_STORE_LABELS.get(clean_text(store_id))
        if not expected:
            return []
        expected_name = normalized_rami_levy_branch(expected)
        return [item for item in catalog
                if 'בשכונה' in clean_text(item['name'])
                and normalized_rami_levy_branch(item['name']) == expected_name]
    expected = RAMI_LEVY_MAIN_STORE_LABELS.get(clean_text(store_id))
    if expected:
        expected_is_neighborhood = 'בשכונה' in clean_text(expected)
        expected_name = normalized_rami_levy_branch(expected)
        return [item for item in catalog
                if ('בשכונה' in clean_text(item['name'])) == expected_is_neighborhood
                and normalized_rami_levy_branch(item['name']) == expected_name]
    return [item for item in catalog
            if 'בשכונה' not in clean_text(item['name'])
            and rami_levy_catalog_matches(db_name, db_address, db_city, item)]


def unique_detail_store(candidates):
    """Collapse duplicate API rows only when every publishable field agrees."""
    unique = {}
    for item in candidates:
        store = item['store']
        key = (store.phone, store.hours, store.source_url, store.latitude, store.longitude)
        unique.setdefault(key, store)
    return next(iter(unique.values())) if len(unique) == 1 else None


def osher_snapshot_match(snapshot, store_id, address, city):
    """Match the audited Osher snapshot without changing feed address data."""
    expected_city = snapshot.get('store_ids', {}).get(clean_text(store_id))
    if expected_city:
        matches = [s for s in snapshot['branches']
                   if normalized(s[0]) == normalized(expected_city)]
        if len(matches) > 1:
            matches = [s for s in matches
                       if house_number(address) == house_number(s[1])
                       and normalized(s[1]) in normalized(address)]
    else:
        matches = [s for s in snapshot['branches']
                   if normalized(city) == normalized(s[0])
                   and house_number(address) == house_number(s[1])
                   and normalized(s[1]) in normalized(address)]
    return matches[0] if len(matches) == 1 else None


def normalized_dor_branch(value):
    value = clean_text(value)
    value = re.sub(r'(?i)am\s*[-:]?\s*pm', ' ', value or '')
    for ignored in ('מושב', 'קיבוץ', 'תפעול', 'משלוחי'):
        value = value.replace(ignored, ' ')
    value = (value.replace('קריית', 'קרית').replace('חייל', 'חיל')
             .replace('יהושוע', 'יהושע').replace('נווה', 'נוה').replace('מחסייה', 'מחסיה'))
    return normalized(value)


def strict_dor_address_match(db_address, db_city, official_address):
    wanted_number = house_number(db_address)
    if not wanted_number or wanted_number != house_number(official_address):
        return False
    ignored = {'רחוב', 'רח', 'כביש', 'מספר', 'קניון', 'מרכז', normalized(db_city)}
    wanted_words = [normalized(word) for word in re.split(r'[\s,]+', db_address or '')]
    wanted_words = [word for word in wanted_words
                    if len(word) >= 3 and not word.isdigit() and word not in ignored]
    candidate = normalized(official_address)
    return bool(wanted_words) and all(word in candidate for word in wanted_words)


def dor_branch_score(db_name, db_address, db_city, official):
    wanted_name = normalized_dor_branch(db_name)
    actual_name = normalized_dor_branch(official['name'])
    name_score = 0
    if wanted_name and wanted_name == actual_name:
        # Prefer the exact card title over a co-located "... supermarket"
        # card that reduces to the same significant word set.
        name_score = 6
    wanted_words = set(re.findall(r'[0-9a-zא-ת]+', clean_text(db_name).lower()))
    actual_words = set(re.findall(r'[0-9a-zא-ת]+', clean_text(official['name']).lower()))
    ignored = {'am', 'pm', 'מושב', 'קיבוץ', 'תפעול', 'תחנה', 'דוכן', 'הדוכן', 'סופר', 'מיני', 'אלונית'}
    wanted_words -= ignored
    actual_words -= ignored
    if wanted_words and wanted_words == actual_words:
        name_score = max(name_score, 4)
    # Some source names omit a city prefix (for example "רמת רחל" versus
    # "ירושלים רמת רחל"). A long unique containment is still an exact branch
    # label match inside Dor Alon's own catalog.
    if len(wanted_name) >= 5 and (wanted_name in actual_name or actual_name in wanted_name):
        name_score = max(name_score, 3)
    wanted_address = normalized(db_address).replace('התעש', 'תעש')
    actual_address = normalized(official['store'].address).replace('התעש', 'תעש')
    # The transparency feed occasionally truncates long addresses. A unique
    # exact containment inside Dor Alon's own address is still deterministic.
    exact_address = (len(wanted_address) >= 8 and len(actual_address) >= 8
                     and (wanted_address in actual_address or actual_address in wanted_address))
    address_score = 7 if exact_address else (2 if strict_dor_address_match(
        db_address or '', db_city or '', official['store'].address
    ) else 0)
    return name_score + address_score


def address_matches(address, city, candidate):
    # Require city, house number and every significant street token; ambiguity is skipped.
    if not city or normalized(city) not in normalized(candidate):
        return False
    number = house_number(address)
    if not number or number != house_number(candidate):
        return False
    tokens = [normalized(w) for w in re.split(r'[\s,]+', address or '')]
    ignored = {'רחוב', 'רח', 'דרך', 'קניון', 'מרכז', normalized(city)}
    tokens = [w for w in tokens if len(w) >= 3 and not w.isdigit() and w not in ignored]
    return bool(tokens) and all(w in normalized(candidate) for w in tokens)


def normalized_sapir_place(value):
    value = clean_text(value).replace('קריית', 'קרית').replace('תל אביב-יפו', 'תל אביב')
    return normalized(value)


def sapir_catalog_matches(db_address, db_city, official):
    """Require exact city, house number and all meaningful street words."""
    if not db_city or normalized_sapir_place(db_city) != normalized_sapir_place(official['city']):
        return False
    candidate = official['store'].address
    number = house_number(db_address)
    if not number or number != house_number(candidate):
        return False
    ignored = {'רחוב', 'רח', 'דרך', 'שדרות', 'שד', 'קניון', 'מרכז', 'מסחרי'}
    tokens = [normalized(word) for word in re.split(r'[\s,]+', clean_text(db_address))]
    tokens = [word for word in tokens
              if len(word) >= 3 and not word.isdigit() and word not in ignored]
    official_address = normalized(candidate)
    return bool(tokens) and all(word in official_address for word in tokens)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        goodpharm_catalog = extract_goodpharm_catalog(fetch(GOODPHARM_URL))
        if not goodpharm_catalog:
            raise RuntimeError('Good Pharm catalog format changed: no branches parsed')
    except Exception as error:
        goodpharm_catalog = []
        print({'warning': 'goodpharm-official-unavailable', 'error': type(error).__name__})
    with psycopg2.connect(database_url()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT id,address,city FROM stores WHERE chain_id='7290058197699'
                AND (geocode_status='review_required' OR store_details_checked_at IS NULL
                     OR store_details_checked_at < NOW()-INTERVAL '7 days')""")
            rows = cursor.fetchall()
            matched = 0
            coordinates = 0
            for db_id, address, city in rows:
                matches = [item for item in goodpharm_catalog
                           if address_matches(address, city, item['store'].address)]
                if len(matches) == 1:
                    item = matches[0]
                    if item['waze_url']:
                        try:
                            point = fetch_waze_destination(item['waze_url'])
                        except Exception:
                            point = None
                        if point:
                            item['store'].latitude, item['store'].longitude = point
                    coordinates += int(persist(
                        cursor, db_id, item['store'], 'goodpharm-official-waze', args.apply,
                        'official_store_waze_destination',
                    ))
                    matched += 1
            print({'source':'goodpharm', 'parsed':len(goodpharm_catalog), 'due':len(rows),
                   'matched':matched, 'coordinates_verified':coordinates,'apply':args.apply})

            try:
                carrefour = extract_carrefour_phones(fetch(CARREFOUR_ACCESSIBILITY_URL))
                if not carrefour:
                    raise RuntimeError('Carrefour accessibility catalog format changed: no branches parsed')
            except Exception as error:
                carrefour = {}
                print({'warning': 'carrefour-official-unavailable', 'error': type(error).__name__})
            cursor.execute("SELECT id,store_id,address FROM stores WHERE chain_id='7290055700007' AND phone IS NULL")
            carrefour_count = 0
            for db_id, store_id, address in cursor.fetchall():
                published = carrefour.get(store_id)
                if not published:
                    continue
                db_number, published_number = house_number(address), house_number(published['address'])
                if db_number and published_number and db_number != published_number:
                    continue
                if args.apply:
                    cursor.execute("""UPDATE stores SET phone=%s, website_url=COALESCE(website_url,%s),
                        store_details_source=COALESCE(store_details_source,'carrefour-official-accessibility'),
                        store_details_source_url=COALESCE(store_details_source_url,%s),
                        store_details_updated_at=NOW(), store_details_checked_at=NOW() WHERE id=%s AND phone IS NULL""",
                        (published['phone'],CARREFOUR_ACCESSIBILITY_URL,CARREFOUR_ACCESSIBILITY_URL,db_id))
                carrefour_count += 1
            print({'source':'carrefour-official-accessibility','parsed':len(carrefour),'matched':carrefour_count,'apply':args.apply})

            # Be publishes the same stable branch code used in the mandatory
            # price files, so this is an identity match rather than fuzzy
            # geocoding. The endpoint also supplies phone, hours and GPS.
            try:
                be_branches = extract_be_branches(fetch(BE_BRANCHES_URL))
                if not be_branches:
                    raise RuntimeError('Be official catalog format changed: no branches parsed')
            except Exception as error:
                be_branches = {}
                print({'warning': 'be-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,store_id FROM stores WHERE chain_name='BE'
                AND (geocode_status='review_required' OR phone IS NULL OR opening_hours_text IS NULL
                     OR store_details_checked_at IS NULL OR store_details_checked_at < NOW()-INTERVAL '7 days')""")
            be_rows = cursor.fetchall()
            be_matched = 0
            be_coordinates = 0
            for db_id, store_id in be_rows:
                official = be_branches.get(clean_text(store_id))
                if not official:
                    continue
                be_coordinates += int(persist(
                    cursor, db_id, official, 'be-official-api', args.apply,
                    'official_store_id_match',
                ))
                be_matched += 1
            print({'source':'be-official-api','parsed':len(be_branches),'due':len(be_rows),
                   'matched':be_matched,'coordinates_verified':be_coordinates,'apply':args.apply})

            # Shufersal's locator exposes the stable branchId from the mandatory
            # price feeds together with GPS, phone and current weekly hours.
            # Identity matching avoids address-based guesses entirely.
            try:
                shufersal = fetch_shufersal_catalog()
            except Exception as error:
                shufersal = {}
                print({'warning': 'shufersal-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,store_id FROM stores
                WHERE chain_id=%s AND chain_name<>'BE'
                  AND (geocode_status='review_required' OR phone IS NULL OR opening_hours_text IS NULL
                       OR store_details_checked_at IS NULL
                       OR store_details_checked_at < NOW()-INTERVAL '7 days')""",
                (SHUFERSAL_CHAIN_ID,))
            shufersal_rows = cursor.fetchall()
            shufersal_matched = 0
            shufersal_coordinates = 0
            for db_id, store_id in shufersal_rows:
                official = shufersal.get(clean_text(store_id))
                if not official:
                    continue
                shufersal_coordinates += int(persist(
                    cursor, db_id, official, 'shufersal-official-locator', args.apply,
                    'official_store_id_match',
                ))
                shufersal_matched += 1
            print({'source':'shufersal-official-locator','parsed':len(shufersal),
                   'due':len(shufersal_rows),'matched':shufersal_matched,
                   'coordinates_verified':shufersal_coordinates,'apply':args.apply})

            # Yohananof's official locator ships branch code, weekly hours and
            # the exact map point together. Match only that stable branch code.
            try:
                yochananof = fetch_yochananof_catalog()
            except Exception as error:
                yochananof = {}
                print({'warning': 'yochananof-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,store_id FROM stores WHERE chain_id=%s
                AND (geocode_status='review_required' OR opening_hours_text IS NULL
                     OR store_details_checked_at IS NULL
                     OR store_details_checked_at < NOW()-INTERVAL '7 days')""",
                (YOCHANANOF_CHAIN_ID,))
            yochananof_rows = cursor.fetchall()
            yochananof_matched = 0
            yochananof_coordinates = 0
            for db_id, store_id in yochananof_rows:
                official = yochananof.get(clean_text(store_id))
                if not official:
                    continue
                yochananof_coordinates += int(persist(
                    cursor, db_id, official, 'yochananof-official-site', args.apply,
                    'official_store_id_match',
                ))
                yochananof_matched += 1
            print({'source':'yochananof-official-site','parsed':len(yochananof),
                   'due':len(yochananof_rows),'matched':yochananof_matched,
                   'coordinates_verified':yochananof_coordinates,'apply':args.apply})

            # This official API has phone numbers and hours, but no GPS. A
            # unique exact-address match enriches details only. Overture may
            # subsequently prove a point from the newly known phone number.
            try:
                rami_levy = extract_rami_levy_api(fetch(RAMI_LEVY_STORES_API))
            except Exception as error:
                rami_levy = []
                print({'warning': 'rami-levy-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,chain_id,store_id,store_name,address,city FROM stores WHERE chain_id=ANY(%s)
                AND (phone IS NULL OR opening_hours_text IS NULL
                     OR store_details_checked_at IS NULL
                     OR store_details_checked_at < NOW()-INTERVAL '7 days')""",
                (list(RAMI_LEVY_CHAIN_IDS),))
            rami_rows = cursor.fetchall()
            rami_matched = 0
            rami_ambiguous = 0
            for db_id, chain_id, store_id, store_name, address, city in rami_rows:
                candidates = rami_levy_catalog_candidates(
                    chain_id, store_id, store_name, address, city, rami_levy,
                )
                official = unique_detail_store(candidates)
                if official is None:
                    rami_ambiguous += int(bool(candidates))
                    continue
                persist(cursor, db_id, official, 'rami-levy-official-api', args.apply)
                rami_matched += 1
            print({'source':'rami-levy-official-api','parsed':len(rami_levy),
                   'due':len(rami_rows),'matched':rami_matched,
                   'ambiguous':rami_ambiguous,'apply':args.apply})

            # Super Sapir publishes current hours, phone and a Waze destination
            # together in each official branch card. A point is accepted only
            # after an exact city + street + house-number match; short or
            # unresolved Waze links never downgrade the row from review_required.
            try:
                sapir = fetch_sapir_catalog()
            except Exception as error:
                sapir = []
                print({'warning': 'super-sapir-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,address,city FROM stores WHERE chain_id=%s
                AND (geocode_status='review_required' OR phone IS NULL OR opening_hours_text IS NULL
                     OR store_details_checked_at IS NULL
                     OR store_details_checked_at < NOW()-INTERVAL '7 days')""",
                (SAPIR_CHAIN_ID,))
            sapir_rows = cursor.fetchall()
            sapir_matched = 0
            sapir_ambiguous = 0
            sapir_coordinates = 0
            waze_cache = {}
            for db_id, address, city in sapir_rows:
                matches = [item for item in sapir if sapir_catalog_matches(address, city, item)]
                if len(matches) != 1:
                    sapir_ambiguous += int(bool(matches))
                    continue
                item = matches[0]
                waze_url = item['waze_url']
                if waze_url:
                    if waze_url not in waze_cache:
                        try:
                            waze_cache[waze_url] = fetch_waze_destination(waze_url)
                        except Exception:
                            waze_cache[waze_url] = None
                    point = waze_cache[waze_url]
                    if point:
                        item['store'].latitude, item['store'].longitude = point
                source = 'super-sapir-official-waze' if item['store'].latitude else 'super-sapir-official-site'
                sapir_coordinates += int(persist(
                    cursor, db_id, item['store'], source, args.apply,
                    'official_store_waze_destination',
                ))
                sapir_matched += 1
            print({'source':'super-sapir-official','parsed':len(sapir),'due':len(sapir_rows),
                   'matched':sapir_matched,'ambiguous':sapir_ambiguous,
                   'coordinates_verified':sapir_coordinates,'apply':args.apply})

            # City Market's price-feed rows currently have address='unknown',
            # but their store_name contains the complete published address.
            # Require that entire official address (including its number and
            # locality) to occur verbatim after normalization, and a unique
            # catalog match, before following the retailer's Waze link.
            try:
                city_market = extract_city_market_catalog(fetch(CITY_MARKET_URL))
            except Exception as error:
                city_market = []
                print({'warning': 'city-market-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,store_name FROM stores WHERE chain_id=%s
                AND geocode_status='review_required'""", (CITY_MARKET_CHAIN_ID,))
            city_market_rows = cursor.fetchall()
            city_market_matches = []
            city_market_ambiguous = 0
            for db_id, store_name in city_market_rows:
                matches = [item for item in city_market
                           if city_market_catalog_matches(store_name, item)]
                if len(matches) == 1:
                    city_market_matches.append((db_id, matches[0]))
                else:
                    city_market_ambiguous += int(bool(matches))
            urls = list(dict.fromkeys(
                item['waze_url'] for _, item in city_market_matches if item['waze_url']
            ))

            def safe_waze_point(url):
                try:
                    return url, fetch_waze_destination(url)
                except Exception:
                    return url, None

            with ThreadPoolExecutor(max_workers=2) as pool:
                city_market_points = dict(pool.map(safe_waze_point, urls))
            city_market_coordinates = 0
            for db_id, item in city_market_matches:
                point = city_market_points.get(item['waze_url'])
                if not point:
                    continue
                official = OfficialStore(
                    address=item['address'], phone=None, hours=None,
                    source_url=CITY_MARKET_URL, latitude=point[0], longitude=point[1],
                )
                city_market_coordinates += int(persist(
                    cursor, db_id, official, 'city-market-official-waze', args.apply,
                    'official_store_waze_destination',
                ))
            print({'source':'city-market-official','parsed':len(city_market),
                   'due':len(city_market_rows),'matched':len(city_market_matches),
                   'ambiguous':city_market_ambiguous,
                   'coordinates_verified':city_market_coordinates,'apply':args.apply})

            # The official locator publishes its coordinates directly in each
            # branch card and refreshes holiday notices/hours itself. Radware
            # can occasionally return a CAPTCHA to headless jobs; in that case
            # retain all existing data and let the next weekly run retry.
            try:
                dor_page = fetch(f"{DOR_ALON_URL}?agali_refresh={date.today().isoformat()}")
                dor_alon = extract_dor_alon(dor_page)
            except Exception as error:
                dor_alon = []
                print({'warning': 'dor-alon-official-unavailable', 'error': type(error).__name__})
            cursor.execute("""SELECT id,store_name,address,city FROM stores WHERE chain_name='Dor Alon'
                AND (geocode_status='review_required' OR phone IS NULL OR opening_hours_text IS NULL
                     OR store_details_checked_at IS NULL OR store_details_checked_at < NOW()-INTERVAL '7 days')""")
            dor_rows = cursor.fetchall()
            dor_count = 0
            dor_ambiguous = 0
            for db_id, store_name, address, city in dor_rows:
                if any(marker in normalized(store_name) for marker in ('וולט', 'משלוחים', 'משלוחי', 'online')):
                    continue
                scored = [(item, dor_branch_score(store_name, address, city, item)) for item in dor_alon]
                best_score = max((score for _, score in scored), default=0)
                matches = [item for item, score in scored if score == best_score and score > 0]
                coordinates = {(item['store'].latitude, item['store'].longitude) for item in matches}
                if len(coordinates) != 1:
                    dor_ambiguous += int(bool(matches))
                    continue
                official = matches[0]['store']
                persist(cursor, db_id, official, 'dor-alon-official', args.apply,
                        'official_catalog_unique_branch_name')
                dor_count += 1
            print({'source':'dor-alon-official','parsed':len(dor_alon),'due':len(dor_rows),
                   'matched':dor_count,'ambiguous':dor_ambiguous,'apply':args.apply})

            # One-time published snapshot. Never reset its age or replace newer hours.
            snapshot = json.loads(Path(__file__).with_name('store_details_osher_snapshot.json').read_text(encoding='utf-8'))
            cursor.execute("SELECT id,store_id,address,city FROM stores WHERE chain_id='7290103152017' AND (opening_hours_text IS NULL OR phone IS NULL OR website_url IS NULL OR website_url<>%s OR geocode_status<>'verified')", (snapshot['source_url'],))
            snapshot_count = 0
            snapshot_coordinates = 0
            for db_id, store_id, address, city in cursor.fetchall():
                match = osher_snapshot_match(snapshot, store_id, address, city)
                if match is None:
                    continue
                _, _, start, early, late, *published_phone = match
                hours = f"ימים א'-ג': {start}-{early}\nימים ד'-ה': {start}-{late}\nיום ו': שעון חורף 07:30-13:00; שעון קיץ 07:30-15:00"
                if args.apply:
                    cursor.execute("""UPDATE stores SET opening_hours_text=COALESCE(opening_hours_text,%s), phone=COALESCE(phone,%s), website_url=%s,
                        opening_hours_updated_at=CASE WHEN opening_hours_text IS NULL THEN %s ELSE opening_hours_updated_at END,
                        store_details_source='osherad-official-snapshot', store_details_source_url=%s,
                        store_details_updated_at=COALESCE(store_details_updated_at,%s) WHERE id=%s""",
                        (hours,published_phone[0] if published_phone else '076-8899999',snapshot['source_url'],snapshot['collected_at'],snapshot['source_url'],snapshot['collected_at'],db_id))
                    point = snapshot.get('coordinates', {}).get(clean_text(store_id))
                    if point:
                        cursor.execute("""UPDATE stores SET latitude=%s,longitude=%s,geocode_status='verified',
                            geocode_source='waze-store-page',geocode_precision='official_poi',
                            geocode_reason='exact_chain_name_and_official_address_waze_poi',
                            geocode_matched_address=%s,geocode_verified_at=NOW()
                            WHERE id=%s AND geocode_status<>'verified'""",
                            (point['latitude'],point['longitude'],point['address'],db_id))
                        snapshot_coordinates += cursor.rowcount
                snapshot_count += 1
            print({'source':'osherad-published-snapshot','matched':snapshot_count,
                   'coordinates_verified':snapshot_coordinates,'apply':args.apply})
        if not args.apply:
            connection.rollback()


if __name__ == '__main__':
    main()
