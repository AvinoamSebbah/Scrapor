"""Small deterministic tests for official store-catalog parsing and matching."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from enrich_store_catalogs import (
    city_market_catalog_matches,
    dor_branch_score,
    extract_be_branches,
    extract_city_market_catalog,
    extract_goodpharm_catalog,
    extract_rami_levy_api,
    extract_sapir_cards,
    extract_waze_destination,
    extract_waze_url_point,
    extract_yochananof_branches,
    format_be_hours,
    format_shufersal_hours,
    rami_levy_address_matches,
    rami_levy_catalog_candidates,
    rami_levy_catalog_matches,
    osher_snapshot_match,
    sapir_catalog_matches,
    unique_detail_store,
)
from enrich_store_details import OfficialStore
from enrich_store_locations_nominatim import exact_candidates, unique_point


class StoreCatalogTests(unittest.TestCase):
    def test_be_uses_published_id_and_formats_hours(self):
        payload = json.dumps([{
            "code": "42",
            "address": "Herzl 10",
            "city": "Haifa",
            "phone": "04-0000000",
            "latitude": "32.8",
            "longitude": "34.99",
            "openingHours": [{"day": "א", "openHour": "0800", "closeHour": "2200"}],
        }])
        stores = extract_be_branches(payload)
        self.assertEqual(set(stores), {"42"})
        self.assertEqual(stores["42"].hours, "יום א': 08:00-22:00")
        self.assertEqual((stores["42"].latitude, stores["42"].longitude), (32.8, 34.99))

    def test_goodpharm_retains_official_waze_destination(self):
        page = '''<div class="elementor-widget-wrap">
          <h1><a href="https://goodpharm.co.il/pharm_storelocation/test">יהוד</a></h1>
          <div class="elementor-widget-text-editor"><div class="elementor-widget-container">רחוב מקלב 8, יהוד</div></div>
          <a href="https://www.waze.com/live-map/directions/example?to=place.example">Waze</a>
        </div>'''
        items = extract_goodpharm_catalog(page)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['name'], 'יהוד')
        self.assertIn('waze.com/live-map', items[0]['waze_url'])

    def test_waze_destination_parser_reads_routing_point_only(self):
        page = '{"routing":{"to":{"address":"test","latLng":{"lat":32.0326,"lng":34.8875}}}}'
        self.assertEqual(extract_waze_destination(page), (32.0326, 34.8875))

    def test_waze_redirect_url_exposes_literal_destination(self):
        url = 'https://www.waze.com/live-map/directions?to=ll.32.02225%2C34.774668'
        self.assertEqual(extract_waze_url_point(url), (32.02225, 34.774668))

    def test_sapir_card_keeps_official_hours_phone_and_waze(self):
        page = '''<div class="jet-listing-grid__item" data-post-id="4392">
          <a class="jet-listing-dynamic-terms__link">קריית ים</a>
          <i class="jet-listing-dynamic-field__icon fas fa-home"></i>
          <div class="jet-listing-dynamic-field__content">גאולה כהן 4</div>
          <section><p class="elementor-heading-title">יום ראשון</p>
            <div class="jet-listing-dynamic-field__content">06:30 -</div>
            <div class="jet-listing-dynamic-field__content">21:00</div></section>
          <a href="04.618.4766/">04.618.4766</a>
          <a href="https://waze.com/ul/hsvbfyhpqv">Waze</a>
        </div>'''
        items = extract_sapir_cards(page)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['store'].phone, '04-618-4766')
        self.assertEqual(items[0]['store'].hours, 'יום ראשון: 06:30-21:00')
        self.assertEqual(items[0]['waze_url'], 'https://waze.com/ul/hsvbfyhpqv')

    def test_sapir_match_requires_city_street_and_house_number(self):
        item = {
            'city': 'קריית ים',
            'store': OfficialStore('גאולה כהן 4', None, None, 'official'),
        }
        self.assertTrue(sapir_catalog_matches('רחוב גאולה כהן 4', 'קרית ים', item))
        self.assertFalse(sapir_catalog_matches('רחוב גאולה כהן 5', 'קרית ים', item))
        self.assertFalse(sapir_catalog_matches('גאולה כהן 4', 'חיפה', item))

    def test_city_market_requires_complete_official_address_in_feed_name(self):
        page = '''<a href="https://waze.com/ul/hsv8wrk4fj">
          <span class="elementor-icon-list-text">לוינסקי 76 תל אביב</span></a>'''
        items = extract_city_market_catalog(page)
        self.assertEqual(len(items), 1)
        self.assertTrue(city_market_catalog_matches(
            'ענק הממתקים בע"מ, לוינסקי 76 תל אביב', items[0],
        ))
        self.assertFalse(city_market_catalog_matches(
            'ענק הממתקים בע"מ, לוינסקי 99 תל אביב', items[0],
        ))

    def test_shufersal_colon_hours_are_not_reformatted_as_digits(self):
        hours = format_shufersal_hours([
            {"day": "א", "open": "07:30", "close": "21:00"},
            {"day": "ש", "open": "סגור", "close": ""},
        ])
        self.assertEqual(hours, "יום א': 07:30-21:00\nשבת: סגור")

    def test_yochananof_uses_official_code_hours_and_map_point(self):
        javascript = ('[{name:"סניף בדיקה",address:"הרצל 10, חיפה",'
                      'openingTimes:{ראשון:"08:00-22:00","שישי שעון חורף":"07:00-13:30"},'
                      'srcForIframe:"https://www.google.com/maps/embed?pb=!2d34.9901!3d32.8012",'
                      'code:"42"}]')
        stores = extract_yochananof_branches(javascript)
        self.assertEqual(set(stores), {"42"})
        self.assertEqual(stores["42"].hours, "ראשון: 08:00-22:00\nשישי שעון חורף: 07:00-13:30")
        self.assertEqual((stores["42"].latitude, stores["42"].longitude), (32.8012, 34.9901))

    def test_rami_api_formats_official_details_without_inventing_gps(self):
        payload = json.dumps({"stores": {"data": [{
            "name": "חיפה", "street": "בר יהודה ישראל", "home_number": 111,
            "city": "חיפה", "tel": "04-0000000",
            "meta": {"open_hours_text": "<p>ימים א'-ה': 08:00-22:00</p>"},
        }]}})
        branches = extract_rami_levy_api(payload)
        self.assertEqual(len(branches), 1)
        self.assertEqual(branches[0]['store'].address, 'בר יהודה ישראל 111, חיפה')
        self.assertEqual(branches[0]['store'].hours, "ימים א'-ה': 08:00-22:00")
        self.assertIsNone(branches[0]['store'].latitude)

    def test_rami_api_rejects_malformed_retailer_time(self):
        payload = json.dumps({"stores": {"data": [{
            "name": "בן יהודה 23 (בשכונה)",
            "meta": {"open_hours_text": "ימים ד'-ה': 07:00-22:300"},
        }]}})
        self.assertIsNone(extract_rami_levy_api(payload)[0]['store'].hours)

    def test_rami_match_requires_house_number_and_all_street_tokens(self):
        official = 'בר יהודה ישראל 111, חיפה'
        self.assertTrue(rami_levy_address_matches('בר יהודה ישראל 111', 'חיפה', official))
        self.assertTrue(rami_levy_address_matches('בר יהודה ישראל 111', '2630.0', official))
        self.assertTrue(rami_levy_address_matches('שדרות בר יהודה ישראל 111', '2630.0', official))
        self.assertFalse(rami_levy_address_matches('בר יהודה ישראל 110', 'חיפה', official))
        self.assertFalse(rami_levy_address_matches('בר כוכבא 111', 'חיפה', official))

    def test_rami_match_rejects_official_card_with_unrelated_branch_name(self):
        official = {
            'name': 'רובוטי ביג באר שבע',
            'store': OfficialStore('חלוצי התעשיה 73, חיפה', None, None, 'official'),
        }
        self.assertFalse(rami_levy_catalog_matches(
            'אדמירליטי', 'חלוצי התעשיה 73', 'חיפה', official,
        ))

    def test_rami_exact_unique_branch_label_can_supply_missing_details(self):
        official = {
            'name': 'רמי לוי בשכונה - חולון',
            'store': OfficialStore('', '03-0000000', 'יום א: 08:00-20:00', 'official'),
        }
        self.assertTrue(rami_levy_catalog_matches(
            'חולון (בשכונה)', 'unknown', 'חולון', official,
        ))

    def test_rami_exact_label_does_not_override_conflicting_house_number(self):
        official = {
            'name': 'אשדוד',
            'store': OfficialStore('היהלומים 9, אשדוד', None, None, 'official'),
        }
        self.assertFalse(rami_levy_catalog_matches(
            'אשדוד', 'היהלומים 8', 'אשדוד', official,
        ))

    def test_rami_neighborhood_uses_audited_price_feed_code(self):
        correct = {
            'name': 'כפר סבא (בשכונה)',
            'store': OfficialStore('', None, 'יום א: 07:00-21:30', 'official'),
        }
        wrong_same_city = {
            'name': 'כפר סבא',
            'store': OfficialStore('גלגלי הפלדה 4, כפר סבא', '09-0', None, 'official'),
        }
        matches = rami_levy_catalog_candidates(
            '7291056200008', '309', 'ר.בשכונה רוטשילד כ"ס',
            'רוטשילד 39', 'כפר סבא', [wrong_same_city, correct],
        )
        self.assertEqual(matches, [correct])

    def test_rami_neighborhood_unknown_code_is_not_guessed(self):
        official = {
            'name': 'הרצליה (בשכונה)',
            'store': OfficialStore('', None, 'יום א: 07:00-21:30', 'official'),
        }
        self.assertEqual(rami_levy_catalog_candidates(
            '7291056200008', '999', 'הרצליה', 'סוקולוב 1', 'הרצליה', [official],
        ), [])

    def test_rami_main_chain_excludes_neighborhood_same_city(self):
        neighborhood = {
            'name': 'עפולה (בשכונה)',
            'store': OfficialStore('', None, 'יום א: 07:00-21:00', 'official'),
        }
        self.assertEqual(rami_levy_catalog_candidates(
            '7290058140886', '44', 'עפולה', 'קהילת ציון 10', 'עפולה', [neighborhood],
        ), [])

    def test_rami_duplicate_api_rows_are_safe_only_when_details_agree(self):
        first = {'name': 'מגדל העמק', 'store': OfficialStore('', '04-1', 'א: 08:00-20:00', 'official')}
        second = {'name': 'מגדל העמק', 'store': OfficialStore('כתובת אחרת', '04-1', 'א: 08:00-20:00', 'official')}
        self.assertIs(unique_detail_store([first, second]), first['store'])
        second['store'].phone = '04-2'
        self.assertIsNone(unique_detail_store([first, second]))

    def test_osher_snapshot_uses_id_and_disambiguates_same_city_by_address(self):
        snapshot = {
            'store_ids': {'3': 'ירושלים'},
            'branches': [
                ['ירושלים', 'בית הדפוס 29', '10:00', '23:00', '00:00'],
                ['ירושלים', 'שמגר 16', '10:00', '23:00', '00:00'],
            ],
        }
        self.assertEqual(
            osher_snapshot_match(snapshot, '3', 'בית הדפוס 29', 'ירושלים')[1],
            'בית הדפוס 29',
        )
        self.assertIsNone(osher_snapshot_match(snapshot, '3', 'כתובת לא ידועה', 'ירושלים'))

    def test_dor_exact_card_beats_colocated_supermarket_label(self):
        station = {
            "name": "עינת",
            "store": OfficialStore("קיבוץ עינת", None, None, "official", 32.08, 34.94),
        }
        supermarket = {
            "name": "עינת סופר אלונית",
            "store": OfficialStore("קיבוץ עינת", None, None, "official", 32.08, 34.94),
        }
        self.assertGreater(
            dor_branch_score("עינת", "קיבוץ עינת", "עינת", station),
            dor_branch_score("עינת", "קיבוץ עינת", "עינת", supermarket),
        )

    def test_be_clock_format_handles_midnight(self):
        self.assertEqual(
            format_be_hours([{"day": "ש", "openHour": "2200", "closeHour": "2400"}]),
            "שבת: 22:00-00:00",
        )

    def test_nominatim_requires_exact_city_street_and_house_number(self):
        results = [{
            "lat": "32.0800", "lon": "34.7800",
            "display_name": "הרצל 10, תל אביב-יפו, ישראל",
            "address": {"road": "הרצל", "house_number": "10", "city": "תל אביב-יפו"},
        }, {
            "lat": "32.0810", "lon": "34.7810",
            "display_name": "הרצל 12, תל אביב-יפו, ישראל",
            "address": {"road": "הרצל", "house_number": "12", "city": "תל אביב-יפו"},
        }]
        matches = exact_candidates("הרצל 10", "תל אביב", results)
        self.assertEqual(len(matches), 1)
        self.assertEqual(unique_point(matches), (32.08, 34.78))

    def test_nominatim_rejects_distant_duplicate_exact_results(self):
        candidates = [
            {"lat": "32.0800", "lon": "34.7800"},
            {"lat": "32.0900", "lon": "34.7900"},
        ]
        self.assertIsNone(unique_point(candidates))


if __name__ == "__main__":
    unittest.main()
