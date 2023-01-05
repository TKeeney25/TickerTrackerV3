import json
from unittest import TestCase
from test_defaults import SCREEN_RESULTS

from structures import ScreenerResponse



class TestScreenerResponse(TestCase):
    def test_runs_without_error(self):
        for quote in json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes']:
            self.assertTrue(ScreenerResponse(quote))

    def test_has_valid_to_dict(self):
        valid_return = {'symbol': 'VSTSX',
                        'longName': 'Vanguard Total Stock Market Index Fund Institutional Select Shares',
                        'quoteType': 'MUTUALFUND', 'firstTradeDateMilliseconds': 1459258200000, 'exchange': 'NAS',
                        'market': 'us_market', 'marketCap': None, 'marketState': 'REGULAR', 'priceHint': 2,
                        'priceToBook': 1.5521913, 'regularMarketChange': 3.79, 'regularMarketChangePercent': 1.7814336,
                        'regularMarketPreviousClose': 212.75, 'regularMarketPrice': 216.54, 'sharesOutstanding': None,
                        'tradeable': 'False', 'triggerable': 'False'}
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        result = ScreenerResponse(quote).to_dict()
        self.assertDictEqual(valid_return, result)
