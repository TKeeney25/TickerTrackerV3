import json
from unittest import TestCase
from test_defaults import *

from structures import *


class TestScreenerResponse(TestCase):
    def test_runs_without_error(self):
        for quote in json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes']:
            self.assertTrue(ScreenerResponse(quote))

    def test_has_valid_to_dict(self):
        valid_return = {'symbol': 'VSTSX',
                        'longName': 'Vanguard Total Stock Market Index Fund Institutional Select Shares',
                        'quoteType': 'MUTUALFUND', 'firstTradeDateMilliseconds': 1459258200000, 'exchange': 'NAS',
                        'market': 'us_market', 'marketCap': 1833714843648, 'marketState': 'REGULAR', 'priceHint': 2,
                        'priceToBook': 1.5521913, 'regularMarketChange': 3.79, 'regularMarketChangePercent': 1.7814336,
                        'regularMarketPreviousClose': 212.75, 'regularMarketPrice': 216.54,
                        'sharesOutstanding': 8468250112,
                        'tradeable': 'True', 'triggerable': 'False'}
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        result = ScreenerResponse(quote).to_dict()
        self.assertDictEqual(valid_return, result)


class TestMSFinanceResponse(TestCase):
    def test_has_valid_to_dict(self):
        valid_result = {'starRating': 2, 'symbol': 'FDN'}
        result = MSFinanceResponse(json.loads(MS_GET_DETAIL)[0]).to_dict()
        self.assertDictEqual(valid_result, result)


class TestYHFinanceResponse(TestCase):
    def test_has_valid_to_dict(self):
        valid_result = {'symbol': 'FDN', 'beta3Year': 1.09, 'totalAssets': 4079491840, 'fundFamily': 'First Trust',
                        'percent_yield': 0.0, 'category': 'Technology', 'ytd': -0.4552694, 'lastBearMkt': 0.0,
                        'lastBullMkt': 0.0, 'oneMonth': -0.016849, 'threeMonth': -0.0355632, 'oneYear': -0.45909852,
                        'threeYear': -0.041809402, 'fiveYear': 0.0733182, 'tenYear': 0.1240088,
                        'annualTotalReturns': [{'year': '2022', 'annualValue': None},
                                               {'year': '2021', 'annualValue': 0.0643247},
                                               {'year': '2020', 'annualValue': 0.526452},
                                               {'year': '2019', 'annualValue': 0.1926275},
                                               {'year': '2018', 'annualValue': 0.062289402},
                                               {'year': '2017', 'annualValue': 0.3762376},
                                               {'year': '2016', 'annualValue': 0.069141105},
                                               {'year': '2015', 'annualValue': 0.21765381},
                                               {'year': '2014', 'annualValue': 0.0242313},
                                               {'year': '2013', 'annualValue': 0.5339656},
                                               {'year': '2012', 'annualValue': 0.2084882},
                                               {'year': '2011', 'annualValue': -0.057432},
                                               {'year': '2010', 'annualValue': 0.3668457},
                                               {'year': '2009', 'annualValue': 0.7915774},
                                               {'year': '2008', 'annualValue': -0.44022572},
                                               {'year': '2007', 'annualValue': 0.1117595},
                                               {'year': '2006', 'annualValue': None}], 'twelveBOne': None}
        result = YHFinanceResponse(json.loads(YH_GET_SUMMARY)).to_dict()
        self.assertDictEqual(valid_result, result)


class TestPerformanceIdResponse(TestCase):
    def test_has_valid_to_dict(self):
        valid_result = {'performanceId': '0P0000603I', 'symbol': 'FDN'}
        result = PerformanceIdResponse(json.loads(PERFORMANCE_ID_RESULTS)[0]).to_dict()
        self.assertDictEqual(valid_result, result)

