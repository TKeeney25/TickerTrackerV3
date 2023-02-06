import json
import sqlite3
from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from fetch import YH_HEADERS, bad_response
from queries import db_start, get_last_month_epoch_ms
from structures import ScreenerResponse, YHFinanceResponse, PerformanceIdResponse, MSFinanceResponse
from test_defaults import *

db_start(testing=True)


def side_effect_request(method, url, json, headers, params):
    if 'https://yh-finance.p.rapidapi.com/screeners/list' == url:
        return mock_response(SCREEN_RESULTS)
    elif 'https://yh-finance.p.rapidapi.com/stock/v2/get-summary' == url:
        symbol = params['symbol']
    elif 'https://ms-finance.p.rapidapi.com/market/v2/auto-complete' == url:
        symbol = params['q']
    elif 'https://ms-finance.p.rapidapi.com/stock/get-detail' == url:
        performance_id = params['PerformanceId']


def mock_response(text, code=200):
    mock = MagicMock()
    mock.status_code = code
    mock.ok = 200 <= code < 300
    mock.text = text
    return mock


requests = MagicMock()
requests.request = side_effect_request


class Test(TestCase):

    def test_bad_response_good(self):
        url = 'https://yh-finance.p.rapidapi.com/screeners/list'
        payload = ''
        querystring = {"quoteType": 'ETF', "sortField": "intradayprice", "region": "US", "size": "50",
                       "offset": 0,
                       "sortType": "ASC"}
        response = requests.request('POST', url, json=payload, headers=YH_HEADERS, params=querystring)
        print(bad_response(response))
