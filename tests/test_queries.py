import json
import sqlite3
from datetime import datetime
from unittest import TestCase

from queries import db_start, get_last_month_epoch_ms
from structures import ScreenerResponse, YHFinanceResponse, PerformanceIdResponse, MSFinanceResponse
from test_defaults import *
db_start(testing=False)
from queries import db  # TODO add db initialization to config file.


def get_today_string():
    return datetime.now().strftime('%Y-%m-%d')


def clean_tables():
    db.drop_tables()
    db.create_tables()


class Test(TestCase):

    def test_create_new_tables(self):
        clean_tables()
        self.assertIs(len(db.cursor.execute('SELECT * FROM funds').fetchall()), 0)

    def test_add_from_screener_runs(self):
        clean_tables()
        for quote in json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes']:
            db.add_from_screener(ScreenerResponse(quote).to_dict(), )
        self.assertTrue(db.cursor.execute('SELECT * FROM funds').fetchall())

    def test_add_from_screener(self):
        valid_return = ['VSTSX', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares',
                        'MUTUALFUND', 1459258200000, 1672964259706972, None, None, None, None, None, None, None, None,
                        None, None, None, None, None, None, 'NAS', None, 'us_market', 1833714843648, 'REGULAR', 2,
                        1.5521913,
                        3.79, 1.7814336, 212.75, 216.54, 8468250112, None, None, None, 'True', 'False', None]
        clean_tables()
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote).to_dict(), )
        return_tuple = db.cursor.execute('SELECT * FROM funds').fetchone()
        valid_return[5] = return_tuple[5]
        valid_return = tuple(valid_return)
        self.assertTupleEqual(db.cursor.execute('SELECT * FROM funds').fetchone(), valid_return)
        print(db.cursor.execute('SELECT * FROM funds').fetchone())
        db.close_connections()

    def test_update_from_screener(self):
        valid_return = ['VSTSX', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares',
                        'MUTUALFUND', 1459258200000, 1672964259706972, None, None, None, None, None, None, None, None,
                        None, None, None, None, None, None, 'NAS', None, 'us_market', 1833714843648, 'REGULAR', 2,
                        1.5521913,
                        3.79, 1.7814336, 212.75, 216.54, 8468250112, None, None, None, 'True', 'False', None]
        invalid_return = ['VSTSX', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares',
                          'MUTUALFUND', 1459258200000, 1672964259706972, None, None, None, None, None, None, None, None,
                          None, None, None, None, None, None, 'NAS', None, 'nonexistent', 1833714843648, 'REGULAR', 2,
                          1.5521913,
                          3.79, 1.7814336, 212.75, 216.54, 8468250112, None, None, None, 'True', 'False', None]
        clean_tables()
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        quote['market'] = 'nonexistent'
        db.add_from_screener(ScreenerResponse(quote).to_dict(), )
        return_tuple = db.cursor.execute('SELECT * FROM funds').fetchone()
        invalid_return[5] = return_tuple[5]
        invalid_return = tuple(invalid_return)
        self.assertTupleEqual(db.cursor.execute('SELECT * FROM funds').fetchone(), invalid_return)
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote).to_dict(), )
        return_tuple = db.cursor.execute('SELECT * FROM funds').fetchone()
        valid_return[5] = return_tuple[5]
        valid_return = tuple(valid_return)
        self.assertTupleEqual(return_tuple, valid_return)

    def test_update_from_yh_finance(self):
        valid_funds_result = ['FDN', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares',
                              'MUTUALFUND', 1459258200000, None, None, None, -0.4552694, 0.0,
                              0.0, -0.016849, -0.0355632, -0.45909852, -0.041809402, 0.0733182, 0.1240088, 1.09, None,
                              'Technology', 'NAS', 'First Trust', 'us_market', 1833714843648, 'REGULAR', 2, 1.5521913,
                              3.79, 1.7814336, 212.75, 216.54, 8468250112, None, 4079491840, None, 'False', 'False',
                              0.0]
        valid_annualTotalReturns_result = [('FDN', 2022, None), ('FDN', 2021, 0.0643247), ('FDN', 2020, 0.526452),
                                           ('FDN', 2019, 0.1926275), ('FDN', 2018, 0.062289402),
                                           ('FDN', 2017, 0.3762376), ('FDN', 2016, 0.069141105),
                                           ('FDN', 2015, 0.21765381), ('FDN', 2014, 0.0242313),
                                           ('FDN', 2013, 0.5339656), ('FDN', 2012, 0.2084882), ('FDN', 2011, -0.057432),
                                           ('FDN', 2010, 0.3668457), ('FDN', 2009, 0.7915774),
                                           ('FDN', 2008, -0.44022572), ('FDN', 2007, 0.1117595), ('FDN', 2006, None)]
        clean_tables()
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][-1]
        db.add_from_screener(ScreenerResponse(quote).to_dict())
        yh_finance = json.loads(YH_GET_SUMMARY)
        db.update_from_yh_finance(YHFinanceResponse(yh_finance).to_dict())
        funds_result = db.cursor.execute('SELECT * FROM funds').fetchone()
        valid_funds_result[5] = funds_result[5]
        valid_funds_result[6] = funds_result[6]
        valid_funds_result = tuple(valid_funds_result)
        self.assertTupleEqual(valid_funds_result, funds_result)
        annualTotalReturns_result = db.cursor.execute('SELECT * FROM annualTotalReturns').fetchall()
        self.assertListEqual(valid_annualTotalReturns_result, annualTotalReturns_result)

    def test_valid_for_yh_finance_view(self):
        clean_tables()

    def test_valid_for_ms_finance_view(self):
        clean_tables()

    def test_delete_unscreened(self):
        clean_tables()
        last_month_epoch = get_last_month_epoch_ms() - 2678400000
        for quote in json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes']:
            db.add_from_screener(ScreenerResponse(quote).to_dict(), )
        db.cursor.execute('UPDATE funds SET lastScreened = :last_month_epoch;', {'last_month_epoch': last_month_epoch})
        db.cursor.execute('COMMIT TRANSACTION;')
        db.delete_unscreened()
        self.assertFalse(db.cursor.execute('SELECT * FROM funds').fetchone())
        for quote in json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes']:
            db.add_from_screener(ScreenerResponse(quote).to_dict(), )
        db.delete_unscreened()
        self.assertTrue(db.cursor.execute('SELECT * FROM funds').fetchone())

    def test_valid_funds_screen_good(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        self.assertTrue(len(db.valid_funds()) == 1)

    def test_valid_funds_screen_bad(self):
        clean_tables()
        tests = [
            json.loads(BAD_TRADE_DATE_SCREEN_FUND)['finance']['result'][0]['quotes'][0],
            json.loads(BAD_MARKET_SCREEN_FUND)['finance']['result'][0]['quotes'][0],
            json.loads(BAD_TRADEABLE_FUND)['finance']['result'][0]['quotes'][0]
        ]
        for test in tests:
            db.add_from_screener(ScreenerResponse(test).to_dict(), )
            self.assertTrue(len(db.valid_funds()) == 0)

    def test_valid_funds_yh_finance_good(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        yh_finance_data1 = json.loads(GOOD_YH_FINANCE_DATA)
        db.update_from_yh_finance(YHFinanceResponse(yh_finance_data1).to_dict(), )
        self.assertTrue(len(db.valid_funds()) == 1)

    def test_valid_funds_yh_finance_bad(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        yh_finance_data2 = json.loads(BAD_TEN_YEAR_YH_FINANCE_DATA)
        yh_finance_data3 = json.loads(BAD_12B1_FINANCE_DATA)
        db.update_from_yh_finance(YHFinanceResponse(yh_finance_data2).to_dict(), )
        self.assertTrue(len(db.valid_funds()) == 0)
        db.update_from_yh_finance(YHFinanceResponse(yh_finance_data3).to_dict(), )
        self.assertTrue(len(db.valid_funds()) == 0)

    def test_valid_funds_ms_finance_good(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        ms_finance_data1 = json.loads(GOOD_MS_FINANCE_DATA)[0]
        db.update_from_ms_finance(MSFinanceResponse(ms_finance_data1).to_dict(), )
        self.assertTrue(len(db.valid_funds()) == 1)

    def test_valid_funds_ms_finance_bad(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        ms_finance_data2 = json.loads(BAD_STAR_RATING_MS_FINANCE_DATA)[0]
        db.update_from_ms_finance(MSFinanceResponse(ms_finance_data2).to_dict(), )
        self.assertTrue(len(db.valid_funds()) == 0)

    def test_update_performance_id(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        performance_id = json.loads(PERFORMANCE_ID_RESULTS)[0]
        db.update_performance_id(PerformanceIdResponse(performance_id).to_dict(), )
        self.assertTrue(db.cursor.execute('SELECT performanceId FROM funds WHERE symbol = "FDN"').fetchone())

    def test_update_performance_id_error(self):
        clean_tables()
        performance_id = json.loads(PERFORMANCE_ID_RESULTS)[0]
        with self.assertRaises(sqlite3.OperationalError):
            db.update_performance_id(PerformanceIdResponse(performance_id).to_dict(), )

    def test_valid_for_perf_id_view_empty(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict(), )
        performance_id = json.loads(PERFORMANCE_ID_RESULTS)[0]
        db.update_performance_id(PerformanceIdResponse(performance_id).to_dict())
        self.assertFalse(db.valid_for_perf_id_view())

    def test_valid_for_perf_id_view_full(self):
        clean_tables()
        quote1 = json.loads(GOOD_SCREEN_FUND)['finance']['result'][0]['quotes'][0]
        db.add_from_screener(ScreenerResponse(quote1).to_dict())
        self.assertTrue(db.valid_for_perf_id_view())
