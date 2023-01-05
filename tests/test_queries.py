import json
from datetime import datetime
from unittest import TestCase

from queries import add_from_screener, create_tables, DB, drop_tables
from structures import ScreenerResponse
from test_defaults import SCREEN_RESULTS

test_db = DB(':memory:')
def get_today_string():
    return datetime.now().strftime('%Y-%m-%d')


class Test(TestCase):

    def test_create_new_tables(self):
        drop_tables(database=test_db)
        create_tables(database=test_db)
        self.assertIs(len(test_db.cursor.execute('SELECT * FROM funds').fetchall()), 0)

    def test_add_from_screener_runs(self):
        drop_tables(database=test_db)
        create_tables(database=test_db)
        for quote in json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes']:
            add_from_screener(ScreenerResponse(quote).to_dict(), database=test_db)
        self.assertTrue(test_db.cursor.execute('SELECT * FROM funds').fetchall())

    def test_add_from_screener(self):
        valid_return = ['VSTSX', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares', 'MUTUALFUND', 1459258200000, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'NAS', None, 'us_market', None, 'REGULAR', 2, 1.5521913, 3.79, 1.7814336, 212.75, 216.54, None, None, None, None, 'False', 'False', None]
        drop_tables(database=test_db)
        create_tables(database=test_db)
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        add_from_screener(ScreenerResponse(quote).to_dict(), database=test_db)
        return_tuple = test_db.cursor.execute('SELECT * FROM funds').fetchone()
        valid_return[5] = return_tuple[5]
        valid_return = tuple(valid_return)
        self.assertTupleEqual(test_db.cursor.execute('SELECT * FROM funds').fetchone(), valid_return)


    def test_update_from_screener(self):
        valid_return = ['VSTSX', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares', 'MUTUALFUND', 1459258200000, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'NAS', None, 'us_market', None, 'REGULAR', 2, 1.5521913, 3.79, 1.7814336, 212.75, 216.54, None, None, None, None, 'False', 'False', None]
        invalid_return = ['VSTSX', None, 'Vanguard Total Stock Market Index Fund Institutional Select Shares', 'MUTUALFUND', 1459258200000, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'NAS', None, 'nonexistent', None, 'REGULAR', 2, 1.5521913, 3.79, 1.7814336, 212.75, 216.54, None, None, None, None, 'False', 'False', None]
        drop_tables(database=test_db)
        create_tables(database=test_db)
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        quote['market'] = 'nonexistent'
        add_from_screener(ScreenerResponse(quote).to_dict(), database=test_db)
        return_tuple = test_db.cursor.execute('SELECT * FROM funds').fetchone()
        invalid_return[5] = return_tuple[5]
        invalid_return = tuple(invalid_return)
        self.assertTupleEqual(test_db.cursor.execute('SELECT * FROM funds').fetchone(), invalid_return)
        quote = json.loads(SCREEN_RESULTS)['finance']['result'][0]['quotes'][0]
        add_from_screener(ScreenerResponse(quote).to_dict(), database=test_db)
        return_tuple = test_db.cursor.execute('SELECT * FROM funds').fetchone()
        valid_return[5] = return_tuple[5]
        valid_return = tuple(valid_return)
        self.assertTupleEqual(return_tuple, valid_return)
