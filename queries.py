import datetime
from dateutil.relativedelta import *
import sqlite3
import time

CREATE_FUNDS_TABLE = '''
CREATE TABLE IF NOT EXISTS funds (
    symbol TEXT NOT NULL,
    performanceId TEXT,
    longName TEXT,
    quoteType TEXT,
    firstTradeDateMilliseconds INTEGER,
    lastScreened INTEGER,
    yhFinanceLastAcquired INTEGER,
    msFinanceLastAcquired INTEGER,
    ytd REAL,
    lastBearMkt REAL,
    lastBullMkt REAL,
    oneMonth REAL,
    threeMonth REAL,
    oneYear REAL,
    threeYear REAL,
    fiveYear REAL,
    tenYear REAL,
    beta3Year REAL,
    bookValue REAL,
    category TEXT,
    exchange TEXT,
    fundFamily TEXT,
    market TEXT,
    marketCap INTEGER,
    marketState TEXT,
    priceHint INTEGER,
    priceToBook REAL,
    regularMarketChange REAL,
    regularMarketChangePercent REAL,
    regularMarketPreviousClose REAL,
    regularMarketPrice REAL,
    sharesOutstanding INTEGER,
    starRating INTEGER,
    totalAssets INTEGER,
    twelveBOne REAL,
    tradeable TEXT,
    triggerable TEXT,
    yield REAL,
    PRIMARY KEY (symbol),
    UNIQUE (performanceId)
);
'''

DROP_FUNDS_TABLE = '''DROP TABLE IF EXISTS funds;'''

CREATE_ANNUALTOTALRETURNS_TABLE = '''
CREATE TABLE IF NOT EXISTS annualTotalReturns (
    symbol TEXT NOT NULL,
    year INTEGER NOT NULL,
    return REAL,
    PRIMARY KEY (symbol, year),
    FOREIGN KEY (symbol) REFERENCES funds(symbol) ON DELETE CASCADE
);
'''

DROP_ANNUALTOTALRETURNS_TABLE = '''DROP TABLE IF EXISTS annualTotalReturns;'''

ADD_FROM_SCREENER = ['''
IF (:symbol IN (SELECT symbol FROM funds)
    THEN
        UPDATE funds
        SET
            longName = :longName,
            quoteType = :quoteType,
            firstTradeDateMilliseconds = :firstTradeDateMilliseconds,
            exchange = :exchange,
            market = :market,
            marketCap = :marketCap,
            marketState = :marketState,
            priceHint = :priceHint,
            priceToBook = :priceToBook,
            regularMarketChange = :regularMarketChange,
            regularMarketChangePercent = :regularMarketChangePercent,
            regularMarketPreviousClose = :regularMarketPreviousClose,
            regularMarketPrice = :regularMarketPrice,
            sharesOutstanding = :sharesOutstanding,
            tradeable = :tradeable,
            triggerable = :triggerable
        WHERE :symbol = symbol;
    ELSE
        INSERT INTO funds (
            symbol,
            longName,
            quoteType,
            firstTradeDateMilliseconds,
            exchange,
            market,
            marketCap,
            marketState,
            priceHint,
            priceToBook,
            regularMarketChange,
            regularMarketChangePercent,
            regularMarketPreviousClose,
            regularMarketPrice,
            sharesOutstanding,
            tradeable,
            triggerable,
        ) VALUES (
            :symbol,
            :longName,
            :quoteType,
            :firstTradeDateMilliseconds,
            :exchange,
            :market,
            :marketCap,
            :marketState,
            :priceHint,
            :priceToBook,
            :regularMarketChange,
            :regularMarketChangePercent,
            :regularMarketPreviousClose,
            :regularMarketPrice,
            :sharesOutstanding,
            :tradeable,
            :triggerable
        );
    END IF;''',
                     'UPDATE funds SET lastScreened = GETDATE() WHERE :symbol = symbol;']

UPDATE_FROM_YH_FINANCE = '''
BEGIN TRANSACTION;
IF (:symbol IN (SELECT symbol FROM funds)
    THEN
        UPDATE funds
        SET
            ytd = :ytd,
            lastBearMkt = :lastBearMkt,
            lastBullMkt = :lastBullMkt,
            oneMonth = :oneMonth,
            threeMonth = :threeMonth,
            oneYear = :oneYear,
            threeYear = :threeYear,
            fiveYear = :fiveYear,
            beta3Year = :beta3Year,
            category = :category,
            totalAssets = :totalAssets,
            fundFamily = :fundFamily,
            yield = :yield,
            yhFinanceLastAcquired = GETDATE())
        WHERE :symbol = symbol;
    END IF;
COMMIT TRANSACTION;
'''

ADD_ANNUALTOTALRETURN = '''
BEGIN TRANSACTION;
IF (EXISTS (SELECT * FROM annualTotalReturns WHERE :symbol = symbol AND :year = year))
    THEN
        UPDATE annualTotalReturns
        SET
            return = :return
        WHERE :symbol = symbol AND :year = year;
    ELSE
        INSERT INTO annualTotalReturns VALUES (
            :symbol,
            :year,
            :return
        );
    END IF;
COMMIT TRANSACTION;
'''

UPDATE_FROM_MS_FINANCE = '''
BEGIN TRANSACTION;
IF (:symbol IN (SELECT symbol FROM funds)
    THEN
        UPDATE funds
        SET
            performanceId = :performanceId,
            starRating = :starRating
        WHERE :symbol = symbol;
    END IF;
COMMIT TRANSACTION;
'''


class DB:
    def __init__(self, dbname='tickerTracker.db'):
        self.connection = sqlite3.connect(dbname)
        self.cursor = self.connection.cursor()

    def close_connections(self):
        self.cursor.close()
        self.connection.close()

    def create_tables(self):
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            self.cursor.execute(CREATE_FUNDS_TABLE)
            self.cursor.execute(CREATE_ANNUALTOTALRETURNS_TABLE)
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error

    def drop_tables(self):
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            self.cursor.execute(DROP_ANNUALTOTALRETURNS_TABLE)
            self.cursor.execute(DROP_FUNDS_TABLE)
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error

    def add_from_screener(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    longName = :longName,
                    quoteType = :quoteType,
                    firstTradeDateMilliseconds = :firstTradeDateMilliseconds,
                    exchange = :exchange,
                    market = :market,
                    marketCap = :marketCap,
                    marketState = :marketState,
                    priceHint = :priceHint,
                    priceToBook = :priceToBook,
                    regularMarketChange = :regularMarketChange,
                    regularMarketChangePercent = :regularMarketChangePercent,
                    regularMarketPreviousClose = :regularMarketPreviousClose,
                    regularMarketPrice = :regularMarketPrice,
                    sharesOutstanding = :sharesOutstanding,
                    tradeable = :tradeable,
                    triggerable = :triggerable
                WHERE :symbol = symbol;''', data)
            else:
                self.cursor.execute('''INSERT INTO funds (
                    symbol,
                    longName,
                    quoteType,
                    firstTradeDateMilliseconds,
                    exchange,
                    market,
                    marketCap,
                    marketState,
                    priceHint,
                    priceToBook,
                    regularMarketChange,
                    regularMarketChangePercent,
                    regularMarketPreviousClose,
                    regularMarketPrice,
                    sharesOutstanding,
                    tradeable,
                    triggerable
                ) VALUES (
                    :symbol,
                    :longName,
                    :quoteType,
                    :firstTradeDateMilliseconds,
                    :exchange,
                    :market,
                    :marketCap,
                    :marketState,
                    :priceHint,
                    :priceToBook,
                    :regularMarketChange,
                    :regularMarketChangePercent,
                    :regularMarketPreviousClose,
                    :regularMarketPrice,
                    :sharesOutstanding,
                    :tradeable,
                    :triggerable
                );''', data)
            self.cursor.execute('UPDATE funds SET lastScreened = :unix_time WHERE :symbol = symbol;', data)
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error

    def update_from_yh_finance(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    ytd = :ytd,
                    lastBearMkt = :lastBearMkt,
                    lastBullMkt = :lastBullMkt,
                    oneMonth = :oneMonth,
                    threeMonth = :threeMonth,
                    oneYear = :oneYear,
                    threeYear = :threeYear,
                    fiveYear = :fiveYear,
                    tenYear = :tenYear,
                    beta3Year = :beta3Year,
                    category = :category,
                    totalAssets = :totalAssets,
                    fundFamily = :fundFamily,
                    yield = :percent_yield,
                    twelveBOne = :twelveBOne
                WHERE :symbol = symbol;''', data)
            else:
                raise sqlite3.OperationalError(f'symbol {data["symbol"]} is not in the database.')
            self.cursor.execute('DELETE FROM annualTotalReturns WHERE :symbol = symbol;', data)
            for single_return in data['annualTotalReturns']:
                single_return['symbol'] = data['symbol']
                self.cursor.execute('INSERT INTO annualTotalReturns VALUES (:symbol, :year, :annualValue);',
                                    single_return)
            self.cursor.execute('UPDATE funds SET yhFinanceLastAcquired = :unix_time WHERE :symbol = symbol;', data)
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error

    def update_from_ms_finance(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    starRating = :starRating
                WHERE :symbol = symbol;''', data)
            else:
                raise sqlite3.OperationalError(f'symbol {data["symbol"]} is not in the database.')
            self.cursor.execute('UPDATE funds SET msFinanceLastAcquired = :unix_time WHERE :symbol = symbol;', data)
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error

    def update_performance_id(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    performanceId = :performanceId
                WHERE :symbol = symbol;''', data)
            else:
                raise sqlite3.OperationalError(f'symbol {data["symbol"]} is not in the database.')
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error

    def valid_for_yh_finance_view(self) -> set:
        sql = '''
        SELECT symbol FROM funds WHERE yhFinanceLastAcquired <= :lastMonthEpoch
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, {'lastMonthEpoch': get_last_month_epoch_ms()}).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error
        return set(selection) & self.valid_funds()

    def valid_for_ms_finance_view(self) -> set:
        sql = '''
        SELECT performanceId FROM funds WHERE msFinanceLastAcquired <= :lastMonthEpoch AND performanceId IS NOT NULL
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, {'lastMonthEpoch': get_last_month_epoch_ms()}).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error
        return set(selection) & self.valid_funds()

    def valid_for_perf_id_view(self) -> set:
        sql = '''
        SELECT symbol FROM funds WHERE performanceId IS NULL;
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error
        return set(selection) & self.valid_funds()

    def valid_funds(self) -> set:
        # TODO add dynamic filter recognition (1 filter file that automatically filters from screen, yh, & ms)
        data = {
            'epoch_ms_ten_years': get_epoch_from_ms(years=10),
            'lastMonthEpoch': get_last_month_epoch_ms()
        }
        sql = '''
            SELECT symbol FROM funds WHERE 
                firstTradeDateMilliseconds < :epoch_ms_ten_years AND
                market = "us_market" AND
                tradeable = "True" AND (
                    yhFinanceLastAcquired IS NULL OR
                    yhFinanceLastAcquired <= :lastMonthEpoch OR (
                        tenYear > 0 AND
                        fiveYear > 0 AND
                        threeYear > 0 AND
                        oneYear > 0 AND
                        (twelveBOne IS NULL OR twelveBOne = 0)
                    )
                ) AND (
                    msFinanceLastAcquired IS NULL OR
                    msFinanceLastAcquired <= :lastMonthEpoch OR 
                    starRating > 3
                );
            '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, data).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error
        return set(selection)

    def delete_unscreened(self):
        sql = '''
        DELETE FROM funds WHERE lastScreened <= :lastMonthEpoch
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, {'lastMonthEpoch': get_last_month_epoch_ms()}).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except sqlite3.OperationalError as error:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise error
        return selection


global db
db = None

def db_start(testing=False):
    global db
    if testing:
        db = DB(':memory:')
    else:
        db = DB()
    return db


def unix_time():
    return int(time.time_ns() / 1000)


def get_last_month_epoch_ms():
    today = datetime.datetime.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    return int(last_month.timestamp() * 1000)


def get_epoch_from_ms(days=0, months=0, years=0):
    today = datetime.datetime.today() - relativedelta(years=years, months=months, days=days)
    return int(today.timestamp() * 1000)


if __name__ == '__main__':
    db = db_start()
    print(db.cursor.execute('SELECT * FROM funds').fetchall())
    db.close_connections()
