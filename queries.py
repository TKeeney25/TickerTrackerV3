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


db = DB()


def create_tables(database=db):
    database.cursor.execute('BEGIN TRANSACTION;')
    try:
        database.cursor.execute(CREATE_FUNDS_TABLE)
        database.cursor.execute(CREATE_ANNUALTOTALRETURNS_TABLE)
        database.cursor.execute('COMMIT TRANSACTION;')
    except sqlite3.OperationalError as error:
        database.cursor.execute('ROLLBACK TRANSACTION;')
        raise error


def drop_tables(database=db):
    database.cursor.execute('BEGIN TRANSACTION;')
    try:
        database.cursor.execute(DROP_ANNUALTOTALRETURNS_TABLE)
        database.cursor.execute(DROP_FUNDS_TABLE)
        database.cursor.execute('COMMIT TRANSACTION;')
    except sqlite3.OperationalError as error:
        database.cursor.execute('ROLLBACK TRANSACTION;')
        raise error

def unix_time():
    return int(time.time_ns() / 1000)

def add_from_screener(data, database=db):
    database.cursor.execute('BEGIN TRANSACTION;')
    data['unix_time'] = unix_time()
    try:
        rows = database.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol', data)
        if rows.fetchone():
            database.cursor.execute('''UPDATE funds
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
            database.cursor.execute('''INSERT INTO funds (
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
        database.cursor.execute('UPDATE funds SET lastScreened = :unix_time WHERE :symbol = symbol;', data)
        database.cursor.execute('COMMIT TRANSACTION;')
    except sqlite3.OperationalError as error:
        database.cursor.execute('ROLLBACK TRANSACTION;')
        raise error

def update_from_yh_finance(data):
    pass


if __name__ == '__main__':
    create_tables()
