import datetime
from typing import Optional

temp = '''
longName = :longName,
            quoteType = :quoteType,
            firstTradeDate = :firstTradeDate,
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
            triggerable = :triggerable
        WHERE :symbol = symbol;'''


class Response:
    def __init__(self, api_id):
        self.api_id = api_id

    def get_vars(self):
        var = vars(self).copy()
        var.pop('api_id')
        return var

    def to_dict(self):
        return_dict = {}
        all_vars = self.get_vars()
        for var in all_vars:
            if isinstance(all_vars[var], Response):
                return_dict[var] = all_vars[var].to_dict()
            else:
                return all_vars[var]
        return return_dict


class DataResponse(Response):
    def __init__(self, api_id, data):
        super().__init__(api_id)
        self.api_id = api_id
        try:
            self.data = data[api_id]
        except (KeyError, TypeError):
            self.data = None


class IntegerResponse(DataResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.get_integer()

    def get_integer(self) -> Optional[int]:
        if self.data is None:
            return None
        if not isinstance(self.data, int):
            self.data = None


class RealResponse(DataResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.get_real_number()

    def get_real_number(self) -> Optional[float]:
        if self.data is None:
            return None
        if 'raw' in self.data:
            self.data = self.data['raw']
        try:
            self.data = float(self.data)
        except ValueError:
            self.data = None


class TextResponse(DataResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.get_text()

    def get_text(self):
        if self.data is None:
            return None
        self.data = str(self.data)


class ScreenerResponse(Response):
    def __init__(self, data):
        super().__init__('main')
        self.symbol = TextResponse('symbol', data)
        self.longName = TextResponse('longName', data)
        self.quoteType = TextResponse('quoteType', data)
        self.firstTradeDateMilliseconds = IntegerResponse('firstTradeDateMilliseconds', data)
        self.exchange = TextResponse('exchange', data)
        self.market = TextResponse('market', data)
        self.marketCap = IntegerResponse('marketCap', data)
        self.marketState = TextResponse('marketState', data)
        self.priceHint = IntegerResponse('priceHint', data)
        self.priceToBook = RealResponse('priceToBook', data)
        self.regularMarketChange = RealResponse('regularMarketChange', data)
        self.regularMarketChangePercent = RealResponse('regularMarketChangePercent', data)
        self.regularMarketPreviousClose = RealResponse('regularMarketPreviousClose', data)
        self.regularMarketPrice = RealResponse('regularMarketPrice', data)
        self.sharesOutstanding = IntegerResponse('sharesOutstanding', data)
        self.tradeable = TextResponse('tradeable', data)
        self.triggerable = TextResponse('triggerable', data)
