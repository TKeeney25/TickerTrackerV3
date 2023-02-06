import asyncio
import json
import datetime
from time import sleep
from typing import Optional

import requests
from requests import Response, request

import util
from queries import db
from structures import ScreenerResponse, YHFinanceResponse, MSFinanceResponse, PerformanceIdResponse
from util.log_util import add_to_log, LogTypes
from util.progress_util import States, PROGRESS_JSON, dump_progress, ScreenType, Properties
from util.settings_util import FILTER_FILE, TIME_STRING, DEBUG_MODE

YH_MAX_CALLS: int = 40000
MS_MAX_CALLS: int = 40000

MAX_TOTAL: int = 5000
DEFAULT_JUMP: int = 250
DEFAULT_FLOOR: int = -1
MAX_RESULTS: int = 50
DEFAULT_DELAYS = [1, 5, 10, 60, 300, 600]

YH_HEADERS = {
    'content-type': "application/json",
    'X-RapidAPI-Host': "yh-finance.p.rapidapi.com",
    'X-RapidAPI-Key': util.settings_util.get_api_key()
}

MS_HEADERS = {
    'X-RapidAPI-Host': "ms-finance.p.rapidapi.com",
    'X-RapidAPI-Key': util.settings_util.get_api_key()
}


# region Helpers
def bad_response(response: Response) -> bool:
    if response is None:
        return True
    if not response.ok:
        return True
    if response.text is None or response.text == '':
        return True
    return False


def get_info(url, headers, params) -> Optional[dict]:
    for delay in DEFAULT_DELAYS:
        response = request('GET', url, headers=headers, params=params)
        if bad_response(response):
            sleep(delay)
            add_to_log(LogTypes.debug, f'Current Delay: {delay}')
            continue
        return json.loads(response.text)
    return None


# endregion

# region Payloads
def default_payload(operator: str, operands: []) -> json:
    return [
        {
            "operator": "AND",
            "operands": [
                {
                    "operator": operator,
                    "operands": operands
                },
                {
                    "operator": "eq",
                    "operands": [
                        "region",
                        "us"
                    ]
                }
            ]
        }
    ]


def gt_payload(gt: float) -> json:
    return default_payload('gt', ["intradayprice", gt])


def btwn_payload(floor: float, roof: float) -> json:
    if roof <= floor:
        raise ValueError('Roof value must be greater than floor value.')
    return default_payload('btwn', ['intradayprice', floor, roof + .0001])


# endregion

# region Screen
def get_json_from_response(response: Response) -> json:
    if bad_response(response):
        return None
    json_response = json.loads(response.text)
    if json_response['finance']['error'] is not None:
        add_to_log(LogTypes.debug,
                   f"Error on response. Code {json_response['finance']['error']['code']}, "
                   f"Description {json_response['finance']['error']['description']}")
        add_to_log(LogTypes.debug, f"Given response is {str(response)}")
        return None
    json_response = json_response["finance"]["result"][0]
    return json_response


def get_screen(quote_type: str, offset: int, payload: json) -> dict:
    """
    Screens data from yahoo finance following the payload inputted.
    :param quote_type: Type of fund being screened.
    :param offset: Offset from the start of the responses. Needed since the max size is 50.
    :param payload: Json payload containing the logic used to screen.
    :return: Dictionary containing useful json.
    """
    delays = [1, 5, 10, 60, 300]  # Progressing delay in seconds for server-side API issues.
    url = "https://yh-finance.p.rapidapi.com/screeners/list"
    querystring = {"quoteType": quote_type, "sortField": "intradayprice", "region": "US", "size": "50",
                   "offset": offset,
                   "sortType": "ASC"}
    add_to_log(LogTypes.debug, f'Payload: {payload}')
    add_to_log(LogTypes.debug, f'Params:  {querystring}')
    for delay in delays:
        response = requests.request("POST", url, json=payload, headers=YH_HEADERS, params=querystring)
        PROGRESS_JSON["yh_calls"] += 1
        dump_progress()
        json_response = get_json_from_response(response)
        if json_response:
            return json_response
        add_to_log(LogTypes.debug, f"Current Delay: {delay}")
        sleep(delay)


def get_symbols_from_quotes(quotes: list) -> str:
    return_str = ""
    for quote in quotes:
        return_str += quote["symbol"] + "\n"
    return return_str


async def screen_all(db_queue: asyncio.Queue):
    if PROGRESS_JSON['state'] == States.ready:
        PROGRESS_JSON['state'] = States.started
        PROGRESS_JSON['last_run'] = datetime.date.today().strftime(TIME_STRING)
        await screen_funds(db_queue, ScreenType.MF)
        await screen_funds(db_queue, ScreenType.ETF)
    elif PROGRESS_JSON['state'] == States.started:
        if PROGRESS_JSON['screen_type'] == ScreenType.MF:
            await screen_funds(db_queue, ScreenType.MF, PROGRESS_JSON['offset'], PROGRESS_JSON['floor'])
            await screen_funds(db_queue, ScreenType.ETF)
        else:
            await screen_funds(db_queue, ScreenType.ETF, PROGRESS_JSON['offset'], PROGRESS_JSON['floor'])
    else:
        return
    PROGRESS_JSON['state'] = States.finished
    dump_progress()


async def screen_funds(db_queue: asyncio.Queue, screen_type: str, offset=0, floor=DEFAULT_FLOOR,
                       roof=DEFAULT_FLOOR + DEFAULT_JUMP):
    if offset >= MAX_TOTAL or offset < 0:
        add_to_log(LogTypes.info, f'Offset of: {offset} reset to 0')
        offset = 0

    PROGRESS_JSON['state'] = States.started
    PROGRESS_JSON['screen_type'] = screen_type
    PROGRESS_JSON['last_run'] = datetime.date.today().strftime(TIME_STRING)
    count = offset

    json_response = get_screen(screen_type, offset, gt_payload(floor))
    total_tickers = json_response["total"]
    if DEBUG_MODE:
        total_tickers = 100
    count += json_response['count']
    offset += json_response['count']
    for fund in json_response['quotes']:
        await db_queue.put((db.add_from_screener, ScreenerResponse(fund).to_dict()))

    PROGRESS_JSON["total_tickers"] = total_tickers
    PROGRESS_JSON["offset"] = offset
    PROGRESS_JSON["floor"] = floor

    dump_progress()

    while count < total_tickers - MAX_RESULTS:
        between_total_tickers = total_tickers
        while offset < between_total_tickers:
            if offset >= MAX_TOTAL:
                add_to_log(LogTypes.error, 'Offset is greater than the max offset')
                raise Exception('Offset is greater than the max offset')
            json_response = get_screen(screen_type, offset, btwn_payload(floor, roof))
            between_total_tickers = json_response["total"]
            if between_total_tickers >= MAX_TOTAL:
                roof -= (roof - floor) / 4  # Change divisor to change rate of approach (1 = instant)
                roof = round(roof, 4)
                add_to_log(LogTypes.info, f'Dropping roof to {roof}')
            count += json_response['count']
            offset += json_response['count']
            for fund in json_response['quotes']:
                await db_queue.put((db.add_from_screener, ScreenerResponse(fund).to_dict()))

            PROGRESS_JSON["yh_calls"] += 1
            PROGRESS_JSON["offset"] = offset
            PROGRESS_JSON["floor"] = floor

            dump_progress()
        if count >= total_tickers - MAX_RESULTS:
            break
        offset = 0
        floor = roof
        roof += DEFAULT_JUMP

    json_response = get_screen(screen_type, offset, gt_payload(floor))
    for fund in json_response['quotes']:
        await db_queue.put((db.add_from_screener, ScreenerResponse(fund).to_dict()))

    PROGRESS_JSON["yh_calls"] += 1
    PROGRESS_JSON["offset"] = offset
    PROGRESS_JSON["floor"] = floor

    dump_progress()


# endregion

# region YH Finance
class YHFunctions:
    fetch_data = 1


def get_yh_info(symbol: str) -> Optional[dict]:
    url = "https://yh-finance.p.rapidapi.com/stock/v2/get-summary"
    querystring = {"symbol": symbol, "region": "US"}
    return get_info(url, headers=YH_HEADERS, params=querystring)


async def fetch_yh_data(yh_queue: asyncio.Queue):
    while PROGRESS_JSON[Properties.state] != States.finished:
        valid_symbols = db.valid_for_yh_finance_view()
        for symbol in valid_symbols:
            await yh_queue.put((GET_YH_INFO, symbol))


# endregion

# region MS Finance
def get_ms_performance_id(symbol: str) -> dict:
    url = "https://ms-finance.p.rapidapi.com/market/v2/auto-complete"
    querystring = {"q": symbol}
    return get_info(url, headers=MS_HEADERS, params=querystring)[0]


def get_ms_info(performance_id) -> dict:
    url = "https://ms-finance.p.rapidapi.com/stock/get-detail"
    querystring = {"PerformanceId": performance_id}
    return get_info(url, headers=MS_HEADERS, params=querystring)[0]


async def fetch_ms_data(ms_queue: asyncio.Queue):
    while PROGRESS_JSON[Properties.state] != States.finished:
        valid_performance_ids = db.valid_for_ms_finance_view()
        for performance_id in valid_performance_ids:
            await ms_queue.put((GET_MS_INFO, performance_id))


async def fetch_ms_performance_ids(ms_queue: asyncio.Queue):
    while PROGRESS_JSON[Properties.state] != States.finished:
        valid_symbols = db.valid_for_ms_finance_view()
        for symbol in valid_symbols:
            await ms_queue.put((GET_MS_PERFORMANCE_ID, symbol))


# endregion

# region Consumers
class Consumer:
    def __init__(self, num, requester, query, structure):
        self.num = num
        self.requester = requester
        self.query = query
        self.structure = structure

    def __eq__(self, other):
        if type(other) is Consumer:
            return other.num == self.num
        else:
            return other == self.num


async def db_consumer(db_queue: asyncio.Queue):
    while True:
        funct, args = await db_queue.get()
        if asyncio.iscoroutinefunction(funct):
            await funct(*args)
        else:
            funct(*args)
        db_queue.task_done()


async def finance_consumer(queue: asyncio.Queue, db_queue: asyncio.Queue):
    while True:
        method, args = await queue.get()
        for consumer in custom_consumers:
            if method == consumer:
                data = consumer.requester(*args)
                if data:
                    await db_queue.put((consumer.query, consumer.structure(data).to_dict()))
        queue.task_done()


GET_YH_INFO = 0
GET_MS_INFO = 1
GET_MS_PERFORMANCE_ID = 2
custom_consumers = [
    Consumer(GET_YH_INFO, get_yh_info, db.update_from_yh_finance, YHFinanceResponse),
    Consumer(GET_MS_INFO, get_ms_info, db.update_from_ms_finance, MSFinanceResponse),
    Consumer(GET_MS_PERFORMANCE_ID, get_ms_performance_id, db.update_performance_id, PerformanceIdResponse)
]


# endregion

async def fetch():
    yh_queue = asyncio.Queue()
    ms_queue = asyncio.Queue()
    db_queue = asyncio.Queue()
    screen_task = asyncio.create_task(screen_all(db_queue))
    producers = [
        asyncio.create_task(fetch_ms_data(ms_queue)),
        asyncio.create_task(fetch_ms_performance_ids(ms_queue))
    ]
    consumers = [
        asyncio.create_task(finance_consumer(yh_queue, db_queue)),
        asyncio.create_task(finance_consumer(ms_queue, db_queue)),
        asyncio.create_task(db_consumer(db_queue))
    ]
    await asyncio.gather(screen_task)
    producers.append(asyncio.create_task(fetch_yh_data(yh_queue)))
    await asyncio.gather(*producers)  # producers
    await yh_queue.join()
    await ms_queue.join()  # Implicitly awaits consumers, too
    await db_queue.join()
    for c in consumers:
        c.cancel()
