import json
import os

import jsonschema
from util import schema_util
from util.settings_util import PROGRESS_FILE


class ScreenType:
    ETF = "ETF"
    MF = "MUTUALFUND"


class States:
    ready = "ready"
    started = "started"
    finished = "finished"


class Properties:
    state = 'state'
    yh_calls = 'yh_calls'
    ms_calls = 'ms_calls'
    screen_type = 'screen_type'
    last_run = 'last_run'
    total_tickers = 'total_tickers'
    offset = 'offset'
    floor = 'floor'


def dump_progress():
    """
    Write current progress to json.
    """
    with open(file=PROGRESS_FILE, mode='w') as progress_file:
        json.dump(PROGRESS_JSON, progress_file)


def create_progress_file():
    """
    Create a settings file.
    """
    if not os.path.exists(PROGRESS_FILE):
        open(PROGRESS_FILE, 'x').close()


try:
    create_progress_file()
    PROGRESS_JSON = json.load(open(PROGRESS_FILE))
    schema_util.validate_progress_json(PROGRESS_JSON)
except (json.JSONDecodeError, jsonschema.ValidationError, FileNotFoundError):
    PROGRESS_JSON = schema_util.DEFAULT_PROGRESS_JSON.copy()
    dump_progress()
