import json
import os

TIME_STRING = '%m_%d_%y'

DATA_FOLDER = 'Data'
LOG_FOLDER = f'{DATA_FOLDER}/logs'
SETTINGS_FILE = f'{DATA_FOLDER}/settings.json'
PROGRESS_FILE = f'{DATA_FOLDER}/progress.json'
FILTER_FILE = f'{DATA_FOLDER}/symbols.dat'
EDIT_FILTER_FILE = FILTER_FILE.replace('.dat', '.~dat')
ACQUIRED_SYMBOLS_FILE = f'{DATA_FOLDER}/acquired_symbols.json'
EDIT_ACQUIRED_SYMBOLS_FILE = ACQUIRED_SYMBOLS_FILE.replace('.json', '.~json')
DEBUG_MODE = False  # Mode to restrict the number of API calls


def get_api_key() -> str:
    """
    Get the current API key.
    :return: String containing the api key.
    """
    json_file = json.load(open(SETTINGS_FILE, 'r'))
    return json_file['api_key']


def get_log_type() -> str:
    """
    Get the current log type.
    :return: String containing the current log type.
    """
    json_file = json.load(open(SETTINGS_FILE, 'r'))
    return json_file['log_type']


def create_settings_file():
    """
    Create a settings file.
    """
    if not os.path.exists(SETTINGS_FILE):
        open(SETTINGS_FILE, 'x').close()
