import codecs
import datetime
import os
import re

import util.settings_util
from util.settings_util import LOG_FOLDER, DATA_FOLDER, SETTINGS_FILE

LOG_OLD_DATE_STR = '%y_%m_'
LOG_DATE_STR = LOG_OLD_DATE_STR + '%d_%H%M%S'
MAX_SIZE_BYTES = 5e8  # Half a gigabyte


class LogTypes:
    error = "error"
    info = "info"
    debug = "debug"


def add_to_log(log_type: str, msg: str):
    """
    Add output string to log.
    :param log_type: Must be a value within LogTypes (error, info, debug).
    :param msg: Message that will be added to the log file.
    """
    if setting_log_type == LogTypes.debug:
        _add_to_log(log_type, msg)
    elif setting_log_type == LogTypes.info:
        if log_type == LogTypes.error or log_type == LogTypes.info:
            _add_to_log(log_type, msg)
    elif setting_log_type == LogTypes.error:
        if log_type == LogTypes.error:
            _add_to_log(log_type, msg)
    else:
        raise ValueError(f'Log type {log_type} does not exist.')


def _add_to_log(log_type: str, msg: str):
    """
    Add output string to log.
    :param log_type: Must be a value within LogTypes (error, info, debug).
    :param msg: Message that will be added to the log file.
    """
    if current_log_file:
        log_file_size = os.path.getsize(f'{LOG_FOLDER}/{current_log_file}')
    if not current_log_file or log_file_size > MAX_SIZE_BYTES:
        create_log_file_name()
    with codecs.open(f'{LOG_FOLDER}/{current_log_file}', "a", 'utf-8') as log_file:
        try:
            log_file.write(f'{log_type}: {msg}\n')
        except UnicodeEncodeError:
            log_file.write(f'{log_type}: {msg.encode("ascii", "ignore").decode("ascii")}\n')


def create_log_file_name():
    """
    Create a log file with format Log_mm_dd_yy_HHMMSS.log
    and set the log file as current.
    """
    global current_log_file
    current_log_file = f'Log_{datetime.datetime.now().strftime(LOG_DATE_STR)}.log'


def delete_old_logs():
    """
    Delete all logs not made within the month.
    """
    for file_name in os.listdir(LOG_FOLDER):
        if '.log' in file_name:
            if not re.search(fr'Log_{datetime.datetime.now().strftime(LOG_OLD_DATE_STR)}\d\d_\d\d\d\d\d\d\.log',
                             file_name):
                os.remove(f'{LOG_FOLDER}/{file_name}')


def create_dirs():
    """
    Create directories used by log_util.
    """
    if not os.path.exists(LOG_FOLDER):
        os.mkdir(DATA_FOLDER)
        os.mkdir(LOG_FOLDER)
        open(SETTINGS_FILE, 'x').close()


create_dirs()
delete_old_logs()
current_log_file = None
setting_log_type = util.settings_util.get_log_type()
