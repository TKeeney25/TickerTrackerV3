import json

import jsonschema_default as jd
import jsonschema as js

PROGRESS_SCHEMA = {
    "type": "object",
    "required": ["state", "yh_calls", "ms_calls", "screen_type", "last_run", "total_tickers", "offset", "floor"],
    "properties": {
        "state": {
            "type": "string",
            "default": "ready"
        },
        "yh_calls": {
            "type": "integer",
            "default": 0
        },
        "ms_calls": {
            "type": "integer",
            "default": 0
        },
        "screen_type": {
            "type": "string",
            "default": ""
        },
        "last_run": {
            "type": "string",
            "default": ""
        },
        "total_tickers": {
            "type": "integer",
            "default": 0
        },
        "offset": {
            "type": "integer",
            "default": 0
        },
        "floor": {
            "type": "integer",
            "default": 0
        }
    }
}

DEFAULT_PROGRESS_JSON = jd.create_from(PROGRESS_SCHEMA)


def validate_progress_json(json_dict: json):
    """
    Validates the progress json against the schema.
    :param json_dict: The current json.
    """
    js.validate(json_dict, PROGRESS_SCHEMA)
