import json
import datetime
import logging
import io
from pytz import timezone
import azure.functions as func

def initialize_logger(stream: io.StringIO) -> None:
    '''
    Initialize the logger to collect logs in the given stream.
    '''
    logger = logging.getLogger('logger')
    logger.addHandler(logging.StreamHandler(stream))

def warning(message: str) -> None:
    logging.getLogger('logger').warning(message)

def info(message: str) -> None:
    '''
    Log the given message in the main logger.
    '''
    logging.getLogger('logger').info(message)

def error(message: str)-> None:
    '''
    Log the given message in the main logger.
    '''
    logging.getLogger('logger').error('Il controllo dei file ha prodotto il seguente errore.\n\n' + message)

def fail_out(error: str) -> str:
    '''
    Return a json
        {"status": "Failed",
        "datetime": <current Rome datetime>,
        "log": <error>}
    '''
    return json.dumps({'status': 'Failed',
            'log': error,
            'datetime': datetime.datetime.now(timezone('Europe/Rome'))\
                .strftime('%Y-%m-%d %H:%M:%S')})

def get_params(req: func.HttpRequest, required_params: set) -> dict:
    '''
    Check that the body of the http request contains all the tags included in the set required_params, and return the corresponding dictionary
    '''
    try:
        req_body = req.get_json()
    except ValueError:
        raise Exception("Unable to decode json body.")
    missing_params = required_params.difference(set(req_body.keys()))
    if len(missing_params) > 0:
        raise Exception("The following required parameters are missing. {}."\
            .format(', '.join(missing_params)))
    return req_body