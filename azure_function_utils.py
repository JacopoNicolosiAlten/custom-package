import json
import datetime
import logging
import io, os
from pytz import timezone
import azure.functions as func
import tempfile
from typing import Dict, Any

_params_path = os.path.join(tempfile.gettempdir(), 'parameters.json')

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

def get_input(req: func.HttpRequest, required_input: set) -> dict:
    '''
    Check that the body of the http request contains all the tags included in the set required_input, and return the corresponding dictionary
    '''
    try:
        req_body = req.get_json()
    except ValueError:
        raise Exception("Unable to decode json body.")
    missing_params = required_input.difference(set(req_body.keys()))
    if len(missing_params) > 0:
        raise Exception("The following required parameters are missing. {}."\
            .format(', '.join(missing_params)))
    return req_body

def initialize_params(req: func.HttpRequest, required_params: set={}, default_params: Dict[str, Any]=dict())-> None:
    '''
    save the parameters of the request in the temp file form the body
    '''
    try:
        params = req.get_json()
    except ValueError:
        raise Exception("Unable to decode json body.")
    for k, v in default_params.items():
        if k not in params.keys():
            params[k]=v
    missing_parameters = required_params.difference(set(params.keys()))
    if len(missing_parameters)>0:
        raise Exception(f'Missing parameters: {", ".join(missing_parameters)}.')
    with open(_params_path, 'w') as file:
        json.dump(params, file)
        info(json.dumps(params))
    return

def get_param(name: str)-> Any:
    '''
    get the parameter from the initialized paramters file
    '''
    with open(_params_path, 'r') as file:
        params: Dict[str, Any] = json.load(file)
    if name not in params.keys():
        raise Exception(f'The parameter "{name}" could not be found.')
    return params[name]