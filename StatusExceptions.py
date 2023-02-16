# === Process REST API endpoint exceptions ===
# ===============================================================================
# Description:   Process REST API endpoint exceptions
# ===============================================================================

import logging
import json


# Enable logger
logger = logging.getLogger(__name__)


# Log exceptions to logger
def process_status_exceptions(res, uri):
    # Override linting
    # pyright: reportUnboundVariable=false

    if res.status_code == 401:
        logger.error('%s with URI: %s', json.loads(
            res.text)['error_description'], uri)
    elif res.status_code == 403:
        logger.error('%s with URI: %s', json.loads(
            res.text)['error_description'], uri)
    elif res.status_code == 404:
        logger.error('%s with URL: %s', json.loads(
            res.text)['message'], uri)
        logger.error('Please check device code or service URI')
        print('ERROR - Please check logs')

