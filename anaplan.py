# ===============================================================================
# Created:        2 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import requests
import json
import sys
import os
import logging
import time
import AuthToken
import threading
import time
import argparse

AuthToken.Auth.access_token = "test value"
AuthToken.Auth.refresh_token = "test value"
