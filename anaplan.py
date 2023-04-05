# ===============================================================================
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging

import utils
import AnaplanOauth
import Globals
import AnaplanOps

# TODO add error handling for missing kwargs error
# TODO Test w/ no network connection
# TODO Improve requests error handling
# TODO add execution of a Process
# TODO option to push all data from SQLite database to Anaplan
# TODO option to load all data from audit regardless of last incremental run
# TODO add blank header if there are no records




