# ===============================================================================
# Description:    A test module to get Workspaces using a loop to test multithreading
# ===============================================================================


import logging
import requests
import time
import threading
import AuthToken


# ===  Configure Get Workspace threading  ===
# Pass in parameters used in looping through retrieving workspaces
class get_workspaces_thread (threading.Thread):
   # Overriding the default `__init__`
   def __init__(self, thread_id, name, delay, counter):
      print('Getting Workspaces - Thread', thread_id)
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      self.name = name
      self.delay = delay
      self.counter = counter
      self.daemon = False

   # Overriding the default subfunction `run()`
   def run(self):
      # Initiate the thread
      print("Starting " + self.name)
      get_workspaces(self.name, self.counter, self.delay)
      print("Exiting " + self.name)


# ===  Get Workspaces class  ===
# Pass in values to be used with the get Workspaces function
# This is only to demonstrate repeatedly calling an API endpoint 
# based upon the counter value
def get_workspaces(threadName, counter, delay):
    get_headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Authorization': 'Bearer ' + AuthToken.Auth.access_token
    }

    while counter:
        res = requests.get(
            'https://api.anaplan.com/2/0/workspaces', headers=get_headers)
        logging.info("List of user workspaces received")

        # Write output to file
        with open('workspaces.json', 'wb') as f:
            f.write(res.text.encode('utf-8'))

        time.sleep(delay)
        print("%s: %s" % (threadName, time.ctime(time.time())))
        counter -= 1
