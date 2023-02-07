import logging
import requests
import time
import threading
import AuthToken


# TODO add comment
class get_workspaces_thread (threading.Thread):
   # Overriding the default `__init__`
   def __init__(self, thread_id, name, delay):
      print('Getting Workspaces', thread_id)
      threading.Thread.__init__(self)
      self.thread_id = thread_id
      self.name = name
      self.delay = delay
      self.daemon = False

   # Overriding the default subfunction `run()`
   def run(self):
      # Initiate the thread
      print("Starting " + self.name)
      get_workspaces(self.name, 5, self.delay)
      print("Exiting " + self.name)


#############################################
# 4. Get Workspaces
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
