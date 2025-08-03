import time
import os
import threading

heartbeat_lock = threading.Lock()

def update_heartbeat(path=os.path.join(os.getcwd(),'status',"heartbeat.txt")):
    with heartbeat_lock:
        with open(path, "w") as f:
            f.write(str(time.time()))
