import os
import shutil
import sys

directory = "/tmp/simpledb"

if not os.path.exists(directory):
    os.makedirs(directory)

if len(sys.argv) > 1:
    if sys.argv[1] == "empty":
        open(directory + '/abc.data', 'a').close()
        os.truncate(directory + '/abc.data', 0);
    else:
        print("Incorrect command")
        exit(0)
else:
    shutil.copy2('abc.data', directory + '/abc.data')

shutil.copy2('abc.meta', directory + '/abc.meta')