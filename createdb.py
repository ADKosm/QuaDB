import os
import shutil
import sys

directory = "/tmp/simpledb"
indexes = directory + '/indexes'

if not os.path.exists(directory):
    os.makedirs(directory)

if not os.path.exists(indexes):
    os.makedirs(indexes)

if len(sys.argv) > 1:
    if sys.argv[1] == "empty":
        open(directory + '/abc.data', 'a').close()
        os.truncate(directory + '/abc.data', 0);

        open(indexes + '/abc_id.index', 'a').close()
        os.truncate(indexes + '/abc_id.index', 0);
    else:
        print("Incorrect command")
        exit(0)
else:
    shutil.copy2('abc.data', directory + '/abc.data')

shutil.copy2('abc.meta', directory + '/abc.meta')
shutil.copy2('abc_id.meta', indexes + '/abc_id.meta')