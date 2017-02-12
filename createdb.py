import os
import shutil

directory = "/tmp/simpledb"

if not os.path.exists(directory):
    os.makedirs(directory)


shutil.copy2('abc.meta', directory + '/abc.meta')
shutil.copy2('abc.data', directory + '/abc.data')