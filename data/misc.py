import glob
import os

# A little helper script for renaming things when necessary
for pathAndFilename in glob.iglob(os.path.join('.', '*ngrid*')):
    file_arr = pathAndFilename.split('_')
    hash_and_filetype = file_arr[len(file_arr) - 1]
    to_rename = 'geocoded_2016_ngrid(boston_gas)_leaks_' + hash_and_filetype
    os.rename(pathAndFilename, to_rename)
