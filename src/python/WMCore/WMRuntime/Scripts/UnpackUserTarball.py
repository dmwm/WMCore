#!/usr/bin/env python
"""
_UnpackUserTarball_

Unpack the user tarball and put it's contents in the right place
"""

import sys
import shutil

if __name__ == '__main__':

    tarballs = sys.argv[1].split(',')
    userFiles = sys.argv[2].split(',')

    for userFile in userFiles:
        if userFile:
            print "Moving", userFile, "to execution directory."
            shutil.move(userFile,'..')

    sys.exit(0)