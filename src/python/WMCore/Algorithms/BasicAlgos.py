#!/bin/env python

"""
_BasicAlgos_

Python implementations of basic Linux functionality

"""

import os
import stat
import time
import hashlib


def tail(filename, nLines = 20):
    """
    _tail_

    A version of tail
    Adapted from code on http://stackoverflow.com/questions/136168/get-last-n-lines-of-a-file-with-python-similar-to-tail
    """


    f = open(filename, 'r')

    assert nLines >= 0
    pos, lines = nLines+1, []
    while len(lines) <= nLines:
        try:
            f.seek(-pos, 2)
        except IOError:
            f.seek(0)
            break
        finally:
            lines = list(f)
        pos *= 2


    f.close()

    return lines[-nLines:]



def getMD5(filename, size = 8192):
    """
    _md5_

    Get the md5 checksum of a particular file
    """

    h = hashlib.md5()
    f = open(filename, 'r')

    # Read the file
    while True:
        bit = f.read(size)
        if len(bit) == 0:
            # EOF
            break
        h.update(bit)

    f.close()
    return h.hexdigest()


def getFileInfo(filename):
    """
    _getFileInfo_

    Return file info in a friendly format
    """

    filestats = os.stat(filename)

    fileInfo = {'Name': filename,
                'Size': filestats [stat.ST_SIZE],
                'LastModification': time.strftime("%m/%d/%Y %I:%M:%S %p",time.localtime(filestats[stat.ST_MTIME])),
                'LastAccess': time.strftime("%m/%d/%Y %I:%M:%S %p",time.localtime(filestats[stat.ST_ATIME]))}
    return fileInfo
