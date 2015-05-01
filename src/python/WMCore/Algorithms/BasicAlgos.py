#!/bin/env python

"""
_BasicAlgos_

Python implementations of basic Linux functionality

"""

import os
import stat
import time
import zlib
import subprocess

def calculateChecksums(filename):
    """
    _calculateChecksums_

    Get the adler32 and crc32 checksums of a file. Return None on error

    Process line by line and adjust for known signed vs. unsigned issues
      http://docs.python.org/library/zlib.html

    The cksum UNIX command line tool implements a CRC32 checksum that is
    different than any of the python algorithms, therefore open cksum
    in a subprocess and feed it the same chunks of data that are used
    to calculate the adler32 checksum.

    """
    adler32Checksum = zlib.adler32("")

    cksum = subprocess.Popen("cksum", stdin = subprocess.PIPE, stdout = subprocess.PIPE)

    f = open(filename, 'rb')
    while True:
        line = f.readline(4096) #limit so memory use doesn't blow up
    if not line:
        break
    adler32Checksum = zlib.adler32(line, adler32Checksum)
    cksum.stdin.write(line)
    f.close()
    cksum.stdin.close()
    cksum.wait()
    return ((adler32Checksum & 0xffffffff), cksum.stdout.read().split()[0])


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
