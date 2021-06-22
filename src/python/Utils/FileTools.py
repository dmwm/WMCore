#!/usr/bin/env python
"""
Utilities related to file handling
"""

from __future__ import print_function, division

import io
import os
import stat
import subprocess
import time
import zlib

from Utils.Utilities import decodeBytesToUnicode
from Utils.PythonVersion import PY3

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
    adler32Checksum = 1  # adler32 of an empty string
    cksumProcess = subprocess.Popen("cksum", stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    # the lambda basically creates an iterator function with zero
    # arguments that steps through the file in 4096 byte chunks
    with open(filename, 'rb') as f:
        for chunk in iter((lambda: f.read(4096)), b''):
            adler32Checksum = zlib.adler32(chunk, adler32Checksum)
            cksumProcess.stdin.write(chunk)

    cksumProcess.stdin.close()
    cksumProcess.wait()

    cksumStdout = cksumProcess.stdout.read().split()
    cksumProcess.stdout.close()

    # consistency check on the cksum output
    filesize = os.stat(filename)[stat.ST_SIZE]
    if len(cksumStdout) != 2 or int(cksumStdout[1]) != filesize:
        raise RuntimeError("Something went wrong with the cksum calculation !")

    if PY3:
        # using native-string approach. convert from bytes to unicode in
        # python 3 only.
        cksumStdout[0] = decodeBytesToUnicode(cksumStdout[0])
    return (format(adler32Checksum & 0xffffffff, '08x'), cksumStdout[0])


def tail(filename, nLines=20):
    """
    _tail_

    A version of tail
    Adapted from code on http://stackoverflow.com/questions/136168/get-last-n-lines-of-a-file-with-python-similar-to-tail
    """
    assert nLines >= 0
    pos, lines = nLines + 1, []

    # make sure only valid utf8 encoded chars will be passed along
    with io.open(filename, 'r', encoding='utf8', errors='ignore') as f:
        while len(lines) <= nLines:
            try:
                f.seek(-pos, 2)
            except IOError:
                f.seek(0)
                break
            finally:
                lines = list(f)
            pos *= 2

    text = "".join(lines[-nLines:])

    return text


def getFileInfo(filename):
    """
    _getFileInfo_

    Return file info in a friendly format
    """

    filestats = os.stat(filename)

    fileInfo = {'Name': filename,
                'Size': filestats[stat.ST_SIZE],
                'LastModification': time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime(filestats[stat.ST_MTIME])),
                'LastAccess': time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime(filestats[stat.ST_ATIME]))}
    return fileInfo


def findMagicStr(filename, matchString):
    """
    _findMagicStr_

    Parse a log file looking for a pattern string
    """
    with io.open(filename, 'r', encoding='utf8', errors='ignore') as logfile:
        # TODO: can we avoid reading the whole file
        for line in logfile:
            if matchString in line:
                yield line

def getFullPath(name, envPath="PATH"):
    """
    :param name: file name
    :param envPath: any environment variable specified for path (PATH, PYTHONPATH, etc)
    :return: full path if it is under PATH env
    """
    for path in os.getenv(envPath).split(os.path.pathsep):
        fullPath = os.path.join(path, name)
        if os.path.exists(fullPath):
            return fullPath
    return None
