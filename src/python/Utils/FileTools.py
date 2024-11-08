#!/usr/bin/env python
"""
Utilities related to file handling
"""

import io
import os
import glob
import stat
import subprocess
import time
import zlib
import logging

from Utils.Utilities import decodeBytesToUnicode


def findFiles(path, pat):
    """
    Find files within given path and matching given pattern.
    :param path: starting directory path (string)
    :param pat: match pattern (string), e.g. *.py or name of the file
    :return: matched file names
    """
    files = []
    for idir, _, _ in os.walk(path):
        files.extend(glob.glob(os.path.join(idir, pat)))
    return files


def tarMode(tfile, opMode):
    """
    Extract proper mode of operation for given tar file. For instance,
    if op='r' and tfile name is file.tar.gz we should get 'r:gz',
    while if tfile name is file.tar.bz2 we should get 'r':bz2', while
    if tfile name is file.tar we should get 'r', etc.
    :param opMode: mode of operation (string), e.g. 'r', or 'w'
    :param tfile: sandbox tar file name (string)
    :return: mode of operation
    """
    ext = tfile.split(".")[-1]
    if ext == "tar":
        return opMode
    mode = opMode + ":" + ext
    return mode


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
    cksumProcess = subprocess.Popen(
        "cksum", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )

    # the lambda basically creates an iterator function with zero
    # arguments that steps through the file in 4096 byte chunks
    with open(filename, "rb") as f:
        for chunk in iter((lambda: f.read(4096)), b""):
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

    cksumStdout[0] = decodeBytesToUnicode(cksumStdout[0])
    return (format(adler32Checksum & 0xFFFFFFFF, "08x"), cksumStdout[0])


def tail(filename, nLines=20):
    """
    _tail_

    A version of tail
    Adapted from code on http://stackoverflow.com/questions/136168/get-last-n-lines-of-a-file-with-python-similar-to-tail
    """
    assert nLines >= 0
    pos, lines = nLines + 1, []

    # make sure only valid utf8 encoded chars will be passed along
    with io.open(filename, "r", encoding="utf8", errors="ignore") as f:
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

    fileInfo = {
        "Name": filename,
        "Size": filestats[stat.ST_SIZE],
        "LastModification": time.strftime(
            "%m/%d/%Y %I:%M:%S %p", time.localtime(filestats[stat.ST_MTIME])
        ),
        "LastAccess": time.strftime(
            "%m/%d/%Y %I:%M:%S %p", time.localtime(filestats[stat.ST_ATIME])
        ),
    }
    return fileInfo


def findMagicStr(filename, matchString):
    """
    _findMagicStr_

    Parse a log file looking for a pattern string
    """
    with io.open(filename, "r", encoding="utf8", errors="ignore") as logfile:
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


def loadEnvFile(wmaEnvFilePath, logger=None):
    """
    _loadEnvFile_
    A simple function to load an additional bash env file into the current script
    runtime environment
    :param wmaEnvFilePath: The path to the environment file to be loaded
    :return:               True if the script has loaded successfully, False otherwise.
    """
    if not logger:
        logger = logging.getLogger()
    subProc = subprocess.run(
        [
            "bash",
            "-c",
            f'source {wmaEnvFilePath} && python -c "import os; print(repr(os.environ.copy()))" ',
        ],
        capture_output=True,
        check=False,
    )
    if subProc.returncode == 0:
        newEnv = eval(subProc.stdout)
        os.environ.update(newEnv)
        if subProc.stderr:
            logger.warning("Environment file: %s loaded with errors:", wmaEnvFilePath)
            logger.warning(subProc.stderr.decode())
        else:
            logger.info("Environment file: %s loaded successfully", wmaEnvFilePath)
        return True
    else:
        logger.error("Failed to load environment file: %s", wmaEnvFilePath)
        logger.error(subProc.stderr.decode())
        return False
