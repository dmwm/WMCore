#!/bin/env python

"""
_SubprocessAlgos_

Little tricks you can do on the command line with Subprocess
i.e., stand-ins for Linux command line functions

"""

from builtins import str
import os
import re
import signal
import logging
import subprocess

from WMCore.Algorithms.Alarm import Alarm, alarmHandler
from WMCore.WMException      import WMException
from Utils.Utilities import decodeBytesToUnicode
from Utils.PythonVersion import PY3

class SubprocessAlgoException(WMException):
    """
    _SubprocessAlgoException_

    Clever exception that does nothing.  Cleverly.
    """
    pass


def findPIDs(name, user = os.getpid()):
    """
    Finds the PIDs for a process with name name being used by a user with a certain uid

    """

    pids = []

    ps = subprocess.Popen(['ps', '-u', user, 'w'], stdout=subprocess.PIPE).communicate()[0]
    processes = ps.split('\n')

    for line in processes:
        if len(line.split()) < 5:
            continue
        if re.match(name, line.split()[4]):
            #Then we have matching process
            pids.append(line.split()[0])

    return pids


def killProcessByName(name, user = os.getpid(), sig = None):
    """
    Kills all processes of a certain type (name)

    """

    pids = findPIDs(name = name, user = user)

    if len(pids) == 0:
        #We have no processes to kill of this type
        return pids

    command = ['kill']
    if sig:
        command.append('-%i' % sig)
    for pid in pids:
        command.append(pid)

    subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]


    return pids


def tailNLinesFromFile(file_, n):
    """
    Loads the last N lines from a file

    """

    if not os.path.isfile(file_):
        return None

    command = ['tail', '-n', str(n), file_]

    output = subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]

    return output.split('\n')



def runCommand(cmd, shell = True, timeout = None):
    """
    Run generic command

    This is NOT secure and hence NOT recommended
    It does however have the timeout functions built into it
    timeout must be an int
    Note, setting timeout = 0 does nothing!
    """

    if timeout:
        if not isinstance(timeout, int):
            timeout = None
            logging.error("SubprocessAlgo.runCommand expected int timeout, got %s", timeout)
        else:
            signal.signal(signal.SIGALRM, alarmHandler)
            signal.alarm(timeout)
    try:
        pipe = subprocess.Popen(cmd, stdout = subprocess.PIPE,
                                stderr = subprocess.PIPE, shell = shell)
        stdout, stderr = pipe.communicate()
        if PY3:
            stdout = decodeBytesToUnicode(stdout)
            stderr = decodeBytesToUnicode(stderr)
        returnCode     = pipe.returncode
    except Alarm:
        msg =  "Alarm sounded while running command after %s seconds.\n" % timeout
        msg += "Command: %s\n" % cmd
        msg += "Raising exception"
        logging.error(msg)
        raise SubprocessAlgoException(msg)

    if timeout:
        signal.alarm(0)

    return stdout, stderr, returnCode
