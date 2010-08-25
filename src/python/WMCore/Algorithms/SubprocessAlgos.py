#!/bin/env python

"""
_SubprocessAlgos_

Little tricks you can do on the command line with Subprocess
i.e., stand-ins for Linux command line functions

"""

import os
import re
import subprocess





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


def tailNLinesFromFile(file, n):
    """
    Loads the last N lines from a file

    """

    if not os.path.isfile(file):
        return None

    command = ['tail', '-n', str(n), file]

    output = subprocess.Popen(command, stdout=subprocess.PIPE).communicate()[0]

    return output.split('\n')
