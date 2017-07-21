#!/usr/bin/env python
"""
_Execute_

Run the stage out commands in a nice non-blocking way

"""
from __future__ import print_function

from subprocess import Popen, PIPE

from WMCore.Storage.StageOutError import StageOutError

def runCommand(command):
    """
    _runCommand_

    Run the command without deadlocking stdout and stderr,

    Returns the exitCode

    """
    # capture stdout and stderr from command
    child = Popen(command, shell=True, bufsize=1, stdin=PIPE, close_fds=True)

    child.communicate()
    retCode = child.returncode

    return retCode

def runCommandWithOutput(command):
    """
    _runCommandWithOutput_

    Run the command without deadlocking stdout and stderr,
    echo all output to sys.stdout and sys.stderr

    Returns the exitCode and the a string containing std out & error

    """
    # capture stdout and stderr from command
    child = Popen(command, shell=True, bufsize=1, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)

    sout, serr = child.communicate()
    retCode = child.returncode
    
    # If child is terminated by signal, err will be negative value. (Unix only)
    sigStr = "Terminated by signal %s\n" % -retCode if retCode < 0 else ""
    output = "%sstdout: %s\nstderr: %s" % (sigStr, sout, serr)
    return retCode, output


def execute(command):
    """
    _execute_

    Execute the command provided, throw a StageOutError if it exits
    non zero

    """
    try:
        exitCode, output = runCommandWithOutput(command)
        msg = "Command exited with status: %s, Output: (%s)" % (exitCode, output)
        print(msg)
    except Exception as ex:
        msg = "Command threw exception: %s" % str(ex)
        print("ERROR: Exception During Stage Out:\n%s" % msg)
        raise StageOutError(msg, Command=command, ExitCode=60311)
    if exitCode:
        msg = "Command exited non-zero: ExitCode:%s \nOutput (%s)" % (exitCode, output)
        print("ERROR: Exception During Stage Out:\n%s" % msg)
        raise StageOutError(msg, Command=command, ExitCode=exitCode)
    return
