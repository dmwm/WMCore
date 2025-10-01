#!/usr/bin/env python
"""
_Execute_

Run the stage out commands in a nice non-blocking way

"""
from __future__ import print_function

from subprocess import Popen, PIPE, TimeoutExpired

from Utils.PythonVersion import PY3
from WMCore.Storage.StageOutError import StageOutError

def runCommand(command):
    """
    _runCommand_

    Run the command without deadlocking stdout and stderr,

    Returns the exitCode

    """
    # capture stdout and stderr from command
    if PY3:
        # python2 pylint complains about `encoding` argument
        child = Popen(command, shell=True, bufsize=1, stdin=PIPE, close_fds=True, encoding='utf8')
    else:
        child = Popen(command, shell=True, bufsize=1, stdin=PIPE, close_fds=True)

    child.communicate()
    retCode = child.returncode

    return retCode

def runCommandWithOutput(command, timeout=None):
    """
    Run the command without deadlocking stdout and stderr,
    echo all output to sys.stdout and sys.stderr
    :param command: string with the command to execute
    :param timeout: the timeout in seconds

    Returns the exitCode and the a string containing std out & error
    """
    # capture stdout and stderr from command
    child = Popen(command, shell=True, bufsize=1, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, encoding='utf8')

    sigStr = ""
    try:
        sout, serr = child.communicate(timeout=timeout)
    except TimeoutExpired:
        child.kill()  # send SIGKILL to the child process
        sout, serr = child.communicate()
        sigStr = f"Command reached timeout of {timeout} seconds "
        sigStr += "and child process was killed.\n"

    retCode = child.returncode
    # If child is terminated by signal, err will be negative value. (Unix only)
    sigStr += "Terminated by signal %s\n" % -retCode if retCode < 0 else ""
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
