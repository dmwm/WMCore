#!/usr/bin/env python
"""
_Execute_

Run the stage out commands in a nice non-blocking way

"""
from __future__ import print_function

import fcntl
import os
import select
import sys
from subprocess import Popen, PIPE

from WMCore.Storage.StageOutError import StageOutError


def makeNonBlocking(fd):
    """
    _makeNonBlocking_

    Make the file descriptor provided non-blocking

    """
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)


def runCommand(command):
    """
    _runCommand_

    Run the command without deadlocking stdou and stderr,
    echo all output to sys.stdout and sys.stderr

    Returns the exitCode

    """
    # capture stdout and stderr from command
    child = Popen(command, shell=True, bufsize=1, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    child.stdin.close()  # don't need to talk to child
    outfile = child.stdout
    outfd = outfile.fileno()
    errfile = child.stderr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)  # don't deadlock!
    makeNonBlocking(errfd)
    outdata = errdata = ''
    outeof = erreof = 0
    while True:
        ready = select.select([outfd, errfd], [], [])  # wait for input
        if outfd in ready[0]:
            try:
                outchunk = outfile.read()
            except Exception as ex:
                msg = "Unable to read stdout chunk... skipping"
                print(msg)
                outchunk = ''
            if outchunk == '': outeof = 1
            sys.stdout.write(outchunk)
        if errfd in ready[0]:
            try:
                errchunk = errfile.read()
            except Exception as ex:
                msg = "Unable to read stderr chunk... skipping"
                print(msg, str(ex))
                errchunk = ""
            if errchunk == '': erreof = 1
            sys.stderr.write(errchunk)
        if outeof and erreof: break
        select.select([], [], [], .1)  # give a little time for buffers to fill

    err = child.wait()
    if os.WIFEXITED(err):
        return os.WEXITSTATUS(err)
    elif os.WIFSIGNALED(err):
        return os.WTERMSIG(err)
    elif os.WIFSTOPPED(err):
        return os.WSTOPSIG(err)
    return err


def runCommandWithOutput(command):
    """
    _runCommand_

    Run the command without deadlocking stdout and stderr,
    echo all output to sys.stdout and sys.stderr

    Returns the exitCode and the a string containing std out & error

    """
    # capture stdout and stderr from command
    child = Popen(command, shell=True, bufsize=1, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    child.stdin.close()  # don't need to talk to child
    outfile = child.stdout
    outfd = outfile.fileno()
    errfile = child.stderr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)  # don't deadlock!
    makeNonBlocking(errfd)
    outdata = errdata = ''
    outeof = erreof = 0
    output = ''
    while True:
        ready = select.select([outfd, errfd], [], [])  # wait for input
        if outfd in ready[0]:
            try:
                outchunk = outfile.read()
            except Exception as ex:
                msg = "Unable to read stdout chunk... skipping"
                print(msg)
                outchunk = ''
            if outchunk == '': outeof = 1
            sys.stdout.write(outchunk)
            output += outchunk
        if errfd in ready[0]:
            try:
                errchunk = errfile.read()
            except Exception as ex:
                msg = "Unable to read stderr chunk... skipping"
                print(msg, str(ex))
                errchunk = ""
            if errchunk == '': erreof = 1
            output += errchunk
            sys.stderr.write(errchunk)
        if outeof and erreof: break
        select.select([], [], [], .1)  # give a little time for buffers to fill

    err = child.wait()
    if os.WIFEXITED(err):
        err = os.WEXITSTATUS(err)
    elif os.WIFSIGNALED(err):
        err = os.WTERMSIG(err)
    elif os.WIFSTOPPED(err):
        err = os.WSTOPSIG(err)
    return err, output


def execute(command):
    """
    _execute_

    Execute the command provided, throw a StageOutError if it exits
    non zero

    """
    try:
        exitCode = runCommand(command)
        msg = "Command exited with status: %s" % (exitCode)
        print(msg)
    except Exception as ex:
        msg = "Command threw exception"
        print("ERROR: Exception During Stage Out:\n")
        print(msg)
        raise StageOutError(msg, Command=command, ExitCode=60311)
    if exitCode:
        msg = "Command exited non-zero"
        print("ERROR: Exception During Stage Out:\n")
        print(msg)
        raise StageOutError(msg, Command=command, ExitCode=exitCode)
    return
