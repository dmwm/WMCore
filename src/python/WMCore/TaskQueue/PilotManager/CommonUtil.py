from JobSubmitter.Submitters.BulkSubmitterInterface import makeNonBlocking

from popen2 import Popen4
import popen2
import fcntl, select
import logging
import sys

def executeCommand(command):
    """
    _executeCommand_
    Util it execute the command provided in a popen object
    """
    print("SubmitterInterface.executeCommand:%s" % command)

    child = popen2.Popen3(command, 1) # capture stdout and stderr from command
    child.tochild.close()             # don't need to talk to child
    outfile = child.fromchild
    outfd = outfile.fileno()
    errfile = child.childerr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)
    makeNonBlocking(errfd)
    outdata = errdata = ''
    outeof = erreof = 0
    stdoutBuffer = ""
    stderrBuffer = ""
    while 1:
        ready = select.select([outfd,errfd],[],[]) # wait for input
        if outfd in ready[0]:
            outchunk = outfile.read()
            if outchunk == '': outeof = 1
            stdoutBuffer += outchunk
            sys.stdout.write(outchunk)
        if errfd in ready[0]:
            errchunk = errfile.read()
            if errchunk == '': erreof = 1
            stderrBuffer += errchunk
            sys.stderr.write(errchunk)
        if outeof and erreof: break
        select.select([],[],[],.1) # give a little time for buffers to fill

    try:
        exitCode = child.poll()
    except Exception, ex:
        msg = "Error retrieving child exit code: %s\n" % ex
        msg += "while executing command:\n"
        msg += command
        msg += "\n"
        logging.error("BulkSubmitterInterface:Failed to Execute Command")
        logging.error(msg)
        raise RuntimeError, msg

    if exitCode:
        msg = "Error executing command:\n"
        msg += command
        msg += "\n"
        msg += "Exited with code: %s\n" % exitCode
        msg += "Returned stderr: %s\n" % stderrBuffer
        logging.error("BulkSubmitterInterface:Failed to Execute Command")
        logging.error(msg)
        raise RuntimeError, msg

    return  stdoutBuffer

