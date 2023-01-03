'''
Created on Jun 16, 2009

@author: meloam
'''

import os
import sys
from types import *

class ProcessMonitor(object):
    '''
    Lets us fork (and optionally exec) processes, monitoring their exit codes
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.processList = {}
        self.returnedList = {}

    def executeAndMonitor(self, child):
        pid = child.forkAndExecute()
        self.processList[pid] = child

    def checkChildren(self, deleteOldOnes = True):
        """
            Checks the status of all our children, updating our lists
        """
        if (deleteOldOnes):
            self.returnedList = {}

        for child in self.processList:
            newstatus, newsignal = child.isRunning()
            if (not ((newstatus == True) and (newsignal == True))):
                # if they BOTH aren't true, we have something to do
                self.returnedList[child.processID] = child
                del self.processList[child.processID]
                if (child.callback):
                    child.callback( child )



class ChildProcess(object):
    """
        base class for child processes
    """

    def __init__(self):
        self.processID = -1
        self.callback  = None
        self.ourStdout = None
        self.ourStderr = None

    def setStdout(self, handle):
        self.ourStdout = handle

    def setStderr(self, handle):
        self.ourStderr = handle

    def setCallback(self, newCallback):
        self.callback = newCallback

    def forkAndExecute(self):
        pid = os.fork()
        if (not pid):
            try:
                # we're in the child
                if (self.ourStderr):
                    sys.stderr = self.ourStderr
                if (self.ourStdout):
                    sys.stderr = self.ourStdout
                exitCode = self.execute()
                print("Falling through ChildProcess.forkAndExecute with code %s" %\
                         exitCode)
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit( exitCode )
            except Exception as e:
                print("Something bad happened in ChildProcess.forkAndExecute in the child: %s" % e)
                os._exit(99)

        else:
            # we're in the parent
            self.processID = pid
            return pid

        raise RuntimeError("Something bad happened in fork()")

    def execute(self):
        """
            overridden in child classes to provide specific things to do
        """
        msg = "ProcessMonitor.ProcessMonitor.execute method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError(msg)

    def isRunning(self):
        if (self.processID == -1):
            raise RuntimeError("Trying to waitpid on nonexistant process")
        pid, status = os.waitpid( self.processID, os.WNOHANG )
        if ((pid == 0) and (status == 0)):
            # we're definately still running if this pops up
            return (True, True)
        else:
            # the child returned, get the status back from it
            # os.waitpid does dumb things to the exit code
            self.realstatus = ((0xFF00 & status) >> 8)
            self.realsignal = ((0x00FF & status))

            return (self.realstatus, self.realsignal)

class ExecProcess(ChildProcess):
    """
        calls a given external executable
    """

    def __init__(self):
        self.args = []

    def setArgs(self, arguments):
        """
            Either accepts a list of arguments which are passed to execvp
            OR     accepts a string which is passed to bash and shell-expanded
        """
        if not isinstance(arguments, list):
            # we got passed a string, pass it to a shell
            self.args[0] = 'bash'
            self.args[1] = '-c'
            self.args[2] = arguments
        else:
            # we got passed a list
            self.args = arguments

    def execute(self):
        if (self.args == []):
            raise RuntimeError("No arguments were set")
        os.execvp(self.args[0], self.args[1:])

class PythonProcess(ChildProcess):
    """
        Calls a function specified by the user
        to pass arguments, wrap it in a lambda
    """
    def __init__(self):
        self.target = None

    def setTarget(self, newtarget):
        if not isinstance(newtarget, (FunctionType, LambdaType)):
            raise RuntimeError("PythonProcess requires a function for target")

        self.target = newtarget

    def execute(self):
        if (self.target == None):
            raise RuntimeError("No execute process was set")
        return self.target()
