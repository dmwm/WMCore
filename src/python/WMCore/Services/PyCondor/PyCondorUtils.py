from __future__ import print_function, division

import logging
import os
import pickle
import signal
import time
import traceback

import htcondor


class OutputObj(object):
    """
    Class used when AuthenticatedSubprocess is created with pickleOut
    It stores the output message of the subprocess, any output object provided
    and extra information for debugging purposes, such as the environment
    """

    def __init__(self, outputMessage, outputObj):
        self.outputMessage = outputMessage
        self.outputObj = outputObj
        self.environmentStr = ""
        for key, val in os.environ.iteritems():
            self.environmentStr += "%s=%s\n" % (key, val)


class AuthenticatedSubprocess(object):
    """
    Context manager for execution of condor commands in a forked, especially useful for
    those commands where a different proxy credential is needed.
    """

    def __init__(self, proxy=None, pickleOut=False, outputObj=None, logger=logging):
        """
        Basic setup of the context manager
        :param proxy: optional path to the proxy file
        :param pickleOut: boolean flag which enables pickled output data
        :param outputObj: structure storing the actual output of the htcondor call
        :param logger: logger object
        """
        self.proxy = proxy
        self.pickleOut = pickleOut
        self.outputObj = outputObj
        self.timedout = False
        self.logger = logger

    def __enter__(self):
        self.r, self.w = os.pipe()
        self.rpipe = os.fdopen(self.r, 'r')
        self.wpipe = os.fdopen(self.w, 'w')
        self.pid = os.fork()
        if self.pid == 0 and self.proxy:
            # CRAB case
            htcondor.SecMan().invalidateAllSessions()
            htcondor.param['SEC_CLIENT_AUTHENTICATION_METHODS'] = 'FS,GSI'
            htcondor.param['DELEGATE_FULL_JOB_GSI_CREDENTIALS'] = 'true'
            htcondor.param['DELEGATE_JOB_GSI_CREDENTIALS_LIFETIME'] = '0'
            os.environ['X509_USER_PROXY'] = self.proxy
            self.rpipe.close()
        elif self.pid == 0:
            self.rpipe.close()
        else:
            self.wpipe.close()
        return self.pid, self.rpipe

    def __exit__(self, a, b, c):
        if self.pid == 0:
            if a is None and b is None and c is None:
                if self.pickleOut:
                    oo = OutputObj("OK", self.outputObj)
                    self.wpipe.write(pickle.dumps(oo))
                else:
                    self.wpipe.write("OK")
                self.wpipe.close()
                os._exit(0)
            else:
                tracebackString = str('\n'.join(traceback.format_tb(c)))
                msg = "Trapped exception in AuthenticatedSubprocess.Fork: %s %s %s \n%s" % \
                      (a, b, c, tracebackString)
                if self.pickleOut:
                    oo = OutputObj(msg, self.outputObj)
                    self.wpipe.write(pickle.dumps(oo))
                else:
                    self.wpipe.write(msg)
                self.wpipe.close()
                os._exit(1)
        else:
            timestart = time.time()
            self.timedout = True
            while (time.time() - timestart) < 3600:
                res = os.waitpid(self.pid, os.WNOHANG)
                if res != (0, 0):
                    self.timedout = False
                    break
                time.sleep(0.100)
            if self.timedout:
                self.logger.warning(
                    "Subprocess with PID %s (executed in AuthenticatedSubprocess) timed out. Killing it." % self.pid)
                os.kill(self.pid, signal.SIGTERM)
                # we should probably wait again and send SIGKILL if the kill does not work
