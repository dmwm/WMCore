"""
 * ApMon - Application Monitoring Tool
 * Version: 2.2.20
 *
 * Copyright (C) 2006 California Institute of Technology
 *
 * Permission is hereby granted, free of charge, to use, copy and modify
 * this software and its documentation (the "Software") for any
 * purpose, provided that existing copyright notices are retained in
 * all copies and that this notice is included verbatim in any distributions
 * or substantial portions of the Software.
 * This software is a part of the MonALISA framework (http://monalisa.cacr.caltech.edu).
 * Users of the Software are asked to feed back problems, benefits,
 * and/or suggestions about the software to the MonALISA Development Team
 * (developers@monalisa.cern.ch). Support for this software - fixing of bugs,
 * incorporation of new features - is done on a best effort basis. All bug
 * fixes and enhancements will be made available under the same terms and
 * conditions as the original software,

 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY DERIVATIVES THEREOF,
 * EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. THIS SOFTWARE IS
 * PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE NO
 * OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 * MODIFICATIONS.
"""
from __future__ import print_function
import time
import threading
import traceback


# Simple logging class
class Logger(object):
    """ Logging main class """

    FATAL = 0		# When something very bad happened and we should quit
    ERROR = 1		# Tipically when something important fails
    WARNING = 2		# Intermediate logging level.
    INFO = 3		# Intermediate logging level.
    NOTICE = 4		# Logging level with detailed information.
    DEBUG = 5		# Logging level for debugging

    LEVELS = ['FATAL', 'ERROR', 'WARNING', 'INFO', 'NOTICE', 'DEBUG']

    # Constructor
    def __init__(self, defaultLevel=INFO):
        self.logLock = threading.Lock()
        self.logLevel = defaultLevel


    def log(self, level, message, printex=False):
        """Print the given message if the level is more serious as the existing one"""
        self.logLock.acquire()
        if level <= self.logLevel:
            print(time.asctime() + ": ApMon["+Logger.LEVELS[level]+"]: "+message)
            if printex:
                traceback.print_exc()
        self.logLock.release()


    def setLogLevel(self, strLevel):
        """ Set the logging level """
        self.logLock.acquire()
        for lIdx in range(len(Logger.LEVELS)):
            if strLevel == Logger.LEVELS[lIdx]:
                self.logLevel = lIdx
        self.logLock.release()

