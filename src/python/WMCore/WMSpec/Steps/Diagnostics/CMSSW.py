#!/usr/bin/env python
# pylint: disable=W0212
# W0212: Names are not accessible for WMTask objects, etc.
"""
_CMSSW_

Diagnostic implementation for a CMSSW job


"""
from __future__ import print_function

from builtins import range, str

import logging
import os.path
import socket

from Utils import FileTools
from WMCore.FwkJobReport.Report import FwkJobReportException
from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler

# strip the lines from the log for the report
DEFAULT_TAIL_LINES_FROM_LOG = 25


class Exit127(DiagnosticHandler):
    """
    Handle non-existant executable

    """

    def __call__(self, errCode, executor, **args):
        msg = "Executable Not Found at host: %s" % socket.getfqdn()
        if args.get('ExceptionInstance', False):
            msg += str(args.get('ExceptionInstance'))
        executor.report.addError(executor.stepName,
                                 50110, "ExecutableNotFound", msg)


class Exit126(DiagnosticHandler):
    """
    Handle bad permissions

    """

    def __call__(self, errCode, executor, **args):
        msg = "Executable permissions not executable at host: %s" % socket.getfqdn()
        if args.get('ExceptionInstance', False):
            msg += str(args.get('ExceptionInstance'))
        executor.report.addError(executor.stepName,
                                 50111, "ExecutableBadPermissions", msg)


class Exit50513(DiagnosticHandler):
    """
    Handle SCRAM script failure

    """

    def __call__(self, errCode, executor, **args):
        """
        Added for Steve to handle SCRAM script failure

        Must fail job (since SCRAM didn't run)

        """
        msg = "SCRAM scripts failed to run!\n"
        if args.get('ExceptionInstance', False):
            msg += str(args.get('ExceptionInstance'))

        executor.report.addError(executor.stepName,
                                 50513, "SCRAMScriptFailure", msg)

        # Then mark the job as failed
        if executor.report.report.status == 0:
            executor.report.report.status = 1


class CMSDefaultHandler(DiagnosticHandler):
    """
    _CMSDefaultHandler_

    Default handler for miscellaneous CMSSW errors.
    """

    def __call__(self, errCode, executor, **args):
        logging.critical("%s Diagnostic Handler invoked", self.__class__.__name__)
        msg = "Error in CMSSW: %s\n" % (errCode)
        jobRepXml = os.path.join(executor.step.builder.workingDir,
                                 executor.step.output.jobReport)

        excepInst = args.get('ExceptionInstance', None)

        description = "Misc. CMSSW error"
        if excepInst:
            if hasattr(excepInst, 'detail'):
                description = excepInst.detail
            msg += str(excepInst)

        if os.path.exists(jobRepXml):
            # job report XML exists, load the exception information from it
            try:
                self.parse(executor, jobRepXml)
            except FwkJobReportException:    
                # Job report is bad, the parse already puts a 50115 in the file    
                pass

            reportStep = executor.report.retrieveStep(executor.stepName)
            reportStep.status = errCode

        # Grab stderr log from CMSSW
        errLog = os.path.join(os.path.dirname(jobRepXml),
                              '%s-stderr.log' % (executor.stepName))
        outLog = os.path.join(os.path.dirname(jobRepXml),
                              '%s-stdout.log' % (executor.stepName))

        if os.path.exists(errLog):
            logTail = FileTools.tail(errLog, DEFAULT_TAIL_LINES_FROM_LOG)
            msg += '\n Adding last %s lines of CMSSW stderr:\n' % DEFAULT_TAIL_LINES_FROM_LOG
            msg += logTail
        if os.path.exists(outLog):
            logTail = FileTools.tail(outLog, DEFAULT_TAIL_LINES_FROM_LOG)
            msg += '\n Adding last %s lines of CMSSW stdout:\n' % DEFAULT_TAIL_LINES_FROM_LOG
            msg += logTail

        executor.report.addError(executor.stepName,
                                 errCode, description, msg)

        return


class CMSRunHandler(DiagnosticHandler):
    """
    _CMSRunHandler_

    Base util for a cmsRun bork, check job report exists and contains appropriate error

    """

    def __init__(self, code, desc):
        DiagnosticHandler.__init__(self)
        self.code = code
        self.desc = desc

    def __call__(self, errCode, executor, **args):
        print("%s Diagnostic Handler invoked" % self.__class__.__name__)
        msg = "Exit %s: %s Exception from cmsRun" % (self.code, self.desc)
        jobRepXml = os.path.join(executor.step.builder.workingDir,
                                 executor.step.output.jobReport)

        if os.path.exists(jobRepXml):
            # job report XML exists, load the exception information from it
            try:
                self.parse(executor, jobRepXml)
            except FwkJobReportException:    
                # Job report is bad, the parse already puts a 50115 in the file    
                pass

            reportStep = executor.report.retrieveStep(executor.stepName)
            reportStep.status = self.code

        errLog = os.path.join(os.path.dirname(jobRepXml),
                              '%s-stderr.log' % (executor.stepName))
        outLog = os.path.join(os.path.dirname(jobRepXml),
                              '%s-stdout.log' % (executor.stepName))

        if os.path.exists(errLog):
            logTail = FileTools.tail(errLog, DEFAULT_TAIL_LINES_FROM_LOG)
            msg += '\n Adding last %s lines of CMSSW stderr:\n' % DEFAULT_TAIL_LINES_FROM_LOG
            msg += logTail
        if os.path.exists(outLog):
            logTail = FileTools.tail(outLog, DEFAULT_TAIL_LINES_FROM_LOG)
            msg += '\n Adding last %s lines of CMSSW stdout:\n' % DEFAULT_TAIL_LINES_FROM_LOG
            msg += logTail

        # make sure the report has the error in it
        errSection = getattr(executor.report.report, "errors", None)
        if errSection is None:
            executor.report.addError(executor.stepName,
                                     self.code, self.desc, msg)
        else:
            if not hasattr(errSection, self.desc):
                executor.report.addError(executor.stepName,
                                         self.code, self.desc, msg)

        print(executor.report.report.errors)
        return


class EDMExceptionHandler(DiagnosticHandler):
    """
    _EDMExceptionHandler_

    Handle unspecified non-zero exit code by checking for a job report file containing an
    EDM error code and report that.

    If the job report isnt there, thats a specific failure

    """

    def __call__(self, errCode, executor, **args):
        """
        _operator()_

        Look for the XML job report, try and read it and extract the error information from it

        """
        jobRepXml = os.path.join(executor.step.builder.workingDir,
                                 executor.step.output.jobReport)

        errLog = os.path.join(os.path.dirname(jobRepXml),
                              '%s-stderr.log' % (executor.stepName))
        outLog = os.path.join(os.path.dirname(jobRepXml),
                              '%s-stdout.log' % (executor.stepName))

        addOn = '\n'
        if os.path.exists(errLog):
            logTail = FileTools.tail(errLog, 10)
            addOn += '\nAdding last ten lines of CMSSW stderr:\n'
            addOn += logTail
        else:
            logging.error("No stderr from CMSSW")
            logging.error(os.listdir(os.path.basename(jobRepXml)))

        if os.path.exists(outLog):
            logTail = FileTools.tail(outLog, DEFAULT_TAIL_LINES_FROM_LOG)
            msg = '\n Adding last %s lines of CMSSW stdout:\n' % DEFAULT_TAIL_LINES_FROM_LOG
            msg += logTail

        # Add the error we were sent
        ex = args.get('ExceptionInstance', None)
        executor.report.addError(executor.stepName,
                                 errCode, "CMSSWStepFailure", msg + str(ex))

        if not os.path.exists(jobRepXml):
            # no report => Error
            msg = "No Job Report Found: %s" % jobRepXml
            executor.report.addError(executor.stepName,
                                     50115, "MissingJobReport", msg)
            return

        # job report XML exists, load the exception information from it
        try:
            self.parse(executor, jobRepXml)
        except FwkJobReportException:    
            # Job report is bad, the parse already puts a 50115 in the file    
            pass

        # make sure the report has the error in it
        errSection = getattr(executor.report.report, "errors", None)
        if errSection is None:
            msg = "Job Report contains no error report, but cmsRun exited non-zero: %s" % errCode
            msg += addOn
            executor.report.addError(executor.stepName,
                                     50116, "MissingErrorReport", msg)
            return

        else:
            # check exit code in report is non zero
            if executor.report.report.status == 0:
                msg = "Job Report contains no error report, but cmsRun exited non-zero: %s" % errCode
                msg += addOn
                executor.report.addError(executor.stepName,
                                         50116, "MissingErrorReport", msg)

            else:
                msg = "Adding extra error in order to hold error report"
                msg += addOn
                executor.report.addError(executor.stepName,
                                         99999, "ErrorLoggingAddition", msg)
        return


class CMSSW(Diagnostic):
    """
    Master diagnostic class

    Sets the default handler.
    """

    def __init__(self):
        Diagnostic.__init__(self)
        self.handlers[50513] = Exit50513()
        self.handlers[126] = Exit126()
        self.handlers[127] = Exit127()
        self.handlers[65] = CMSRunHandler(8001, "CMSException")
        self.handlers[66] = CMSRunHandler(8002, "StdException")
        self.handlers[67] = CMSRunHandler(8003, "UnknownException")
        self.handlers[68] = CMSRunHandler(8004, "StdBadAlloc")
        self.handlers[88] = CMSRunHandler(7000, "CommandLineProcessing")
        self.handlers[89] = CMSRunHandler(7001, "ConfigFileNotFound")
        self.handlers[90] = CMSRunHandler(7002, "ConfigFileReadError")

        self.defaultHandler = CMSDefaultHandler()

        # for all the exception codes between 1 and 225, use a default that attempts to read the code
        # from the job report
        catchAll = EDMExceptionHandler()
        for x in range(0, 255):
            if x not in self.handlers:
                self.handlers.__setitem__(x, catchAll)
