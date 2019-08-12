#!/usr/bin/env python
"""
_LogArchive_

Diagnostic implementation for a job's LogArchive step
"""

import os

from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler

class LAExceptionHandler(DiagnosticHandler):
    """
    _LAExceptionHandler_

    Generic handler for the LogArchive step

    I have no idea what this should do

    """
    def __call__(self, errCode, executor, **args):
        """
        _operator()_

        Twiddle thumbs, contemplate navel, toss coin

        """
        msg         = "Error in LogArchive: %s\n" % (errCode)
        description = "Misc. LogArchive error: %s\n" % (errCode)
        if args.get('ex', False):
            msg += str(args.get('ex'))
        if args.get('ExceptionInstance', False):
            msg += str(args.get('ExceptionInstance'))

        jobRepXml = os.path.join(executor.step.builder.workingDir,
                                 getattr(executor.step.output, "jobReport", ""))

        if not os.path.exists(jobRepXml):
            # no report => Error
            msg = "No Job Report Found: %s" % jobRepXml
            executor.report.addError(executor.stepName, 50115, "MissingJobReport", msg)
            return

        # job report XML exists, load the exception information from it
        self.parse(executor, jobRepXml)

        # make sure the report has the error in it
        errSection = getattr(executor.report.report, "errors", None)
        if errSection == None:
            msg = "Job Report contains no error report, but LogArchiveManager exited non-zero: %s" % errCode
            executor.report.addError(executor.stepName,
                                     errCode, description, msg)
            executor.report.addError(executor.stepName, 50116, "MissingErrorReport", msg)
            return

        else:
            #check exit code in report is non zero
            if executor.report.report.status == 0:
                msg = "Job Report contains no error report, but LogArchiveManager exited non-zero: %s" % errCode
                executor.report.addError(executor.stepName, 50116, "MissingErrorReport", msg)
            executor.report.addError(executor.stepName,
                                     errCode, description, msg)
        return

class LogArchive(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)

        # Setup a default handler
        catchAll            = LAExceptionHandler()
        self.defaultHandler = catchAll
        for x in range(0, 255):
            if x not in self.handlers:
                self.handlers.__setitem__(x, catchAll)
