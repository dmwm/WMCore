#!/usr/bin/env python
"""
_DQMUpload_

Diagnostic implementation for a job DQMUpload


"""

from builtins import range
import os

from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler


class Exit70318(DiagnosticHandler):
    def __call__(self, errCode, executor, **args):
        msg = "Failed to upload a DQM file to the GUI server."
        executor.report.addError(executor.stepName, 70318, "DQMUploadFailure", msg)


class DUExceptionHandler(DiagnosticHandler):
    """
    _DUExceptionHandler_

    Generic handler for the DQMUpload step

    I have no idea what this should do

    """
    def __call__(self, errCode, executor, **args):
        """
        _operator()_

        Twiddle thumbs, contemplate navel, toss coin

        """
        jobRepXml = os.path.join(executor.step.builder.workingDir,
                                 executor.step.output.jobReport)

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
            msg = "Job Report contains no error report, but DQMUpload exited non-zero: %s" % errCode
            executor.report.addError(executor.stepName, 50116, "MissingErrorReport", msg)
        else:
            #check exit code in report is non zero
            if executor.report.report.status == 0:
                msg = "Job Report contains no error report, but DQMUpload exited non-zero: %s" % errCode
                executor.report.addError(executor.stepName, 50116, "MissingErrorReport", msg)

        return

class DQMUpload(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)
        self.handlers[70318] = Exit70318()

        catchAll = DUExceptionHandler()
        dummyOut = [self.handlers.__setitem__(x, catchAll) for x in range(0, 255) if x not in self.handlers]
