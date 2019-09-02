#!/usr/bin/env python
"""
_AlcaHarvest_

Diagnostic implementation for a AlcaHarvest job


"""

import os

from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler


class Exit60319(DiagnosticHandler):
    def __call__(self, errCode, executor, **args):
        msg = "Failed to copy AlcaHarvest condition file to target directory."
        executor.report.addError(executor.stepName,
                                 60319, "AlcaHarvestFailure", msg)


class AHExceptionHandler(DiagnosticHandler):
    """
    _AHExceptionHandler_

    Generic handler for the AlcaHarvest step

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
            msg = "Job Report contains no error report, but AlcaHarvest exited non-zero: %s" % errCode
            executor.report.addError(executor.stepName, 50116, "MissingErrorReport", msg)
        else:
            #check exit code in report is non zero
            if executor.report.report.status == 0:
                msg = "Job Report contains no error report, but AlcaHarvest exited non-zero: %s" % errCode
                executor.report.addError(executor.stepName, 50116, "MissingErrorReport", msg)
        return

class AlcaHarvest(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)
        self.handlers[60319] = Exit60319()

        catchAll = AHExceptionHandler()
        for x in range(0, 255):
            if x not in self.handlers:
                self.handlers.__setitem__(x, catchAll)
