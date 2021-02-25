#!/usr/bin/env python
"""
_StageOut_

Diagnostic implementation for a job StageOut


"""

from builtins import range
import os
from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler


class SOMExceptionHandler(DiagnosticHandler):
    """
    _SOMExceptionHandler_

    Generic handler for the StageOutManager

    I have no idea what this should do

    """
    def __call__(self, errCode, executor, **args):
        """
        _operator()_

        Twiddle thumbs, contemplate navel, toss coin

        """
        msg         = "Error in StageOut: %s\n" % (errCode)
        description = "Misc. StageOut error: %s\n" % (errCode)
        if args.get('ex', None):
            msg += str(args.get('ex'))
        if args.get('ExceptionInstance', False):
            msg += str(args.get('ExceptionInstance'))

        #jobRepXml = os.path.join(executor.step.builder.workingDir,
        #                         executor.step.output.jobReport)

        #if not os.path.exists(jobRepXml):
        #    # no report => Error
        #    msg = "No Job Report Found: %s" % jobRepXml
        #    executor.report.addError(50115, "MissingJobReport", msg)
        #    return

        # job report XML exists, load the exception information from it
        #executor.report.parse(jobRepXml)


        # make sure the report has the error in it
        errSection = getattr(executor.report.report, "errors", None)
        if errSection == None:
            localmsg = "Job Report contains no error report, but StageOutManager exited non-zero: %s" % errCode
            executor.report.addError(executor.stepName, 50116, "MissingErrorReport", localmsg)

        else:
            #check exit code in report is non zero
            if executor.report.report.status == 0:
                msg = "Job Report contains no error report, but StageOutManager exited non-zero: %s" % errCode


        executor.report.addError(executor.stepName,
                                 errCode, description, msg)

        return

class StageOut(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)

        # Set defaults
        catchAll            = SOMExceptionHandler()
        self.defaultHandler = catchAll

        [ self.handlers.__setitem__(x, catchAll) for x in range(0, 255) if x not in self.handlers ]
