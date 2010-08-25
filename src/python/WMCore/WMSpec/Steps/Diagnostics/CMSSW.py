#!/usr/bin/env python
"""
_CMSSW_

Diagnostic implementation for a CMSSW job


"""

import os
from WMCore.WMSpec.Steps.Diagnostic import Diagnostic, DiagnosticHandler

class Exit127(DiagnosticHandler):
    def __call__(self, errCode, executor, **args):
        msg = "Executable Not Found"
        executor.report.addError(50110, "ExecutableNotFound", msg)
        
class Exit126(DiagnosticHandler):
    def __call__(self, errCode, executor, **args):
        msg = "Executable permissions not executable"
        executor.report.addError(50111, "ExecutableBadPermissions", msg)


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
        print "%s Diagnostic Handler invoked" % self.__class__.__name__
        msg = "Exit %s: %s Exception from cmsRun" % (self.code, self.desc)
        jobRepXml = os.path.join(executor.step.builder.workingDir,
                                 executor.step.output.jobReport)
        
        if os.path.exists(jobRepXml):
            # job report XML exists, load the exception information from it
            executor.report.parse(jobRepXml)

        # make sure the report has the error in it
        errSection = getattr(executor.report.report, "errors", None)
        if errSection == None:
            executor.report.addError(self.code, self.desc, msg)
        else:
            if not hasattr(errSection, self.desc):
                executor.report.addError(self.code, self.desc, msg)

        print executor.report.report.errors
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

        if not os.path.exists(jobRepXml):
            # no report => Error
            msg = "No Job Report Found: %s" % jobRepXml
            executor.report.addError(50115, "MissingJobReport", msg)
            return
        
        # job report XML exists, load the exception information from it
        executor.report.parse(jobRepXml)
        
        
        # make sure the report has the error in it
        errSection = getattr(executor.report.report, "errors", None)
        if errSection == None:
            msg = "Job Report contains no error report, but cmsRun exited non-zero: %s" % errCode
            executor.report.addError(50116, "MissingErrorReport", msg)
            return

        else:
            #check exit code in report is non zero
            if executor.report.report.status == 0:
                msg = "Job Report contains no error report, but cmsRun exited non-zero: %s" % errCode
                executor.report.addError(50116, "MissingErrorReport", msg)
        return


    
class CMSSW(Diagnostic):

    def __init__(self):
        Diagnostic.__init__(self)
        self.handlers[126] = Exit126()
        self.handlers[127] = Exit127()
        self.handlers[65]  = CMSRunHandler(8001, "CMSExeption")
        self.handlers[66]  = CMSRunHandler(8002, "StdExeption")
        self.handlers[67]  = CMSRunHandler(8003, "UnknownExeption")
        self.handlers[68]  = CMSRunHandler(8004, "StdBadAlloc")
        self.handlers[88]  = CMSRunHandler(7000, "CommandLineProcessing")
        self.handlers[89]  = CMSRunHandler(7001, "ConfigFileNotFound")
        self.handlers[90]  = CMSRunHandler(7002, "ConfigFileReadError")

        
        # for all the exception codes between 1 and 225, use a default that attempts to read the code
        # from the job report
        catchAll = EDMExceptionHandler()
        [ self.handlers.__setitem__(x, catchAll) for x in range(0, 255) if not self.handlers.has_key(x) ]

        
