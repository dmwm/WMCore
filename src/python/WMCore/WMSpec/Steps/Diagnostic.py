#!/usr/bin/env python
"""
_Diagnostic_


Interface and base class for step specific diagnostic handlers


"""

from WMCore.FwkJobReport.Report import FwkJobReportException


class DiagnosticHandler(object):
    """
    _DiagnosticHandler_

    Interface definition for handlers for a specific error condition


    """
    def __call__(self, errorCode, executorInstance, **args):
        """
        _operator(errCode, executor)_

        Override to act on a particular error, use the executorInstance
        to access things like the step, logfiles, and report.

        Args will be used to provide extra information such as Exception
        instances etc

        """
        msg = "DiagnosticHandler.__call__ not "
        msg += "implemented for class %s" % self.__class__.__name__
        raise NotImplementedError(msg)

    def parse(self, executorInstance, jobRepXml):
        """
        Add an error to report if parsing the xml fails.
        """
        try:
            executorInstance.report.parse(jobRepXml, executorInstance.stepName)
        except FwkJobReportException as ex:
            # Job report is bad, the parse already puts a 50115 in the file
            msg = "Error reading XML job report file, possibly corrupt XML File:\n"
            msg += "Details: %s" % str(ex)
            executorInstance.report.addError(executorInstance.stepName, 50115, "BadFWJRXML", msg)
            raise


class DefaultDiagnosticHandler(DiagnosticHandler):
    """
    _DefaultDiagnosticHandler_

    Catch-all that just adds information to the report

    """
    def __call__(self, errorCode, executorInstance, **args):
        pass





class Diagnostic(object):
    """
    _Diagnostic_

    Base class for a Diagnostic implementation specific to a step type
    Also works as a bare minimum Diagnostic if overriding is not needed

    """
    def __init__(self):
        self.handlers = {}
        self.defaultHandler = DefaultDiagnosticHandler()



    def __call__(self, errCode, executor, **args):
        """
        _operator(errCode, executor, args)_

        Invoke the diagnostic to produce an error report

        """
        handler = self.handlers.get(errCode, self.defaultHandler)
        handler(errCode, executor, **args)
        executor.saveReport()
