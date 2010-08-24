#!/usr/bin/env python
"""
_Diagnostic_


Interface and base class for step specific diagnostic handlers


"""




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
        raise NotImplementedError, msg


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






