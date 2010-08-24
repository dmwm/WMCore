#!/usr/bin/env python
"""
BossLite exceptions
"""




import inspect

class BossLiteError(Exception):
    """
    errors base class
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(BossLiteError, self).__init__(value)
        Exception.__init__( self  )
        self.value = self.__class__.__name__
        self.msg = str(value)
        self.data = {}
        # take more information if it applies
        try:
            stack = inspect.trace(1)[-1]
            self.data = { 'FileName' : stack[1],
                          'LineNumber' : stack[2],
                          'MethodName' : stack[3],
                          'LineContent' : stack[4] }
        except Exception:
            pass


    def __str__(self):
        """
        __str__
        """

        return repr(self.msg)

    def message(self):
        """
        error description
        """

        return self.msg
    

class JobError(BossLiteError):
    """
    errors with jobs
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(JobError, self).__init__(value)
        BossLiteError.__init__( self, value )


class TaskError(BossLiteError):
    """
    errors with tasks
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(TaskError, self).__init__(value)
        BossLiteError.__init__( self, value )


class DbError(BossLiteError):
    """
    MySQL, SQLite and possible other exceptions errors are redirected to
    this exception type
    """

    def __init__(self, value):
        """
        __init__
        """
        
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(DbError, self).__init__(value)
        BossLiteError.__init__( self, value )

class SchedulerError(BossLiteError):
    """
    scheduler errors
    """

    def __init__(self, value, msg='', command=None):
        """
        __init__
        """

        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(SchedulerError, self).__init__(value)
        BossLiteError.__init__(self, value)
        self.value = str(value)
        self.msg = str(msg)
        self.data['Command'] = str(command)

    def __str__(self):
        """
        __str__
        """

        return self.value + '\n' + self.msg

    def description(self):
        """
        returns a short description of the exception
        """

        return self.value

    def errorDump(self):
        """
        returns the original error message 
        """

        return self.msg


class TimeOut(BossLiteError):
    """
    operation timed out
    """

    def __init__(self, command, partialOut, value, start=None, stop=None):
        """
        __init__
        """

        self.timeout = value
        self.start = start
        self.stop = stop
        self.value = \
              "Command Timed Out after %d seconds, issued at %d, ended at %d" \
              % (self.timeout, self.start, self.stop )
        # // the stupid python does not follow its rules:
        # // Exception does not inherit from object, no way to call super
        # super(TimeOut, self).__init__(self.__str__())
        BossLiteError.__init__(self, self.value)
        self.data['Command'] = command
        self.data['partialOutput'] = partialOut

    def commandOutput( self ) :
        """
        returns the partial output recorded before timeout
        """

        return self.data['partialOutput']









