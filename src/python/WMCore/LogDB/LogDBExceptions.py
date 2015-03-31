"""LogDB Exceptions"""

class LogDBError(StandardError):
    """Standard error baseclass"""
    def __init__(self, error):
        StandardError.__init__(self, error)
        self.msg = LogDBError.__class__.__name__
        self.error = error

    def __str__(self):
        return "%s: %s" % (self.msg, self.error)
