"""LogDB Exceptions"""

class LogDBError(Exception):
    """Standard error baseclass"""
    def __init__(self, error):
        Exception.__init__(self, error)
        self.msg = LogDBError.__class__.__name__
        self.error = error

    def __str__(self):
        return "%s: %s" % (self.msg, self.error)
