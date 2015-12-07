#!/usr/bin/env python
"""WorkQueue Exceptions"""

class WorkQueueError(Exception):
    """Standard error baseclass"""
    def __init__(self, error):
        Exception.__init__(self, error)
        self.msg = WorkQueueError.__class__.__name__
        self.error = error

    def __str__(self):
        return "%s: %s" % (self.msg, self.error)

class WorkQueueWMSpecError(WorkQueueError):
    """Problem with the spec file"""
    def __init__(self, wmspec, error):
        WorkQueueError.__init__(self, error)
        self.wmspec = wmspec
        if hasattr(self.wmspec, 'name'):
            self.msg = "Invalid WMSpec: '%s'" % self.wmspec.name()
        else:
            self.msg = "Invalid WMSpec:"

class WorkQueueNoWorkError(WorkQueueError):
    """No work for spec"""
    def __init__(self, wmspec, error):
        WorkQueueError.__init__(self, error)
        self.wmspec = wmspec
        if hasattr(self.wmspec, 'name'):
            self.msg = "No work in spec: '%s' Check inputs" % self.wmspec.name()
        else:
            self.msg = "No work in spec: Check inputs"

class WorkQueueNoMatchingElements(WorkQueueError):
    """Didn't find any element"""
    def __init__(self, error):
        WorkQueueError.__init__(self, error)
        self.msg = WorkQueueNoMatchingElements.__class__.__name__
        self.error = error

    def __str__(self):
        return "%s: %s" % (self.msg, self.error)

TERMINAL_EXCEPTIONS = (WorkQueueWMSpecError, WorkQueueNoWorkError)
