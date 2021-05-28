#!/usr/bin/env python




from builtins import object
class FetcherInterface(object):
    """
    _FetcherInterface_

    define standard interface for fetcher implementations.

    Init takes the working directory for the sandbox

    Call invokes the fetcher on a WMTask instance

    """

    def __init__(self):
        self.workingDir = None
        self.cacheDir = None

    def setWorkingDirectory(self, workingDir):
        self.workingDir = workingDir

    def workingDirectory(self):
        return self.workingDir

    def setCacheDirectory(self, cacheDir):
        self.cacheDir = cacheDir

    def cacheDirectory(self):
        return self.cacheDir

    def __call__(self, wmTaskHelper):
        """
        _operator(wmTask)_

        Override to pull infomation from the WMTask, retrieve information and
        then insert it into the sandbox

        """
        msg = "FetcherInterface.__call__(wmTask) not overridden by:\n"
        msg += "%s implementation\n" % self.__class__.__name__
        raise NotImplementedError(msg)
