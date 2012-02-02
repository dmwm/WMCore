"""
_ProcessPoolTestWorker_

"""

from WMCore.ProcessPool.ProcessPool import ProcessPoolWorker

class ProcessPoolTestWorker(ProcessPoolWorker):

    def __call__(self, input):
        """
        __call__

        """

        return input
