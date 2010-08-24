from WMCore.WMBS.Job import Job

class ComplexJob(Job):
    """
    Base class for jobs not split along file lines. These jobs
    must know the number of events to process, the first event
    the number of lumi's to process, and the runs to process.
    """
    def __init__(self, subscription=None, numberfiles=1, id = -1, logger=None, dbfactory = None):
        pass