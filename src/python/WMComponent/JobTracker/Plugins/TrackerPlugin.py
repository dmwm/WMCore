#!/bin/env python

#Basic model fot a tracker plugin
#Should be overridden by more advanced models

__revision__ = "$Id: TrackerPlugin.py,v 1.1 2009/10/02 21:30:58 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


class TrackerPlugin:


    def __init__(self, config):
        """Overwrite this"""

        return


    def __call__(self, jobDict):
        """Overwrite this"""

        return



    def kill(self, killList):
        """Overwrite this"""

        return


    def track(self, jobDict):
        """Overwrite this"""

        trackDict = {}
        #This should return info regarding the jobs.
        #trackDict should be a dictionary indexed by job name
        #Each value should also be a dictionary, containing the job status, the time
        #since it entered that status, and the reason
        #jobInfo = {'Status': string, 'StatusTime': int since entering status, 'StatusReason': string}
        #See TestTracker for an idea

        return trackDict
