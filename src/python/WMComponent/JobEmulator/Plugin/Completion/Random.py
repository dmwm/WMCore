#!/usr/bin/env python
"""
_RandomCompletionPlugin_

Plugin for the Job Emulator to successfully complete
a certain percentage of jobs after a certain amount
of time.

"""
import datetime
from random import random

class Random:
    """
    _Random_

    Plugin for the Job Emulator to successfully complete
    a certain percentage of jobs after a certain amount
    of time.
    """

    def processJob(self, jobInfo, jobRunningLocation=None):
        """
        _processJobs_

        Determine if a job's status should change, and return
        the new status.

        The jobInfo parameter is a four element long list with
        the following items:
          jobInfo[0]: Job ID
          jobInfo[1]: Job type: processing, merge, cleanup,...
          jobInfo[2]: Start time
          jobInfo[3]: Job status: new, finished, failed

        This method assumes that the self.avgCompletionTime and
        self.avgSuccessRate variables have been set.

        """
        if self.avgCompletionTime == "00:00:00":
            if random() < float(self.avgSuccessRate):
                return "finished"
            else:
                return "failed"

        
        # We start completing/failing jobs once a job has been in the
        # queue for 85% of its specified interval.
        completionHours, completionMinutes, completionSeconds = \
                       self.avgCompletionTime.split(":")
        completionInterval = int((int(completionHours) * 3600 + int(completionMinutes) \
                             * 60 + int(completionSeconds)) * 0.85)
        interval = datetime.timedelta(seconds=completionInterval)
        if (datetime.datetime.now() - jobInfo[2]) < interval:
            return jobInfo[3]

        done = random()
        # We only complete/fail jobs 30% of the time.
        if done < 0.7:
            return jobInfo[3]
        if random() < float(self.avgSuccessRate):
            return "finished"
        else:
            return "failed"


