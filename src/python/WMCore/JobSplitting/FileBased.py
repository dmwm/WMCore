#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: FileBased.py,v 1.8 2009/02/19 19:53:04 sfoulkes Exp $"
__version__  = "$Revision: 1.8 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class FileBased(JobFactory):
    """
    Split jobs by number of files.
    """
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of "files_per_job".  If the "files_per_job" parameters is not
        passed in jobs will process a maximum of 10 files.
        """
        filesPerJob = kwargs.get("files_per_job", 10)
        filesInJob = 0
        jobs = Set()

        baseName = makeUUID()

        for availableFile in self.subscription.availableFiles():
            if filesInJob == 0 or filesInJob == filesPerJob:
                job = jobInstance(name = "%s-%s" % (baseName, len(jobs) + 1))
                jobs.add(job)
                filesInJob = 0

            filesInJob += 1
            job.addFile(availableFile)

        jobGroup = groupInstance(subscription = self.subscription)
        jobGroup.add(jobs)
        jobGroup.commit()
        jobGroup.recordAcquire(list(jobs))

        return [jobGroup]
