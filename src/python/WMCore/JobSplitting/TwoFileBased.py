#!/usr/bin/env python
"""
_TwoFileBased_

File based job splitting for two file read workflows.  This works the same as
normal file based splitting except that the input files will also have their
parentage information loaded so that the parents can be included in the job.
"""

__revision__ = "$Id: TwoFileBased.py,v 1.2 2009/07/22 16:24:54 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

class TwoFileBased(JobFactory):
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
            availableFile.loadData(parentage = 1)
            if filesInJob == 0 or filesInJob == filesPerJob:
                job = jobInstance(name = "%s-%s" % (baseName, len(jobs) + 1))
                jobs.add(job)
                filesInJob = 0

            filesInJob += 1
            self.subscription.acquireFiles(availableFile)
            job.addFile(availableFile)

        if len(jobs) == 0:
            return []

        jobGroup = groupInstance(subscription = self.subscription)
        jobGroup.add(jobs)
        jobGroup.commit()

        return [jobGroup]
