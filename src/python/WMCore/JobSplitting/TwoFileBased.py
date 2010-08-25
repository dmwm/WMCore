#!/usr/bin/env python
"""
_TwoFileBased_

File based job splitting for two file read workflows.  This works the same as
normal file based splitting except that the input files will also have their
parentage information loaded so that the parents can be included in the job.
"""

__revision__ = "$Id: TwoFileBased.py,v 1.5 2009/08/03 19:48:14 sfoulkes Exp $"
__version__  = "$Revision: 1.5 $"

import logging
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
        filesPerJob = int(kwargs.get("files_per_job", 10))
        filesInJob = 0
        jobGroups = []
        jobGroup = None

        baseName = makeUUID()

        for availableFile in self.subscription.availableFiles():
            availableFile.loadData(parentage = 1)
            if filesInJob == 0 or filesInJob == filesPerJob:
                if jobGroup:
                    jobGroup.commit()
                jobGroup = groupInstance(subscription = self.subscription)
                jobGroups.append(jobGroup)
                job = jobInstance(name = "%s-%s" % (baseName, len(jobGroups) + 1))
                jobGroup.add(job)
                filesInJob = 0

            filesInJob += 1
            self.subscription.acquireFiles(availableFile)
            job.addFile(availableFile)

        if jobGroup:
            jobGroup.commit()

        return jobGroups
