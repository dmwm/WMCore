#!/usr/bin/env python
"""
_MergeBySize_

"Splitting" algorithm to combine a fileset into merge job definitions
based on the size of the files

"""

__revision__ = "$Id: MergeBySize.py,v 1.4 2009/02/26 20:06:40 ewv Exp $"
__version__ = "$Revision: 1.4 $"

from sets import Set
from WMCore.JobSplitting.JobFactory import JobFactory



class MergeBySize(JobFactory):
    """
    _MergeBySize_

    Process a fileset to generate merge jobs based on size.
    Files will be combined based on size,  grouped by
    run,and sorted by lumisection

    an option is provided to include all files in overflow jobs


    """
    def algorithm(self, jobInstance = None, jobName=None, *args, **kwargs):
        """
        _algorithm_

        Implement merge algorithm for the subscription provided

        """
        jobs = Set()
        fileset = list(self.subscription.availableFiles())

        mergeSize = kwargs['merge_size']
        overflow  = kwargs.get('all_files', False)
        fileset.sort()

        accumSize = 0
        accumFiles = []

        for f in fileset:
            accumSize += f['size']
            accumFiles.append(f)
            if accumSize >= mergeSize:
                job  = jobInstance(name = '%s-%s' % (jobName, len(jobs) +1))
                job.addFile(accumFiles)
                job.mask.setMaxAndSkipEvents(-1, 0)
                jobs.add(job)
                accumSize = 0
                accumFiles = []

        if len(accumFiles) > 0:
            if overflow:
                job =  jobInstance(name = '%s-%s' % (jobName, len(jobs) +1 ))
                job.addFile(accumFiles)
                job.mask.setMaxAndSkipEvents(-1, 0)
                jobs.add(job)


        return jobs







