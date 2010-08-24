#!/usr/bin/env python
"""
_MergeBySize_

"Splitting" algorithm to combine a fileset into merge job definitions
based on the size of the files

"""

__revision__ = "$Id: MergeBySize.py,v 1.3 2008/10/01 22:01:33 metson Exp $"
__version__ = "$Revision: 1.3 $"

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
    def algorithm(self, job_instance = None, jobname=None, *args, **kwargs):
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
                job  = job_instance(name = '%s-%s' % (jobname, len(jobs) +1))
                job.addFile(accumFiles)
                job.mask.setMaxAndSkipEvents(-1, 0)
                jobs.add(job)
                accumSize = 0
                accumFiles = []
                
        if len(accumFiles) > 0:
            if overflow:
                job =  job_instance(name = '%s-%s' % (jobname, len(jobs) +1 ))
                job.addFile(accumFiles)
                job.mask.setMaxAndSkipEvents(-1, 0)
                jobs.add(job)


        return jobs







