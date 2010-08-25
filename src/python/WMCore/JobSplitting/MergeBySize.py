#!/usr/bin/env python
"""
_MergeBySize_

"Splitting" algorithm to combine a fileset into merge job definitions
based on the size of the files

"""

__revision__ = "$Id: MergeBySize.py,v 1.5 2009/09/30 12:30:54 metson Exp $"
__version__ = "$Revision: 1.5 $"

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
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Implement merge algorithm for the subscription provided

        """
        fileset = list(self.subscription.availableFiles())

        mergeSize = kwargs['merge_size']
        overflow  = kwargs.get('all_files', False)
        fileset.sort()

        accumSize = 0
        accumFiles = []
        locationDict = self.sortByLocation()
        for location in locationDict:
            self.newGroup()
            for f in locationDict[location]:
                accumSize += f['size']
                accumFiles.append(f)
                if accumSize >= mergeSize:
                    self.newJob(name = '%s-%s' % (jobName, len(jobs) +1))
                    self.currentJob.addFile(accumFiles)
                    self.currentJob.mask.setMaxAndSkipEvents(-1, 0)
                    accumSize = 0
                    accumFiles = []
    
            if len(accumFiles) > 0:
                if overflow:
                    self.newJob(name = '%s-%s' % (jobName, len(jobs) +1 ))
                    self.currentJob.addFile(accumFiles)
                    self.currentJob.mask.setMaxAndSkipEvents(-1, 0)