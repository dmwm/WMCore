#!/usr/bin/env python
"""
_MergeBySize_

"Splitting" algorithm to combine a fileset into merge job definitions
based on the size of the files

"""




from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUIDLib import makeUUID
from WMCore.DataStructs.Fileset import Fileset



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

        mergeSize = int(kwargs['merge_size'])
        overflow  = bool(kwargs.get('all_files', False))
        fileset.sort()

        accumSize = 0
        jobFiles = Fileset()
        locationDict = self.sortByLocation()
        for location in locationDict:
            baseName = makeUUID()
            self.newGroup()
            for f in locationDict[location]:
                accumSize += f['size']
                jobFiles.addFile(f)
                if accumSize >= mergeSize:
                    self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.jobs) + 1),
                                      files = jobFiles)
                    self.currentJob["mask"].setMaxAndSkipEvents(-1, 0)
                    accumSize = 0
                    jobFiles = Fileset()

            if len(jobFiles) > 0:
                if overflow:
                    self.newJob(name = '%s-%s' % (baseName, len(self.currentGroup.jobs) + 1),
                                      files = jobFiles)
                    self.currentJob["mask"].setMaxAndSkipEvents(-1, 0)
