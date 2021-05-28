#!/usr/bin/env python
#pylint: disable=E1103
# E1103:  Attach objects to threading
"""
_TwoFileBased_

File based job splitting for two file read workflows.  This works the same as
normal file based splitting except that the input files will also have their
parentage information loaded so that the parents can be included in the job.
"""




import logging
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DAOFactory              import DAOFactory

from WMCore.WMBS.File  import File

class TwoFileBased(JobFactory):
    """
    Two file read workflow splitting

    """


    def __init__(self, package='WMCore.DataStructs',
                 subscription=None,
                 generators=[],
                 limit = None):
        """
        __init__

        Create the DAOs
        """

        myThread = threading.currentThread()

        JobFactory.__init__(self, package = 'WMCore.WMBS',
                            subscription = subscription,
                            generators = generators,
                            limit = limit)


        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        self.getParentInfoAction     = self.daoFactory(classname = "Files.GetParentAndGrandParentInfo")



        return



    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of "files_per_job".  If the "files_per_job" parameters is not
        passed in jobs will process a maximum of 10 files.
        """
        filesPerJob = int(kwargs.get("files_per_job", 10))
        jobsPerGroup = int(kwargs.get("jobs_per_group", 0))
        filesInJob   = 0
        totalJobs    = 0
        listOfFiles  = []

        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict:
            #Now we have all the files in a certain location
            fileList    = locationDict[location]
            filesInJob  = 0
            jobsInGroup = 0
            self.newGroup()
            if len(fileList) == 0:
                #No files for this location
                #This isn't supposed to happen, but better safe then sorry
                logging.debug("Have location %s with no files" % (location))
                continue
            for file in fileList:
                parentLFNs = self.findParent(lfn = file['lfn'])
                for lfn in parentLFNs:
                    parent = File(lfn = lfn)
                    file['parents'].add(parent)
                if filesInJob == 0 or filesInJob == filesPerJob:
                    if jobsPerGroup:
                        if jobsInGroup > jobsPerGroup:
                            self.newGroup()
                            jobsInGroup = 0

                    self.newJob(name = self.getJobName(length=totalJobs))

                    filesInJob   = 0
                    totalJobs   += 1
                    jobsInGroup += 1

                filesInJob += 1
                self.currentJob.addFile(file)

                listOfFiles.append(file)




        return




    def findParent(self, lfn):
        """
        _findParent_

        Find the parents for a file based on its lfn
        """


        parentsInfo = self.getParentInfoAction.execute([lfn])
        newParents = set()
        for parentInfo in parentsInfo:

            # This will catch straight to merge files that do not have redneck
            # parents.  We will mark the straight to merge file from the job
            # as a child of the merged parent.
            if int(parentInfo["merged"]) == 1:
                newParents.add(parentInfo["lfn"])

            elif parentInfo['gpmerged'] == None:
                continue

            # Handle the files that result from merge jobs that aren't redneck
            # children.  We have to setup parentage and then check on whether or
            # not this file has any redneck children and update their parentage
            # information.
            elif int(parentInfo["gpmerged"]) == 1:
                newParents.add(parentInfo["gplfn"])

            # If that didn't work, we've reached the great-grandparents
            # And we have to work via recursion
            else:
                parentSet = self.findParent(lfn = parentInfo['gplfn'])
                for parent in parentSet:
                    newParents.add(parent)

        return newParents
