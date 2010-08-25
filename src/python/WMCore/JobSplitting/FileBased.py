#!/usr/bin/env python
"""
_EventBased_

Event based splitting algorithm that will chop a fileset into
a set of jobs based on event counts
"""

__revision__ = "$Id: FileBased.py,v 1.11 2009/07/30 18:37:07 sfoulkes Exp $"
__version__  = "$Revision: 1.11 $"

from sets import Set
import logging
import threading

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUID import makeUUID

from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.File    import File

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

        myThread = threading.currentThread()
        
        filesPerJob = int(kwargs.get("files_per_job", 10))
        filesInJob = 0
        jobs = []
        

        baseName = makeUUID()

        self.subscription.loadData()
        
        fileset = self.subscription.availableFiles()


        locationDict = {}
        jobDict      = {}
        fileDict     = {}
        jobGroupList = []


        #Count files

        #Now things get complicated
        #For every file, we want every location, and to somehow associate them
        for file in fileset:
            file.loadData()
            locations = file.getLocations()
            #logging.info(file)
            for location in locations:
                #Find and enter all new locations
                if not location in locationDict.keys():
                    #logging.info('Added a new site %s' %(location))
                    locationDict[location] = []
                    fileDict[location]     = []
                    jobDict[location]      = []
                locationDict[location].append(file["id"])


        #Assign files

        #This attaches file IDs to the location they have with the most other files.
        #It's a stupid sorting algorithm, but it works.
        for availableFile in fileset:
            locations = availableFile.getLocations()
            if len(locations) == 0:
                logging.error("No location for file %s" %(availableFile["id"]))
                continue
            if len(locations) > 1:
                temp_location = None
                for location in locations:
                    if temp_location == None:
                        temp_location = location
                    else:
                        if len(locationDict[location]) > len(locationDict[temp_location]):
                            temp_location = location
                fileDict[temp_location].append(availableFile["id"])
            else:
                fileDict[locations[0]].append(availableFile["id"])




        #Create jobs
        listOfFiles = []
        
        #Once we have the files sorted into a dict, we can then group them into jobs
        for location in fileDict.keys():
            #We skip locations with no jobs assigned to them
            if len(fileDict[location]) == 0:
                continue

            #Setup variables
            filesInJob = 0
            
            for file in fileDict[location]:
                if filesInJob == 0 or filesInJob == filesPerJob:
                    job = jobInstance(name = "%s-%s-%s" % (baseName, location, len(jobDict[location]) + 1))
                    job["location"] = location
                    jobDict[location].append(job)
                    filesInJob = 0

                    
                filesInJob += 1
                job.addFile(availableFile)
                listOfFiles.append(File(id = file))
                

            logging.info('I have %i jobs for location %s' %(len(jobDict[location]), location))

            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(jobDict[location])
            #This depends on the newer WMBS implementation of jobGroup

            jobGroup.commit()
            #jobGroup.create()
            jobGroup.setSite(location)
            logging.info('I have committed a jobGroup with id %i' %(jobGroup.id))


            #Add the new jobGroup to the list
            jobGroupList.append(jobGroup)
            

        #if len(jobGroupList) == 0:
        #    return []


        #We need here to acquire all the files we have assigned to jobs
        self.subscription.acquireFiles(files = listOfFiles)

        return jobGroupList
