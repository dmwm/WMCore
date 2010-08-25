#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections
"""

__revision__ = "$Id: LumiBased.py,v 1.6 2009/08/05 19:47:32 mnorman Exp $"
__version__  = "$Revision: 1.6 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUID import makeUUID

class LumiBased(JobFactory):
    """
    Split jobs by number of events
    """

    locations = []

    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        Implement lumi based splitting algorithm.  By default splits into JobGroups by lumi
        section, and then each job has one file.
        """
        jobs = []
        JobGroupList = []


        #  //
        # // get the fileset
        #//
        fileset = self.subscription.availableFiles()

        #lumisPerJob = kwargs['lumis_per_job']
        filesPerJob  = kwargs.get('files_per_job', 1)
        eventsPerJob = kwargs.get('events_per_job', None)
        lumisPerJob  = kwargs.get('lumis_per_job', None)

        #Default behavior
        #Splits into one JobGroup per lumi section
        # and one file per job in jobGroup


        lumiDict = {}
        self.locations = []


        #Get a dictionary of sites, files
        locationDict = self.sortByLocation()

        for location in locationDict.keys():
            for f in locationDict[location]:
                if hasattr(f, "loadData"):
                    f.loadData()

                if sum([ len(run) for run in f['runs']]) == 0:
                    continue
                fileLumi     = None
                fileLumiList = []
                #Let's grab all the Lumi sections in the file
                for run in f['runs']:
                    for i in run:
                        fileLumiList.append(int(str(run.run) + str(i)))

                for fileLumi in fileLumiList:
                    if not lumiDict.has_key(fileLumi):
                        lumiDict[fileLumi] = []
                    lumiDict[fileLumi].append(f)

       
            if not lumisPerJob == None:
                JobGroupList.extend(self.LumiBasedJobSplitting(lumiDict, lumisPerJob, jobInstance, groupInstance, location))
            elif not eventsPerJob == None:
                JobGroupList.extend(self.EventBasedJobSplitting(lumiDict, eventsPerJob, jobInstance, groupInstance, location))
            else:
                JobGroupList.extend(self.FileBasedJobSplitting(lumiDict, filesPerJob, jobInstance, groupInstance, location))


        return JobGroupList



    def FileBasedJobSplitting(self, lumiDict, filesPerJob, jobInstance, groupInstance, location):

        JobGroupList = []

        assignedFiles = []

        for lumi in lumiDict.keys():
            joblist = []
            baseName = makeUUID()
            jobFiles = Fileset()
            
            #Now split them into sections according to files per job
            for f in lumiDict[lumi]:
                if f['lfn'] in assignedFiles:
                    continue
                assignedFiles.append(f['lfn'])

                #Is it too big (should never be)
                if len(jobFiles) < filesPerJob:
                    jobFiles.addFile(f)
                #Now is it too big?
                if len(jobFiles) == filesPerJob:
                    job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                      files = jobFiles)
                    job["mask"].setMaxAndSkipLumis(1, lumi)
                    joblist.append(job)
                    jobFiles = Fileset()

            #If we've run out of files before completing a job
            if len(jobFiles) != 0:
                job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                  files = jobFiles)
                job["mask"].setMaxAndSkipLumis(1, lumi)
                joblist.append(job)
                jobFiles = Fileset()

            if len(joblist) > 0:
                jobGroup = groupInstance(subscription = self.subscription)
                jobGroup.add(joblist)
                jobGroup.commit()
                #jobGroup.setSite(location)
                joblist = []
                JobGroupList.append(jobGroup)

        return JobGroupList


    def LumiBasedJobSplitting(self, lumiDict, lumisPerJob, jobInstance, groupInstance, location):

        JobGroupList  = []
        currentLumis  = 0
        joblist       = []
        jobFiles      = Fileset()
        baseName = makeUUID()

        assignedFiles = []

        for lumi in lumiDict.keys():

            if currentLumis < lumisPerJob:
                lumisInJob = []
                for f in lumiDict[lumi]:
                    if f['lfn'] in assignedFiles:
                        continue
                    jobFiles.addFile(f)
                    assignedFiles.append(f['lfn'])
                    for run in list(f['runs']):
                        for l in run:
                            lumiID = int(str(run.run) + str(l))
                            if not lumiID in lumisInJob:
                                lumisInJob.append(lumiID)
                #Now increment
                currentLumis += len(lumisInJob)

            #If we now have enough lumis, we end.
            if currentLumis >= lumisPerJob:
                job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                  files = jobFiles)
                job["mask"].setMaxAndSkipLumis(currentLumis, lumi)
                joblist.append(job)
                #Wipe clean
                currentLumis = 0
                jobFiles = Fileset()

        if not len(jobFiles.getFiles()) == 0:
            #Then we have files we need to check in because we ran out of lumis before filling the last job
            job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                              files = jobFiles)
            job["mask"].setMaxAndSkipLumis(lumisPerJob, lumi)
            joblist.append(job)

        if len(joblist) > 0:
            #When done, create the jobGroup
            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(joblist)
            #jobGroup.setSite(location)
            jobGroup.commit()
            JobGroupList.append(jobGroup)

        return JobGroupList


    def EventBasedJobSplitting(self, lumiDict, eventsPerJob, jobInstance, groupInstance, location):
        
        JobGroupList  = []
        currentEvents = 0
        baseName = makeUUID()
        assignedFiles = []
        joblist = []

        for lumi in lumiDict.keys():
            jobFiles = Fileset()
            for file in lumiDict[lumi]:
                if file['lfn'] in assignedFiles:
                    continue
                assignedFiles.append(file['lfn'])

                eventsInFile = file['events']

                if eventsInFile > eventsPerJob:
                    #Push the panic button
                    print "File %s is too big to be processed.  Skipping" %(file['lfn'])
                    continue
                #If we don't have enough events, add the file to the job
                if eventsPerJob - currentEvents >= eventsInFile:
                    currentEvents = currentEvents + eventsInFile
                    jobFiles.addFile(file)
                #If you have enough events, end the job and start a new one
                else:
                    job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                      files = jobFiles)
                    job["mask"].setMaxAndSkipLumis(1, lumi)
                    joblist.append(job)

                    #Clear Fileset
                    jobFiles = Fileset()

                    #Now add next file into the next job
                    currentEvents = eventsInFile
                    jobFiles.addFile(file)
                    
            #If we have excess events, make a final job
            if not currentEvents == 0:
                job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                  files = jobFiles)
                job["mask"].setMaxAndSkipLumis(1, lumi)
                joblist.append(job)
                jobFiles = Fileset()
                currentEvents = 0
                    
            #For each location and lumi create a jobGroup and append it to JobGroupList
            if len(joblist) > 0:
                jobGroup = groupInstance(subscription = self.subscription)
                jobGroup.add(joblist)
                #jobGroup.setSite(location)
                jobGroup.commit()
                joblist = []
                JobGroupList.append(jobGroup)

        return JobGroupList
