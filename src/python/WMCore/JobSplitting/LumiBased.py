#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections
"""

__revision__ = "$Id: LumiBased.py,v 1.3 2009/06/05 16:40:34 mnorman Exp $"
__version__  = "$Revision: 1.3 $"

from sets import Set

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.DataStructs.Fileset import Fileset
from WMCore.Services.UUID import makeUUID

class LumiBased(JobFactory):
    """
    Split jobs by number of events
    """
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

        #Get all files in fileset
        for f in fileset:

            if sum([ len(run) for run in f['runs']]) == 0:
                continue
            fileLumi = None
            fileLumiList = []
            #Let's grab all the Lumi sections in the file
            for run in f['runs']:
                for i in run:
                    fileLumiList.append(i)
            fileLumi = min(fileLumiList)

            if not lumiDict.has_key(fileLumi):
                lumiDict[fileLumi] = []

            lumiDict[fileLumi].append(f)


        if not lumisPerJob == None:
            JobGroupList = self.LumiBasedJobSplitting(lumiDict, lumisPerJob, jobInstance, groupInstance)
        elif not eventsPerJob == None:
            JobGroupList = self.EventBasedJobSplitting(lumiDict, eventsPerJob, jobInstance, groupInstance)
        else:
            JobGroupList = self.FileBasedJobSplitting(lumiDict, filesPerJob, jobInstance, groupInstance)

        return JobGroupList



    def FileBasedJobSplitting(self, lumiDict, filesPerJob, jobInstance, groupInstance):

        JobGroupList = []
        baseName = makeUUID()
        
        for lumi in lumiDict.keys():
            joblist = []

            #Now split them into sections according to files per job
            while len(lumiDict[lumi]) > 0:
                jobFiles = Fileset()
                for i in range(filesPerJob):
                    #Watch out if your last job has less then the full number of files
                    if len(lumiDict[lumi]) > 0:
                        jobFiles.addFile(lumiDict[lumi].pop())

                # Create the job
                job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                  files = jobFiles)
                job["mask"].setMaxAndSkipLumis(1, lumi)
                joblist.append(job)


            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(joblist)
            jobGroup.commit()
            JobGroupList.append(jobGroup)

        return JobGroupList


    def LumiBasedJobSplitting(self, lumiDict, lumisPerJob, jobInstance, groupInstance):

        JobGroupList  = []
        currentLumis  = 0
        joblist       = []
        jobFiles      = Fileset()
        baseName = makeUUID()


        for lumi in lumiDict.keys():

            #If we don't have enough Lumis in this job, add another one
            if currentLumis < lumisPerJob:
                for f in lumiDict[lumi]:
                    jobFiles.addFile(f)
                #Now increment
                currentLumis = currentLumis + 1

            #If we now have enough lumis, we end.
            if currentLumis == lumisPerJob:
                job = jobInstance(name = '%s-%s' % (baseName, len(joblist) + 1),
                                  files = jobFiles)
                job["mask"].setMaxAndSkipLumis(lumisPerJob, lumi)
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

        #When done, create the jobGroup
        jobGroup = groupInstance(subscription = self.subscription)
        jobGroup.add(joblist)
        jobGroup.commit()
        JobGroupList.append(jobGroup)

        return JobGroupList


    def EventBasedJobSplitting(self, lumiDict, eventsPerJob, jobInstance, groupInstance):
        
        JobGroupList  = []
        currentEvents = 0
        baseName = makeUUID()

        for lumi in lumiDict.keys():
            joblist = []

            jobFiles = Fileset()
            for file in lumiDict[lumi]:
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

                    
            #For each lumi create a jobGroup and append it to JobGroupList

            jobGroup = groupInstance(subscription = self.subscription)
            jobGroup.add(joblist)
            jobGroup.commit()
            JobGroupList.append(jobGroup)

        return JobGroupList
