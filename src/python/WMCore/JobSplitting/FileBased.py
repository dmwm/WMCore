#!/usr/bin/env python
"""
_FileBased_

File based splitting algorithm that will chop a fileset into
a set of jobs based on file boundaries
"""

from __future__ import division
from future.utils import viewvalues
from builtins import int

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File
from WMCore.WMSpec.WMTask import buildLumiMask
from WMCore.JobSplitting.LumiBased import isGoodRun, isGoodLumi


class FileBased(JobFactory):

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split up all the available files such that each job will process a
        maximum of 'files_per_job'.  If the 'files_per_job' parameters is not
        passed in jobs will process a maximum of 10 files.
        """

        filesPerJob   = int(kwargs.get("files_per_job", 10))
        jobsPerGroup  = int(kwargs.get("jobs_per_group", 0))
        totalFiles    = int(kwargs.get("total_files", 0))
        runs          = kwargs.get('runs', None)
        lumis         = kwargs.get('lumis', None)
        runBoundaries = kwargs.get("respect_run_boundaries", False)
        getParents    = kwargs.get("include_parents", False)
        filesInJob    = 0
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))

        goodRunList = {}
        if runs and lumis:
            goodRunList = buildLumiMask(runs, lumis)

        #Get a dictionary of sites, files
        lDict = self.sortByLocation()
        locationDict = {}

        for key in lDict:
            newlist = []
            for f in lDict[key]:
                if runs and lumis:
                    ## Skip this file is it has no runs.
                    if len(f['runs']) == 0:
                        continue
                    f['lumiCount'] = 0
                    f['runs'] = sorted(f['runs'])
                    for run in f['runs']:
                        run.lumis.sort()
                        f['lumiCount'] += len(run.lumis)
                    f['lowestRun'] = f['runs'][0]
                    ## Skip this file is it has no lumis.
                    if f['lumiCount'] == 0:
                        continue
                    ## Do average event per lumi calculation.
                    f['avgEvtsPerLumi'] = int(round(f['events'] / f['lumiCount']))
                newlist.append(f)
            locationDict[key] = sorted(newlist, key = lambda f: f['lfn'])

        ## Make a list with all the files, sorting them by LFN. Remove from the list all
        ## the files filtered out by the lumi-mask (if there is one).
        files = []
        for filesPerLocSet in viewvalues(locationDict):
            for f in filesPerLocSet:
                files.append(f)
        if len(files):
            files = sorted(files, key = lambda f: f['lfn'])
            if runs and lumis:
                skippedFiles = []
                for f in files:
                    skipFile = True
                    for run in f['runs']:
                        if not isGoodRun(goodRunList, run.run):
                            continue
                        for lumi in run:
                            if not isGoodLumi(goodRunList, run.run, lumi):
                                continue
                            skipFile = False
                    if skipFile:
                        skippedFiles.append(f)
                for f in skippedFiles:
                    files.remove(f)

        ## Keep only the first totalFiles files. Remove the other files from the locationDict.
        if totalFiles > 0 and totalFiles < len(files):
            removedFiles = files[totalFiles:]
            files = files[:totalFiles]
            for f in removedFiles:
                for locSet in locationDict:
                    if f in locationDict[locSet]:
                        locationDict[locSet].remove(f)

        for locSet in locationDict:
            #Now we have all the files in a certain location set
            fileList = locationDict[locSet]
            filesInJob  = 0
            jobsInGroup = 0
            self.newGroup()
            if len(fileList) == 0:
                continue
            jobRun = None
            for f in fileList:
                if getParents:
                    parentLFNs = self.findParent(lfn = f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn = lfn)
                        f['parents'].add(parent)
                fileRun = f.get('minrun', None)
                createNewJob = False
                if filesInJob == 0 or filesInJob == filesPerJob or (runBoundaries and fileRun != jobRun):
                    createNewJob = True
                if runs and lumis:
                    for run in f['runs']:
                        if not isGoodRun(goodRunList, run.run):
                            continue
                        firstLumi = None
                        lastLumi = None
                        for lumi in run:
                            if not isGoodLumi(goodRunList, run.run, lumi):
                                if firstLumi != None and lastLumi != None:
                                    self.currentJob['mask'].addRunAndLumis(run = run.run, lumis = [firstLumi, lastLumi])
                                    addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                    runAddedTime = addedEvents * timePerEvent
                                    runAddedSize = addedEvents * sizePerEvent
                                    self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                                    firstLumi = None
                                    lastLumi = None
                                continue
                            if lastLumi != None and lumi != lastLumi + 1:
                                self.currentJob['mask'].addRunAndLumis(run = run.run, lumis = [firstLumi, lastLumi])
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                                firstLumi = None
                                lastLumi = None
                            if createNewJob:
                                if jobsPerGroup:
                                    if jobsInGroup > jobsPerGroup:
                                        self.newGroup()
                                        jobsInGroup = 0
                                self.newJob(name = self.getJobName())
                                self.currentJob.addResourceEstimates(memory = memoryRequirement)
                                filesInJob = 0
                                jobsInGroup += 1
                                jobRun = fileRun
                                createNewJob = False
                                self.currentJob.addFile(f)
                                filesInJob += 1
                            if firstLumi == None:
                                firstLumi = lumi
                            lastLumi = lumi
                            if self.currentJob and not f in self.currentJob['input_files']:
                                self.currentJob.addFile(f)
                                filesInJob += 1
                        if firstLumi != None and lastLumi != None:
                            self.currentJob['mask'].addRunAndLumis(run = run.run, lumis = [firstLumi, lastLumi])
                            addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                            runAddedTime = addedEvents * timePerEvent
                            runAddedSize = addedEvents * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                            firstLumi = None
                            lastLumi = None
                else:
                    if createNewJob:
                        if jobsPerGroup:
                            if jobsInGroup > jobsPerGroup:
                                self.newGroup()
                                jobsInGroup = 0
                        self.newJob(name = self.getJobName())
                        self.currentJob.addResourceEstimates(memory = memoryRequirement)
                        filesInJob = 0
                        jobsInGroup += 1
                        jobRun = fileRun
                    self.currentJob.addFile(f)
                    filesInJob += 1
                    fileTime = f['events'] * timePerEvent
                    fileSize = f['events'] * sizePerEvent
                    self.currentJob.addResourceEstimates(jobTime = fileTime, disk = fileSize)

        return
