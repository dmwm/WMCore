#!/usr/bin/env python
"""
_EventAwareLumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections, failing jobs with too many
events in a lumi and adapting the number of lumis per job
according to the average number of events per lumi in the files.

Created on Sep 25, 2012

@author: dballest
"""

import logging
import operator
import traceback
import math

from WMCore.DataStructs.Run         import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.JobSplitting.LumiBased  import isGoodLumi, isGoodRun
from WMCore.WMBS.File               import File
from WMCore.WMSpec.WMTask           import buildLumiMask

class EventAwareLumiBased(JobFactory):
    """
    Split jobs by lumis taking into account events per lumi
    """

    locations = []


    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split files into a number of lumis per job
        Allow a flag to determine if we split files between jobs
        """

        avgEventsPerJob = int(kwargs.get('events_per_job', 5000))
        eventLimit      = int(kwargs.get('max_events_per_lumi', 20000))
        splitOnFile     = bool(kwargs.get('halt_job_on_file_boundaries', True))
        ignoreACDC      = bool(kwargs.get('ignore_acdc_except', False))
        collectionName  = kwargs.get('collectionName', None)
        splitOnRun      = kwargs.get('splitOnRun', True)
        getParents      = kwargs.get('include_parents', False)
        runWhitelist    = kwargs.get('runWhitelist', [])
        runs            = kwargs.get('runs', None)
        lumis           = kwargs.get('lumis', None)
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))
        deterministicPileup = kwargs.get('deterministicPileup', False)
        eventsPerLumiInDataset = 0

        if deterministicPileup and self.package == 'WMCore.WMBS':
            getJobNumber = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
            jobNumber = getJobNumber.execute(workflow = self.subscription.getWorkflow().id)
            self.nJobs = jobNumber

        goodRunList = {}
        if runs and lumis:
            goodRunList = buildLumiMask(runs, lumis)

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL       = kwargs.get('couchURL')
                couchDB        = kwargs.get('couchDB')
                filesetName    = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                owner          = kwargs.get('owner')
                group          = kwargs.get('group')

                logging.info('Creating jobs for ACDC fileset %s' % filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                goodRunList = dcs.getLumiWhitelist(collectionName, filesetName, owner, group)
            except Exception, ex:
                msg =  "Exception while trying to load goodRunList\n"
                if ignoreACDC:
                    msg +=  "Ditching goodRunList\n"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logging.error(msg)
                    goodRunList = {}
                else:
                    msg +=  "Refusing to create any jobs.\n"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logging.error(msg)
                    return

        lDict = self.sortByLocation()
        locationDict = {}

        # First we need to load the data
        if self.package == 'WMCore.WMBS':
            loadRunLumi = self.daoFactory(classname = "Files.GetBulkRunLumi")

        for key in lDict.keys():
            newlist = []
            # First we need to load the data
            if self.package == 'WMCore.WMBS':
                fileLumis = loadRunLumi.execute(files = lDict[key])
                for f in lDict[key]:
                    lumiDict = fileLumis.get(f['id'], {})
                    for run in lumiDict.keys():
                        f.addRun(run = Run(run, *lumiDict[run]))

            for f in lDict[key]:
                if len(f['runs']) == 0:
                    continue
                f['runs'] = sorted(f['runs'])
                f['lumiCount'] = 0
                for run in f['runs']:
                    run.lumis.sort()
                    f['lumiCount'] += len(run.lumis)
                f['lowestRun'] = f['runs'][0]

                #Do average event per lumi calculation
                if f['lumiCount']:
                    f['avgEvtsPerLumi'] = float(f['events'])/f['lumiCount']
                    if deterministicPileup:
                        # We assume that all lumis are equal in the dataset
                        eventsPerLumiInDataset = f['avgEvtsPerLumi']
                else:
                    #No lumis in the file, ignore it
                    continue
                newlist.append(f)


            locationDict[key] = sorted(newlist, key=operator.itemgetter('lowestRun'))

        totalJobs      = 0
        lastLumi       = None
        firstLumi      = None
        lastRun        = None
        lumisInJob     = 0
        currentJobAvgEventCount = 0
        for location in locationDict:

            # For each location, we need a new jobGroup
            self.newGroup()
            stopJob = True
            for f in locationDict[location]:

                if getParents:
                    parentLFNs = self.findParent(lfn = f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn = lfn)
                        f['parents'].add(parent)

                updateSplitOnJobStop = False
                failNextJob          = False
                #If the number of events per lumi is higher than the limit
                #and it's only one lumi then ditch that lumi
                if f['avgEvtsPerLumi'] > eventLimit and f['lumiCount'] == 1:
                    failNextJob = True
                    stopJob = True
                    lumisPerJob = 1
                elif splitOnFile:
                    # Then we have to split on every boundary
                    stopJob = True
                    #Check the average number of events per lumi in this file
                    #Adapt the lumis per job to match the target conditions
                    if f['avgEvtsPerLumi']:
                        #If there are events in the file
                        ratio = float(avgEventsPerJob) / f['avgEvtsPerLumi']
                        lumisPerJob = max(int(math.floor(ratio)), 1)
                    else:
                        #Zero event file, then the ratio goes to infinity. Computers don't like that
                        lumisPerJob = f['lumiCount']
                else:
                    #Analyze how many events does this job already has
                    #Check how many we want as target, include as many lumi sections as possible
                    updateSplitOnJobStop = True
                    eventsRemaining = max(avgEventsPerJob - currentJobAvgEventCount, 0)
                    if f['avgEvtsPerLumi']:
                        lumisAllowed = int(math.floor(float(eventsRemaining) / f['avgEvtsPerLumi']))
                    else:
                        lumisAllowed = f['lumiCount']
                    lumisPerJob = max(lumisInJob + lumisAllowed, 1)

                for run in f['runs']:
                    if not isGoodRun(goodRunList = goodRunList, run = run.run):
                        # Then skip this one
                        continue
                    if len(runWhitelist) > 0 and not run.run in runWhitelist:
                        # Skip due to run whitelist
                        continue
                    firstLumi = None

                    if splitOnRun and run.run != lastRun:
                        # Then we need to kill this job and get a new one
                        stopJob = True

                    # Now loop over the lumis
                    for lumi in run:
                        if not isGoodLumi(goodRunList, run = run.run, lumi = lumi):
                            # Kill the chain of good lumis
                            # Skip this lumi
                            if firstLumi != None and firstLumi != lumi:
                                self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                                       lumis = [firstLumi, lastLumi])
                                eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = eventsAdded * timePerEvent
                                runAddedSize = eventsAdded * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                                firstLumi = None
                                lastLumi = None
                            continue

                        # You have to kill the lumi chain if they're not continuous
                        if lastLumi and not lumi == lastLumi + 1:
                            self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                                   lumis = [firstLumi, lastLumi])
                            eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                            runAddedTime = eventsAdded * timePerEvent
                            runAddedSize = eventsAdded * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                            firstLumi = None
                            lastLumi = None

                        if firstLumi == None:
                            # Set the first lumi in the run
                            firstLumi = lumi

                        # If we're full, end the job
                        if lumisInJob == lumisPerJob:
                            stopJob = True
                        # Actually do the new job creation
                        if stopJob:
                            if firstLumi != None and lastLumi != None and lastRun != None:
                                self.currentJob['mask'].addRunAndLumis(run = lastRun,
                                                                       lumis = [firstLumi, lastLumi])
                                eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = eventsAdded * timePerEvent
                                runAddedSize = eventsAdded * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                            msg = None
                            if failNextJob:
                                msg = "File %s has too many events (%d) in %d lumi(s)" % (f['lfn'],
                                                                                          f['events'],
                                                                                          f['lumiCount'])
                            self.newJob(name = self.getJobName(), failedJob = failNextJob,
                                        failedReason = msg)
                            if deterministicPileup:
                                self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * lumisPerJob * eventsPerLumiInDataset)
                            self.currentJob.addResourceEstimates(memory = memoryRequirement)
                            failNextJob = False
                            firstLumi = lumi
                            lumisInJob = 0
                            currentJobAvgEventCount = 0
                            totalJobs += 1

                            # Add the file to new jobs
                            self.currentJob.addFile(f)

                            if updateSplitOnJobStop:
                                #Then we were carrying from a previous file
                                #Reset calculations for this file
                                updateSplitOnJobStop = False
                                if f['avgEvtsPerLumi']:
                                    ratio = float(avgEventsPerJob) / f['avgEvtsPerLumi']
                                    lumisPerJob = max(int(math.floor(ratio)), 1)
                                else:
                                    lumisPerJob = f['lumiCount']

                        lumisInJob += 1
                        lastLumi = lumi
                        stopJob = False
                        lastRun = run.run

                        if self.currentJob and not f in self.currentJob['input_files']:
                            self.currentJob.addFile(f)

                    if firstLumi != None and lastLumi != None:
                        # Add this run to the mask
                        self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                               lumis = [firstLumi, lastLumi])
                        eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                        runAddedTime = eventsAdded * timePerEvent
                        runAddedSize = eventsAdded * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                        firstLumi = None
                        lastLumi = None

                if not splitOnFile:
                    currentJobAvgEventCount += f['avgEvtsPerLumi'] * min(lumisInJob, f['lumiCount'])

        return
