#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
_LumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections
"""




import operator
import logging
import threading
import traceback

from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File               import File
from WMCore.WMSpec.WMTask           import buildLumiMask

def isGoodLumi(goodRunList, run, lumi):
    """
    _isGoodLumi_

    Checks to see if runs match a run-lumi combination in the goodRunList
    This is a pain in the ass.
    """
    if goodRunList == None or goodRunList == {}:
        return True

    if not isGoodRun(goodRunList = goodRunList, run = run):
        return False

    for runRange in goodRunList.get(str(run), [[]]):
        # For each run range, which should have 2 elements
        if not len(runRange) == 2:
            # Then we're very confused and should exit
            logging.error("Invalid run range!  Failing this lumi!")
        elif runRange[0] <= lumi and runRange[1] >= lumi:
            # If a lumi is within a particular runRange, return true.
            return True
    return False

def isGoodRun(goodRunList, run):
    """
    _isGoodRun_

    Tell if this is a good run
    """
    if goodRunList == None or goodRunList == {}:
        return True

    if str(run) in goodRunList.keys():
        # @e can find a run
        return True

    return False


class LumiBased(JobFactory):
    """
    Split jobs by number of events
    """

    locations = []

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split files into a number of lumis per job
        Allow a flag to determine if we split files between jobs
        """

        myThread = threading.currentThread()

        lumisPerJob = int(kwargs.get('lumis_per_job', 1))
        splitOnFile = bool(kwargs.get('halt_job_on_file_boundaries', True))
        ignoreACDC = bool(kwargs.get('ignore_acdc_except', False))
        collectionName = kwargs.get('collectionName', None)
        splitOnRun = kwargs.get('splitOnRun', True)
        getParents = kwargs.get('include_parents', False)
        runWhitelist = kwargs.get('runWhitelist', [])
        runs = kwargs.get('runs', None)
        lumis = kwargs.get('lumis', None)
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))

        goodRunList = {}
        if runs and lumis:
            goodRunList = buildLumiMask(runs, lumis)

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL = kwargs.get('couchURL')
                couchDB = kwargs.get('couchDB')
                filesetName = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                owner = kwargs.get('owner')
                group = kwargs.get('group')

                logging.info('Creating jobs for ACDC fileset %s' % filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                goodRunList = dcs.getLumiWhitelist(collectionName, filesetName, owner, group)
            except Exception, ex:
                msg = "Exception while trying to load goodRunList\n"
                if ignoreACDC:
                    msg += "Ditching goodRunList\n"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    logging.error(msg)
                    goodRunList = {}
                else:
                    msg += "Refusing to create any jobs.\n"
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
                # if hasattr(f, 'loadData'):
                #    f.loadData()
                if len(f['runs']) == 0:
                    continue
                f['lumiCount'] = 0
                f['runs'] = sorted(f['runs'])
                for run in f['runs']:
                    run.lumis.sort()
                    f['lumiCount'] += len(run.lumis)
                f['lowestRun'] = f['runs'][0]
                # Do average event per lumi calculation
                if f['lumiCount']:
                    f['avgEvtsPerLumi'] = float(f['events']) / f['lumiCount']
                else:
                    # No lumis in the file, ignore it
                    continue
                newlist.append(f)
            locationDict[key] = sorted(newlist, key = operator.itemgetter('lowestRun'))




        # Split files into jobs with each job containing
        # EXACTLY lumisPerJob number of lumis (except for maybe the last one)

        totalJobs = 0
        firstRun = None
        lastLumi = None
        firstLumi = None
        stopJob = True
        lastRun = None
        lumisInJob = 0
        for location in locationDict.keys():

            # For each location, we need a new jobGroup
            self.newGroup()
            stopJob = True
            for f in locationDict[location]:

                if getParents:
                    parentLFNs = self.findParent(lfn = f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn = lfn)
                        f['parents'].add(parent)

                if splitOnFile:
                    # Then we have to split on every boundary
                    stopJob = True

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
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime,
                                                                     disk = runAddedSize)
                                firstLumi = None
                                lastLumi = None
                            continue

                        # You have to kill the lumi chain if they're not continuous
                        if lastLumi and not lumi == lastLumi + 1:
                            self.currentJob['mask'].addRunAndLumis(run = run.run,
                                                                   lumis = [firstLumi, lastLumi])
                            addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                            runAddedTime = addedEvents * timePerEvent
                            runAddedSize = addedEvents * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime = runAddedTime,
                                                                 disk = runAddedSize)
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
                                addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = addedEvents * timePerEvent
                                runAddedSize = addedEvents * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime = runAddedTime,
                                                                     disk = runAddedSize)
                            self.newJob(name = self.getJobName(length = totalJobs))
                            self.currentJob.addResourceEstimates(memory = memoryRequirement)
                            firstLumi = lumi
                            lumisInJob = 0
                            totalJobs += 1

                            # Add the file to new jobs
                            self.currentJob.addFile(f)

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
                        addedEvents = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                        runAddedTime = addedEvents * timePerEvent
                        runAddedSize = addedEvents * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime = runAddedTime, disk = runAddedSize)
                        firstLumi = None
                        lastLumi = None

        return
