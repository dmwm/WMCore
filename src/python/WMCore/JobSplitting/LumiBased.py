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
from WMCore.DataStructs.LumiList import LumiList

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
        totalLumis = int(kwargs.get('total_lumis', 0))
        splitOnFile = bool(kwargs.get('halt_job_on_file_boundaries', True))
        ignoreACDC = bool(kwargs.get('ignore_acdc_except', False))
        collectionName = kwargs.get('collectionName', None)
        splitOnRun = kwargs.get('splitOnRun', True)
        getParents = kwargs.get('include_parents', False)
        runWhitelist = kwargs.get('runWhitelist', [])
        runs = kwargs.get('runs', None)
        lumis = kwargs.get('lumis', None)
        deterministicPileup = kwargs.get('deterministicPileup', False)
        eventsPerLumiInDataset = 0

        if deterministicPileup and self.package == 'WMCore.WMBS':
            getJobNumber = self.daoFactory(classname = "Jobs.GetNumberOfJobsPerWorkflow")
            jobNumber = getJobNumber.execute(workflow = self.subscription.getWorkflow().id)
            self.nJobs = jobNumber

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
                    if deterministicPileup:
                        # We assume that all lumis are equal in the dataset
                        eventsPerLumiInDataset = f['avgEvtsPerLumi']
                else:
                    # No lumis in the file, ignore it
                    continue
                newlist.append(f)
            locationDict[key] = sorted(newlist, key = operator.itemgetter('lowestRun'))




        # Split files into jobs with each job containing
        # EXACTLY lumisPerJob number of lumis (except for maybe the last one)

        totalJobs = 0
        lastLumi = None
        firstLumi = None
        stopJob = True
        stopTask = False
        lastRun = None
        lumisInJob = 0
        lumisInTask = 0
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
                            self.newJob(name = self.getJobName())
                            self.currentJob.addResourceEstimates(memory = memoryRequirement)
                            if deterministicPileup:
                                self.currentJob.addBaggageParameter("skipPileupEvents", (self.nJobs - 1) * lumisPerJob * eventsPerLumiInDataset)
                            firstLumi = lumi
                            lumisInJob = 0
                            totalJobs += 1

                            # Add the file to new jobs
                            self.currentJob.addFile(f)

                        lumisInJob += 1
                        lumisInTask += 1
                        lastLumi = lumi
                        stopJob = False
                        lastRun = run.run

                        if self.currentJob and not f in self.currentJob['input_files']:
                            self.currentJob.addFile(f)

                        if totalLumis > 0 and lumisInTask >= totalLumis:
                            stopTask = True
                            break

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

                    if stopTask:
                        break

                if stopTask:
                    break

            # We now have a list of jobs in a job group.  We will now iterate through them
            # to verify we have no single lumi processed by multiple jobs.
            jobs = self.currentGroup.newjobs
            overlap = 0
            lumicount = 0
            logging.debug("Current job group has %d jobs." % len(jobs))
            for idx1 in xrange(len(jobs)):
                job1 = jobs[idx1]
                lumicount += len(set(LumiList(compactList=job1['mask'].getRunAndLumis()).getLumis()))
                for idx2 in xrange(idx1+1, len(jobs)):
                    job2 = jobs[idx2]
                    overlap += self.lumiCorrection(job1, job2, locationDict[location])
            logging.info("There were %d overlapping lumis and %d total lumis." % (overlap, lumicount))

            if stopTask:
                break

        return

    def lumiCorrection(self, job1, job2, locations):
        """
        Due to error in processing (in particular, the Run I Tier-0), some
        lumi sections may be spread across multiple jobs.  Where possible:
         - Remove a lumi from job2 if it is in both job1 and job2
         - If the lumi is in multiple files and it spanned multiple jobs,
           make sure that all those files are processed by job1.

        NOTE: This will not help in the case where a lumi is split across
        multiple blocks.

        Returns the number of affected lumis
        """
        lumis1 = LumiList(compactList=job1['mask'].getRunAndLumis())
        lumis2 = LumiList(compactList=job2['mask'].getRunAndLumis())
        ilumis = lumis1 & lumis2
        lumiPairs = ilumis.getLumis()
        if not lumiPairs:
            return 0
        logging.warning("%d lumis appear in multiple jobs: %s" % (len(lumiPairs), str(ilumis)))
        job2['mask'].removeLumiList(ilumis)

        for run, lumi in lumiPairs:
            for fileObj in locations:
                if fileObj in job1['input_files']:
                    continue
                for runObj in fileObj['runs']:
                    if run == runObj.run:
                        if lumi in runObj.lumis:
                            if fileObj not in job1['input_files']:
                                logging.warning("Adding file %s to job input files so it will process all of a lumi section." % fileObj['lfn'])
                                job1.addFile(fileObj)
                                break

        return len(lumiPairs)
