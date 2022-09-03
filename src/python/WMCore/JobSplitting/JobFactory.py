#!/usr/bin/env python
"""
_JobFactory_

"""

from builtins import range

import logging
import threading
import operator

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Services.UUIDLib import makeUUID
from WMCore.WMBS.File import File as WMBSFile


class JobFactory(WMObject):
    """
    A JobFactory is created with a subscription (which has a fileset). It is a
    base class of an object instance of an object representing some job
    splitting algorithm. It is called with a job type (at least) to return a
    JobGroup object. The JobFactory should be subclassed by real splitting
    algorithm implementations.
    """

    def __init__(self, package='WMCore.DataStructs', subscription=None, generators=None, limit=0):
        super(JobFactory, self).__init__()
        self.package = package
        self.subscription = subscription
        self.generators = generators if generators else []
        self.jobInstance = None
        self.groupInstance = None
        self.jobGroups = []
        self.currentGroup = None
        self.currentJob = None
        self.nJobs = 0
        self.baseUUID = None
        self.limit = limit
        self.transaction = None
        self.proxies = []
        self.grabByProxy = False
        self.daoFactory = None
        self.timing = {'jobInstance': 0, 'sortByLocation': 0, 'acquireFiles': 0, 'jobGroup': 0}
        self.siteWhitelist = []
        self.siteBlacklist = []
        self.trustSitelists = False
        self.trustPUSitelists = False

        if package == "WMCore.WMBS":
            myThread = threading.currentThread()

            self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                         logger=myThread.logger,
                                         dbinterface=myThread.dbi)
            self.getParentInfoAction = self.daoFactory(classname="Files.GetParentAndGrandParentInfo")

            self.pnn_to_psn = self.daoFactory(classname="Locations.GetPNNtoPSNMapping").execute()

        return

    def __call__(self, jobtype="Job", grouptype="JobGroup", *args, **kwargs):
        """
        __call__


        """

        # Need to reset the internal data for multiple calls to the factory
        self.jobGroups = []
        self.currentGroup = None
        self.currentJob = None

        self.siteWhitelist = kwargs.get("siteWhitelist", [])
        self.siteBlacklist = kwargs.get("siteBlacklist", [])
        self.trustSitelists = kwargs.get("trustSitelists", False)
        self.trustPUSitelists = kwargs.get("trustPUSitelists", False)

        # Every time we restart, re-zero the jobs
        self.nJobs = 0

        # Create a new name
        self.baseUUID = makeUUID()

        module = "%s.%s" % (self.package, jobtype)
        module = __import__(module, globals(), locals(), [jobtype])  # , -1)
        self.jobInstance = getattr(module, jobtype.split('.')[-1])

        module = "%s.%s" % (self.package, grouptype)
        module = __import__(module, globals(), locals(), [grouptype])
        self.groupInstance = getattr(module, grouptype.split('.')[-1])

        list([x.start() for x in self.generators])

        self.limit = int(kwargs.get("file_load_limit", self.limit))
        self.algorithm(*args, **kwargs)
        self.commit()

        list([x.finish() for x in self.generators])
        return self.jobGroups

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        A splitting algorithm that takes all available files from the
        subscription and splits them into jobs and inserts them into job groups.
        The algorithm must return a list of job groups.
        """
        self.newGroup()
        self.newJob(name='myJob')
        return

    def newGroup(self):
        """
        Return and new JobGroup
        """
        self.appendJobGroup()
        self.currentGroup = self.groupInstance(subscription=self.subscription)
        list([x.startGroup(self.currentGroup) for x in self.generators])
        return

    def newJob(self, name=None, files=None, failedJob=False, failedReason=None):
        """
        Instantiate a new Job onject, apply all the generators to it
        """
        if name is None:
            name = self.getJobName()
        self.currentJob = self.jobInstance(name, files)
        self.currentJob["task"] = self.subscription.taskName()
        self.currentJob["workflow"] = self.subscription.workflowName()
        self.currentJob["jobType"] = self.subscription["type"]
        self.currentJob["taskType"] = self.subscription.workflowType()
        self.currentJob["owner"] = self.subscription.owner()

        # All production jobs must be run 1
        if self.subscription["type"] == "Production":
            self.currentJob["mask"].setMaxAndSkipRuns(0, 1)

        # Some jobs are not meant to be submitted, ever
        if failedJob:
            self.currentJob["failedOnCreation"] = True
            self.currentJob["failedReason"] = failedReason

        # Decides how the pileup data will be handled in runtime
        if self.trustPUSitelists:
            self.currentJob.addBaggageParameter("trustPUSitelists", self.trustPUSitelists)

        self.nJobs += 1
        for gen in self.generators:
            gen(self.currentJob)
        self.currentGroup.add(self.currentJob)
        return

    def appendJobGroup(self):
        """
        Append jobGroup to jobGroup list

        """

        if self.currentGroup:
            list([x.finishGroup(self.currentGroup) for x in self.generators])
        if self.currentGroup:
            self.jobGroups.append(self.currentGroup)
            self.currentGroup = None

        return

    def commit(self):
        """
        Bulk commit the JobGroups all at once
        """
        self.appendJobGroup()

        if len(self.jobGroups) == 0:
            return

        logging.debug("About to commit %i jobGroups", len(self.jobGroups))
        logging.debug("About to commit %i jobs", len(self.jobGroups[0].newjobs))

        if self.package == 'WMCore.WMBS':

            for jobGroup in self.jobGroups:

                for job in jobGroup.newjobs:
                    # temporary place holder for file location
                    fileLocations = set([])
                    if self.trustSitelists:
                        locSet = set(self.siteWhitelist) - set(self.siteBlacklist)
                    else:
                        locSet = set([])
                        for pnn in job['input_files'][0]['locations']:
                            locSet.update(self.pnn_to_psn.get(pnn, []))
                        fileLocations = locSet
                        if len(self.siteWhitelist) > 0:
                            locSet = locSet & set(self.siteWhitelist)
                        if len(self.siteBlacklist) > 0:
                            locSet = locSet - set(self.siteBlacklist)

                    job['possiblePSN'] = locSet
                    if len(job['possiblePSN']) == 0:
                        job["fileLocations"] = fileLocations
                        job["siteWhitelist"] = self.siteWhitelist
                        job["siteBlacklist"] = self.siteBlacklist
                # now after the jobs are created, remove input file locations
                # they are no longer needed and just take up space
                for job in jobGroup.newjobs:
                    for fileInfo in job['input_files']:
                        fileInfo['locations'] = set([])

            self.subscription.bulkCommit(jobGroups=self.jobGroups)

        else:

            # we have a DataStructs job and have to do everything by hand
            for jobGroup in self.jobGroups:
                jobGroup.commit()
                for job in jobGroup.jobs:
                    job.save()
            self.subscription.save()

        return

    def sortByLocation(self):
        """
        _sortByLocation_

        Retrieve available files and return them sorted by location.
        The keys in the dict correspond to a set of locations.

        If TrustSiteLists is set ignore the file locations and treat all files
        as being present in the same place (use key "AAA" location).
        """

        fileDict = {}

        if self.grabByProxy:
            logging.debug("About to load files by proxy")
            fileset = self.loadFiles(size=self.limit)
            logging.debug("Loaded %i files", len(fileset))
        else:
            logging.debug("About to load files by DAO")
            fileset = self.subscription.availableFiles(limit=self.limit, doingJobSplitting=True)

        # Reverse sorting this set by location is required to match how sorting was done in py2
        # Unittests that rely on this sorting method
        # - WMCore_t.WMBS_t.JobSplitting_t.FileBased_t.FileBasedTest:testSiteWhiteBlacklist
        # - WMCore_t.WMBS_t.JobSplitting_t.FileBased_t.FileBasedTest:testLocationSplit
        for fileInfo in sorted(fileset, key=operator.itemgetter('locations'), reverse=True):

            if self.trustSitelists:
                locSet = frozenset(set(['AAA']))
            else:
                locSet = frozenset(fileInfo['locations'])

                if len(locSet) == 0:
                    logging.error("File %s has no locations!", fileInfo['lfn'])

            if locSet in fileDict:
                fileDict[locSet].append(fileInfo)
            else:
                fileDict[locSet] = [fileInfo]

        return fileDict

    def getJobName(self, length=None):
        """
        _getJobName_

        Creates a job name based on workflow and task
        Uses a passed in integer length that MUST be unique!
        """

        if not length:
            length = self.nJobs

        name = '%s-%i' % (self.baseUUID, length)

        return name

    def open(self):
        """
        _open_

        Open a connection to the database, and put
        resulting ResultProxies in self.proxies
        """

        logging.debug("Opening DB resultProxies for JobFactory")

        myThread = threading.currentThread()

        subAction = self.daoFactory(classname="Subscriptions.GetAvailableFilesNoLocations")
        results = subAction.execute(subscription=self.subscription['id'],
                                    returnCursor=True,
                                    conn=myThread.transaction.conn,
                                    transaction=True)

        for proxy in results:
            self.proxies.append(proxy)

        logging.debug("Received %i proxies", len(self.proxies))
        # Activate everything so that we grab files by proxy
        self.grabByProxy = True

        return

    def close(self):
        """
        _close_

        Close any leftover connections
        """
        self.proxies = []
        self.grabByProxy = False
        return

    def loadFiles(self, size=10):
        """
        _loadFiles_

        Grab some files from the resultProxy
        Should handle multiple proxies.  Not really sure about that
        """

        if len(self.proxies) < 1:
            # Well, you don't have any proxies.
            # This is what happens when you ran out of files last time
            logging.info("No additional files found; Ending.")
            return set()

        resultProxy = self.proxies[0]
        rawResults = []
        if isinstance(resultProxy.keys, list):
            keys = resultProxy.keys
        else:
            # the object below is of the type: sqlalchemy.engine.result.RMKeyView
            keys = list(resultProxy.keys())
        files = set()

        while len(rawResults) < size and len(self.proxies) > 0:
            length = size - len(rawResults)
            newResults = resultProxy.fetchmany(size=length)
            if len(newResults) < length:
                # Assume we're all out
                # Eliminate this proxy
                self.proxies.remove(resultProxy)
            rawResults.extend(newResults)

        if rawResults == []:
            # Nothing to do
            return set()

        fileList = self.formatDict(results=rawResults, keys=keys)
        fileIDs = list(set([x['fileid'] for x in fileList]))

        myThread = threading.currentThread()
        fileInfoAct = self.daoFactory(classname="Files.GetForJobSplittingByID")
        fileInfoDict = fileInfoAct.execute(file=fileIDs,
                                           conn=myThread.transaction.conn,
                                           transaction=True)

        getLocAction = self.daoFactory(classname="Files.GetLocationBulk")
        getLocDict = getLocAction.execute(files=fileIDs,
                                          conn=myThread.transaction.conn,
                                          transaction=True)

        for fID in fileIDs:
            fl = WMBSFile(id=fID)
            fl.update(fileInfoDict[fID])
            locations = getLocDict.get((fID), [])
            for loc in locations:
                fl.setLocation(loc, immediateSave=False)
            files.add(fl)

        return files

    def formatDict(self, results, keys):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming 'fileid' to file.
        """

        formattedResults = []
        for entry in results:
            indivDict = {}
            for i in range(len(keys)):
                # Assign each key to a value
                indivDict[str(keys[i]).lower()] = entry[i]
            formattedResults.append(indivDict)

        return formattedResults

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

            elif parentInfo['gpmerged'] is None:
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
                parentSet = self.findParent(lfn=parentInfo['gplfn'])
                for parent in parentSet:
                    newParents.add(parent)

        return newParents

    def checkForAmountOfWork(self):
        """
        Depending on what and how we are running the job splitting, check or
        not for a minimum amount of work (lumi sections) before passing the list
        of files all the way to the algorithm
        :return: a boolean flag
        """
        if self.collectionName:
            return False

        fileset = self.subscription.getFileset()
        fileset.load()
        if not fileset.open:
            return False  # it's closed, so just run the last jobs for it

        return True

    def getFilesSortedByLocation(self, eventsPerJob):
        """
        _getFilesSortedByLocation_

        Function used by the EventAware* splitting algorithms for retrieving
        a list of files available and sort them by location.

        If the fileset is closed, keep splitting these data. Otherwise check
        whether there are enough events in each of these locations, if events
        don't match the desired events_per_job splitting parameter, then skip
        those files until further cycles.
        :param eventsPerJob: number of events desired in the splitting
        :return: a dictionary of files, key'ed by a frozenset location
        """
        lDict = self.sortByLocation()
        if not self.loadRunLumi:
            return lDict  # then it's a DataStruct/CRAB splitting

        if self.checkForAmountOfWork():
            # first, check whether we have enough files to reach the desired events_per_job
            for sites in list(lDict):  # lDict changes size during for loop!
                availableEventsPerLocation = sum([f['events'] for f in lDict[sites]])
                if eventsPerJob > availableEventsPerLocation:
                    # then we don't split these files for the moment
                    lDict.pop(sites)

        return lDict

    @staticmethod
    def getPerformanceParameters(defaultParams):
        """
        _getPerformanceParameters_

        This generic JobFactory function allows all splitters to easily
        retrieve the parameters to specify the resource requirements for
        the created jobs.
        # TODO: Check couchDB for performance on the current task
                and if not available it falls back to the default params.
        It returns a tuple with the following values:
        timePerEvent, sizePerEvent, memoryRequirement
        """
        timePerEvent = defaultParams.get('timePerEvent', 0) or 0
        sizePerEvent = defaultParams.get('sizePerEvent', 0) or 0
        memory = defaultParams.get('memoryRequirement', 0) or 0
        return timePerEvent, sizePerEvent, memory
