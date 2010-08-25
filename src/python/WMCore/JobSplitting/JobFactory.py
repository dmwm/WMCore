#!/usr/bin/env python






import logging
import threading
import gc

from WMCore.DataStructs.WMObject import WMObject
from WMCore.DataStructs.Fileset  import Fileset
from WMCore.DataStructs.File     import File
from WMCore.Services.UUID        import makeUUID
from WMCore.WMBS.File            import File as WMBSFile

class JobFactory(WMObject):
    """
    A JobFactory is created with a subscription (which has a fileset). It is a
    base class of an object instance of an object representing some job
    splitting algorithm. It is called with a job type (at least) to return a
    JobGroup object. The JobFactory should be subclassed by real splitting
    algorithm implementations.
    """
    def __init__(self,
                 package='WMCore.DataStructs',
                 subscription=None,
                 generators=[],
                 limit = 0):
        self.package = package
        self.subscription  = subscription
        self.generators    = generators
        self.jobInstance   = None
        self.groupInstance = None
        self.jobGroups     = []
        self.currentGroup  = None
        self.currentJob    = None
        self.nJobs         = 0
        self.baseUUID      = None
        self.limit         = limit
        self.transaction   = None
        self.proxies       = []
        self.grabByProxy   = False
        self.daoFactory    = None
        self.timing = {'jobInstance': 0, 'sortByLocation': 0, 'acquireFiles': 0, 'jobGroup': 0}

    def __call__(self, jobtype = "Job", grouptype = "JobGroup", *args, **kwargs):
        """
        __call__

        
        """

        #Need to reset the internal data for multiple calls to the factory
        self.jobGroups = []
        self.currentGroup = None
        self.currentJob = None

        self.siteBlacklist = kwargs.get("siteBlacklist", [])
        self.siteWhitelist = kwargs.get("siteWhitelist", [])

        # Every time we restart, re-zero the jobs
        self.nJobs = 0

        # Create a new name
        self.baseUUID = makeUUID()

        module = "%s.%s" % (self.package, jobtype)
        module = __import__(module, globals(), locals(), [jobtype])#, -1)
        self.jobInstance = getattr(module, jobtype.split('.')[-1])

        module = "%s.%s" % (self.package, grouptype)
        module = __import__(module, globals(), locals(), [grouptype])
        self.groupInstance = getattr(module, grouptype.split('.')[-1])

        map(lambda x: x.start(), self.generators)


        self.limit        = int(kwargs.get("file_load_limit", self.limit))
        self.algorithm(*args, **kwargs)
        self.commit()
        #gc.collect()

        map(lambda x: x.finish(), self.generators)
        return self.jobGroups

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        A splitting algorithm that takes all available files from the
        subscription and splits them into jobs and inserts them into job groups.
        The algorithm must return a list of job groups.
        """
        self.newGroup(args, kwargs)
        self.newJob(name='myJob')

    def newGroup(self, *args, **kwargs):
        """
        Return and new JobGroup
        """
        self.appendJobGroup()
        self.currentGroup = self.groupInstance(subscription=self.subscription)
        map(lambda x: x.startGroup(self.currentGroup), self.generators)


    def newJob(self, name=None, files=None):
        """
        Instantiate a new Job onject, apply all the generators to it
        """
        self.currentJob = self.jobInstance(name, files)
        self.currentJob["task"] = self.subscription.taskName()
        self.currentJob["workflow"] = self.subscription.workflowName()
        self.currentJob["owner"] = self.subscription.owner()
        self.currentJob["siteBlacklist"] = self.siteBlacklist
        self.currentJob["siteWhitelist"] = self.siteWhitelist

        self.nJobs += 1
        for gen in self.generators:
            gen(self.currentJob)
        self.currentGroup.add(self.currentJob)


    def appendJobGroup(self):
        """
        Append jobGroup to jobGroup list

        """

        if self.currentGroup:
            map(lambda x: x.finishGroup(self.currentGroup), self.generators)
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

        logging.debug("About to commit %i jobGroups" % (len(self.jobGroups)))
        logging.debug("About to commit %i jobs" % (len(self.jobGroups[0].newjobs)))

        if self.package == 'WMCore.WMBS':
            self.subscription.bulkCommit(jobGroups = self.jobGroups)
        else:
            # Then we have a DataStructs job, and we have to do everything
            # by hand.
            for jobGroup in self.jobGroups:
                jobGroup.commit()
                for job in jobGroup.jobs:
                    job.save()
            self.subscription.save()

        gc.collect()
        return

    def sortByLocation(self):
        """
        _sortByLocation_

        Sorts the files in the job by location and passes back a dictionary of files, with each key corresponding
        to a set of locations
        """

        fileDict = {}

        if self.grabByProxy:
            logging.debug("About to load files by proxy")
            fileset = self.loadFiles(size = self.limit)
            logging.debug("Loaded %i files" % (len(fileset)))
        else:
            fileset = self.subscription.availableFiles(limit = self.limit)
            logging.debug("About to load files by DAO")

        for file in fileset:
            locSet = frozenset(file['locations'])

            if len(locSet) == 0:
                msg = 'File %s has no locations!' %(file['lfn'])
                logging.error(msg)

            if locSet in fileDict.keys():
                fileDict[locSet].append(file)
            else:
                fileDict[locSet] = [file]

        return fileDict


    def getJobName(self, length = None):
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

        # Handle all DAO stuff
        from WMCore.DAOFactory  import DAOFactory
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)


        subAction = self.daoFactory(classname = "Subscriptions.GetAvailableFiles")
        results   = subAction.execute(subscription = self.subscription['id'],
                                      returnCursor = True)

        for proxy in results:
            self.proxies.append(proxy)
            logging.debug("Received %i proxies" % (len(self.proxies)))

        # Activate everything so that we grab files by proxy
        self.grabByProxy  = True



        return






    def close(self):
        """
        _close_

        Close any leftover connections
        """


        self.proxies     = []
        self.grabByProxy = False




    def loadFiles(self, size = 10):
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
        rawResults  = []
        if type(resultProxy.keys) == list:
            keys  = resultProxy.keys
        else:
            keys  = resultProxy.keys()
            if type(keys) == set:
                # If it's a set, handle it
                keys = list(keys)
        files       = set()


        while len(rawResults) < size and len(self.proxies) > 0:
            length = size - len(rawResults)
            newResults = resultProxy.fetchmany(size = length)
            if len(newResults) < length:
                # Assume we're all out
                # Eliminate this proxy
                self.proxies.remove(resultProxy)
            rawResults.extend(newResults)



        fileList = self.formatDict(results = rawResults, keys = keys)


        fileInfoAct  = self.daoFactory(classname = "Files.GetByID")
        fileInfoDict = fileInfoAct.execute(file = [x["file"] for x in fileList])
        
        #Run through all files
        for f in fileList:
            fl = WMBSFile(id = f['file'])
            #fl.loadChecksum()
            fl.update(fileInfoDict[f['file']])
            if 'locations' in f.keys():
                fl.setLocation(f['locations'], immediateSave = False)
            files.add(fl)


        return files
            




    def formatDict(self, results, keys):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming 'fileid' to file.
        """

        myThread = threading.currentThread()

        formattedResults = []
        for entry in results:
            indivDict = {}
            for i in range(len(keys)):
                # Assign each key to a value
                indivDict[str(keys[i])] = entry[i]
            formattedResults.append(indivDict)
                           
        for formattedResult in formattedResults:
            if "file" in formattedResult.keys():
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        #Now the tricky part
        tempResults = {}
        for formattedResult in formattedResults:
            if formattedResult["file"] not in tempResults.keys():
                tempResults[formattedResult["file"]] = []
            if "se_name" in formattedResult.keys():
                tempResults[formattedResult["file"]].append(formattedResult["se_name"])

        finalResults = []
        for key in tempResults.keys():
            tmpDict = {"file": key}
            if not tempResults[key] == []:
                tmpDict['locations'] = tempResults[key]
            finalResults.append(tmpDict)

        return finalResults

