#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
# W6501: It doesn't like string formatting in logging messages
"""
The actual error handler algorithm
"""
__all__ = []



import threading
import logging
import traceback
import collections

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.WMSpec.WMWorkload           import WMWorkload, WMWorkloadHelper
from WMCore.WMException                 import WMException

class ErrorHandlerException(WMException):
    """
    The Exception class for the ErrorHandlerPoller

    """
    pass


class ErrorHandlerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.changeState = ChangeState(self.config)

        self.maxRetries     = self.config.ErrorHandler.maxRetries
        self.maxProcessSize = getattr(self.config.ErrorHandler, 'maxProcessSize', 250)

        self.getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")

        self.dataCollection = DataCollectionService(url = config.ACDC.couchurl,
                                                    database = config.ACDC.database)

        self.specCache = collections.deque(maxlen = 1000)

        return
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

        # For now, does nothing

        return

        

    def terminate(self, params):
        """
        _terminate_
        
        Do one pass, then commit suicide
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


    def processRetries(self, jobs, jobType):
        """
        Actually do the retries

        """
        logging.info("Processing retries for %i failed jobs of type %s." % (len(jobs), jobType))
        exhaustJobs = []
        cooloffJobs = []

	# Retries < max retry count
        for ajob in jobs:
            # Retries < max retry count
            if ajob['retry_count'] < self.maxRetries:
                cooloffJobs.append(ajob)
            # Check if Retries >= max retry count
            elif ajob['retry_count'] >= self.maxRetries:
                exhaustJobs.append(ajob)
                logging.error("Exhausting job %i" % ajob['id'])
                logging.debug("JobInfo: %s" % ajob)
            else:
                logging.debug("Job %i had %s retries remaining" \
                              % (ajob['id'], str(ajob['retry_count'])))

        #Now to actually do something.
        logging.debug("About to propagate jobs")
        self.changeState.propagate(exhaustJobs, 'exhausted', \
                                   '%sfailed' %(jobType))
        self.changeState.propagate(cooloffJobs, '%scooloff' %(jobType), \
                                   '%sfailed' %(jobType))

        # Remove all the files in the exhausted jobs.
        logging.debug("About to fail input files for exhausted jobs")
        for job in exhaustJobs:
            job.failInputFiles()

        return exhaustJobs


    def handleACDC(self, jobList):
        """
        _handleACDC_

        Do the ACDC creation and hope it works
        """
        logging.debug("Entering ACDC with %i jobs" % len(jobList))
        collectionDict = {}

        for job in jobList:
            if not job['spec'] in collectionDict.keys():
                collectionDict[job['spec']] = []
            collectionDict[job['spec']].append(job)
            job.getMask()

        for spec in collectionDict.keys():
            
            # Load spec if we have to:
            wmSpec = None
            for sDict in self.specCache:
                # First, is the spec in the cache?
                if sDict['spec'] == spec:
                    wmSpec = sDict['wmSpec']
                    break
            if not wmSpec:
                # Then we didn't find the spec in the cache
                wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
                wmWorkload.load(spec)
                self.specCache.append({'spec': spec, 'wmSpec': wmWorkload})
                wmSpec = wmWorkload
                # Should only need to create a collection once for a given spec
                self.dataCollection.createCollection(wmSpec = wmSpec)



            logging.debug("About to begin collection creation in ACDC")
            failedJobs = collectionDict[spec]
            self.dataCollection.failedJobs(failedJobs)

        return

    def splitJobList(self, jobList, jobType):
        """
        _splitJobList_

        Split up list of jobs into more manageable chunks if necessary
        """
        if len(jobList) < 1:
            # Nothing to do
            return

        myThread = threading.currentThread()

        while len(jobList) > 0:
            # Loop over the list and handle it one chunk at a time
            tmpList = jobList[:self.maxProcessSize]
            jobList = jobList[self.maxProcessSize:]
            logging.debug("About to process %i errors" % len(tmpList))
            myThread.transaction.begin()
            exhaustList = self.processRetries(tmpList, jobType)
            self.handleACDC(jobList = exhaustList)
            myThread.transaction.commit()

        return
            

            

    def handleErrors(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """

        createList = []
        submitList = []
        jobList    = []

        # Run over created jobs
        idList = self.getJobs.execute(state = 'CreateFailed')
        logging.info("Found %s failed jobs failed during creation" \
                     % len(idList))
        if len(idList) > 0:
            createList = self.loadJobsFromList(idList = idList)

        # Run over submitted jobs
        idList = self.getJobs.execute(state = 'SubmitFailed')
        logging.info("Found %s failed jobs failed during submit" \
                     % len(idList))
        if len(idList) > 0:
            submitList = self.loadJobsFromList(idList = idList)

        # Run over executed jobs
        idList = self.getJobs.execute(state = 'JobFailed')
        logging.info("Found %s failed jobs failed during execution" \
                     % len(idList))
        if len(idList) > 0:
            jobList = self.loadJobsFromList(idList = idList)

        self.splitJobList(jobList = createList, jobType = 'create')
        self.splitJobList(jobList = submitList, jobType = 'submit')
        self.splitJobList(jobList = jobList,    jobType = 'job')

        return


    def loadJobsFromList(self, idList):
        """
        _loadJobsFromList_

        Load jobs in bulk
        """

        loadAction = self.daoFactory(classname = "Jobs.LoadForErrorHandler")


        binds = []
        for jobID in idList:
            binds.append({"jobid": jobID})

        results = loadAction.execute(jobID = binds)

        # You have to have a list
        if type(results) == dict:
            results = [results]

        listOfJobs = []
        for entry in results:
            # One job per entry
            tmpJob = Job(id = entry['id'])
            tmpJob.update(entry)
            listOfJobs.append(tmpJob)


        return listOfJobs


    def algorithm(self, parameters = None):
        """
	Performs the handleErrors method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Running error handling algorithm")
        myThread = threading.currentThread()
        try:
            self.handleErrors()
        except WMException, ex:
            try:
                myThread.transaction.rollback()
            except:
                pass
            raise
        except Exception, ex:
            msg = "Caught exception in ErrorHandler\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            logging.error(msg)
            if getattr(myThread, 'transaction', None) != None \
               and getattr(myThread.transaction, 'transaction', None) != None:
                myThread.transaction.rollback()
            raise ErrorHandlerException(msg)

