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

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.ACDC.DataCollectionService  import DataCollectionService
from WMCore.WMSpec.WMWorkload           import WMWorkload, WMWorkloadHelper


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

        self.maxRetries = self.config.ErrorHandler.maxRetries

        self.getJobs = self.daoFactory(classname = "Jobs.GetAllJobs")

        self.dataCollection = DataCollectionService(url = config.ACDC.couchurl,
                                                    database = config.ACDC.database)

        self.specCache = {}

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

        exhaustJobs = []
        cooloffJobs = []

	# Retries < max retry count
        for ajob in jobs:
            # Retries < max retry count
            if ajob['retry_count'] < self.maxRetries:
                cooloffJobs.append(ajob)
            # Check if Retries >= max retry count
            if ajob['retry_count'] >= self.maxRetries:
                exhaustJobs.append(ajob)
                #SIMON's CODE SHOULD PUT the job in "newstate" state
            else:
                logging.debug("Job %i had %s retries remaining" \
                              % (ajob['id'], str(ajob['retry_count'])))

        #Now to actually do something.
        self.changeState.propagate(exhaustJobs, 'exhausted', \
                                   '%sfailed' %(jobType))
        self.changeState.propagate(cooloffJobs, '%scooloff' %(jobType), \
                                   '%sfailed' %(jobType))

        # Remove all the files in the exhausted jobs.
        for job in exhaustJobs:
            job.failInputFiles()

        return exhaustJobs


    def handleACDC(self, jobList):
        """
        _handleACDC_

        Do the ACDC creation and hope it works
        """

        collectionDict = {}

        for job in jobList:
            if not job['spec'] in collectionDict.keys():
                collectionDict[job['spec']] = []
            collectionDict[job['spec']].append(job)

        for spec in collectionDict.keys():
            
            # Load spec if we have to:
            if not spec in self.specCache.keys():
                wmWorkload = WMWorkloadHelper(WMWorkload("workload"))
                wmWorkload.load(spec)
                self.specCache[spec] = wmWorkload
                wmSpec = wmWorkload
            else:
                wmSpec = self.specCache[spec]


            self.dataCollection.createCollection(wmSpec = wmSpec)

            failedJobs = collectionDict[spec]
            self.dataCollection.failedJobs(failedJobs)

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


        fullList = []
        fullList.extend(jobList)
        fullList.extend(submitList)
        fullList.extend(createList)

        exhaustList = []

        exhaustList.extend(self.processRetries(createList, 'create'))
        exhaustList.extend(self.processRetries(submitList, 'submit'))
        exhaustList.extend(self.processRetries(jobList, 'job'))

        # Now do ACDC
        try:
            self.handleACDC(jobList = exhaustList)
        except Exception, ex:
            logging.error("ACDC threw an exception: %s" % ex)
            logging.error(str(traceback.format_exc()))

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
            myThread.transaction.begin()
            self.handleErrors()
            myThread.transaction.commit()
        except Exception, ex:
            msg = "Caught exception in ErrorHandler\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            logging.error(msg)
            if hasattr(myThread, 'transaction') \
                   and myThread.transaction != None \
                   and hasattr(myThread.transaction, 'transaction') \
                   and myThread.transaction.transaction != None:
                myThread.transaction.rollback()
            raise Exception(msg)

