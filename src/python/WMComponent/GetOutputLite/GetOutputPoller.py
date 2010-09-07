#!/usr/bin/env python
#pylint: disable-msg=E1103
# E1103: The thread will have a logger and a dbi before it gets here

"""
The actual GetOutputLite poller algorithm
"""

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.DAOFactory        import DAOFactory

from WMCore.ProcessPool.ProcessPool        import ProcessPool

class GetOutputPoller(BaseWorkerThread):
    """
    Polls for jobs in new process_status or to notify as finished
    """

    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        self.config = config

        self.myThread = threading.currentThread()
        logging.info("Thread: [%s]" %str(self.myThread ))

        self.daoFactory = DAOFactory(package = "BossLite",
                                     logger = self.myThread.logger,
                                     dbinterface = self.myThread.dbi)
        self.getjobs    = self.daoFactory(classname = "Job.LoadForOutput")

        self.processPool = ProcessPool(
                      "GetOutputLite.OutputWorker", 
                      self.config.GetOutputLite.processes, 
                      componentDir = self.config.GetOutputLite.componentDir, 
                      config = self.config, slaveInit = {} 
                    )

	    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        return

    def terminate(self, params):
        """
        _terminate_

        Terminate the function after one more run.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return

    def algorithm(self, parameters):
        """
	Performs the archiveJobs method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Thread [%s]"%str(self.myThread))

        ## processing done and aborted jobs..finished jobs!
        for statme in ['SD', 'A']:
            logging.info('Processing "%s" status...'%statme)
            ## load task / job id with a dao
            jobdata = self.getjobs.execute( statme,
                                           self.config.GetOutputLite.loadlimit
                                          )
            ##   group jobs by task
            tasklist = []
            task = { 'taskId': None, 'status': statme, 'joblist': [] }
            for entry in jobdata:
                if entry['taskId'] == task['taskId']:  
                    task['joblist'].append(entry['jobId'])
                else:
                    if task['taskId'] is not None:
                        tasklist.append(task)
                    task = { 'taskId': entry['taskId'],
                             'status': statme,
                             'joblist': [entry['jobId']] } 
            ## add last task if not yet on the working list 
            if task not in tasklist and task['taskId'] is not None:
                tasklist.append(task)
            if len(tasklist) > 0:
                logging.info("Having %i work to enqueue..."%len(tasklist))
                #logging.info(str(tasklist))
                ##     give work to the subprocess
                for t in tasklist:
                    self.processPool.enqueue( [t] )
                ##     get back the work from the subprocess
                result = self.processPool.dequeue(len(tasklist))
                logging.debug(str(result))
                logging.info("...work done!")
            else:
                logging.info('No jobs to process!')
        
        return

