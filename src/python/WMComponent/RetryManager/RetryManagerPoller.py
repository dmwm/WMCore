#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The actual retry algorithm(s)
"""
__all__ = []
__revision__ = "$Id: RetryManagerPoller.py,v 1.6 2010/04/02 13:03:15 mnorman Exp $"
__version__ = "$Revision: 1.6 $"

import threading
import logging
import datetime
import time
import os
import os.path

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine.Transitions import Transitions

def convertdatetime(t):
    """
    Convert dates into useable format.
    """
    return int(time.mktime(t.timetuple()))

def timestamp():
    """
    generate a timestamp
    """
    t = datetime.datetime.now()
    return convertdatetime(t)

class RetryManagerPoller(BaseWorkerThread):
    """
    Polls for Jobs in CoolOff State and attempts to retry them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        # Create a factory to load plugins
        self.pluginFactory = WMFactory("plugins",
                                       self.config.RetryManager.pluginPath)
        
        self.changeState = ChangeState(self.config)

        
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        return


    
    
    def setup(self, parameters):
        """
        Currently does nothing
        """

        return
    

    def terminate(self, params):
        """
        Run one more time through, then terminate

        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)


    def algorithm(self, parameters):
        """
	Performs the handleErrors method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.doRetries()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise


    def processRetries(self, jobs, jobType):
        """
        _processRetries_
        
        Actually does the dirty work of figuring out what to do with jobs
        """

        transitions = Transitions()
        oldstate = '%scooloff' % (jobType)
        if not oldstate in transitions.keys():
            logging.error('Unknown job type %s' % (jobType))
            return
        jobList  = []
        propList = []

        newJobType  = transitions[oldstate][0]
    
        for jid in jobs:
            job = Job(id = jid)
            job.loadData()
            jobList.append(job)

        #Now we should have the jobs
        propList = self.selectRetryAlgo(jobList, jobType)

        if len(propList) > 0:
            self.changeState.propagate(propList, newJobType, oldstate)


        return


    def selectRetryAlgo(self, jobList, jobType):
        """
        _selectRetryAlgo_

        Selects which retry algorithm to use
        """

        result = []

        if len(jobList) == 0:
            return result

        
        pluginName = self.config.RetryManager.pluginName
        if pluginName == '' or pluginName == None:
            pluginName = 'RetryAlgo'
        plugin = '%s%s' % (jobType.capitalize(), pluginName)
        name = '%s.%s' % (self.config.RetryManager.pluginPath, plugin)
        path = os.path.join(self.config.RetryManager.WMCoreBase,
                            'src/python',
                            name.replace('.','/')) + '.py'

        if not os.path.isfile(path):
            # Then we don't have the plugin
            msg = "WARNING!  RetryManager is set to use Non-Existant Plugin %s!" \
                  % (pluginName)
            logging.error(msg)
            raise Exception (msg)
            


        loadedClass = self.pluginFactory.loadObject(classname = plugin)
        loadedClass.setup(config = self.config)
        for job in jobList:
            if loadedClass.isReady(job):
                result.append(job)


        else:
            logging.error('Could find no module.  Am using default RetryAlgo')
            for job in jobList:
                if self.defaultRetryAlgo(job, jobType):
                    result.append(job)

        return result

    def defaultRetryAlgo(self, job, jobType):
        """
        _defaultRetryAlgo_

        This is the default way to tell whether jobs have satisfied cooldown; they have waited for a certain
        amount of time
        """

        cooloffTime = self.config.RetryManager.coolOffTime.get(jobType, None)
        if not cooloffTime:
            logging.error('Unknown cooloffTime for type %s: passing' %(jobType))
            return

        currentTime = timestamp()
        if currentTime - job['state_time'] > cooloffTime:
            return True
        else:
            return False


    def doRetries(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover the jobs that are in create cooloff
        jobs = self.getJobs.execute(state = 'createcooloff')
        logging.debug("Found %s jobs in createcooloff" % len(jobs))
        self.processRetries(jobs, 'create')

         # Discover the jobs that are in submit cooloff
        jobs = self.getJobs.execute(state = 'submitcooloff')
        logging.debug("Found %s jobs in submitcooloff" % len(jobs))
        self.processRetries(jobs, 'submit')

	# Discover the jobs that are in run cooloff
        jobs = self.getJobs.execute(state = 'jobcooloff')
        logging.debug("Found %s jobs in jobcooloff" % len(jobs))
        self.processRetries(jobs, 'job')





