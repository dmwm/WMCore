#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The actual retry algorithm(s)
"""
__all__ = []
__revision__ = "$Id: RetryManagerPoller.py,v 1.3 2009/10/15 20:25:01 mnorman Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "anzar@fnal.gov"

import threading
import logging
import re
import datetime
import time
import os
import os.path
from sets import Set

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.WMFactory         import WMFactory
from WMCore.DAOFactory        import DAOFactory

from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine.Transitions import Transitions

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
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()

        self.daofactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        self.changeState = ChangeState(self.config)

        
        self.getJobs = self.daofactory(classname = "Jobs.GetAllJobs")

        return

    def terminate(self,params):
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


    def processRetries(self, jobs, type):
        """
        _processRetries_
        
        Actually does the dirty work of figuring out what to do with jobs
        """

        transitions = Transitions()
        oldstate = '%scooloff' %(type)
        if not oldstate in transitions.keys():
            print transitions.keys()
            print type
            logging.error('Unknown job type %s' %(type))
            return
        jobList  = []
        propList = []

        newJobType  = transitions[oldstate][0]
    
        for id in jobs:
            job = Job(id = id)
            job.loadData()
            jobList.append(job)

        currentTime = self.timestamp()
        #Now we should have the jobs
        propList = self.selectRetryAlgo(jobList, type)

        if len(propList) > 0:
            self.changeState.propagate(propList, newJobType, oldstate)


        return


    def selectRetryAlgo(self, jobList, type):
        """
        _selectRetryAlgo_

        Selects which retry algorithm to use
        """

        myThread = threading.currentThread()

        result = []

        if len(jobList) == 0:
            return result

        
        pluginName = self.config.RetryManager.pluginName
        if pluginName == '' or pluginName == None:
            pluginName = 'RetryAlgo'
        plugin = '%s%s' %(type.capitalize(), pluginName)
        name = '%s.%s'%(self.config.RetryManager.pluginPath, plugin)
        path = os.path.join(self.config.RetryManager.WMCoreBase, 'src/python', name.replace('.','/')) + '.py'

        if os.path.isfile(path):
            module = __import__(name, globals(), locals(), ['RetryAlgo'])
            instance = getattr(module, 'RetryAlgo')
            loadedClass = instance(self.config)
            for job in jobList:
                if loadedClass.isReady(job):
                    result.append(job)


        else:
            logging.error('Could find no module.  Am using default to determine whether cooldown has expired')
            for job in jobList:
                if self.defaultRetryAlgo(job, type):
                    result.append(job)

        return result

    def defaultRetryAlgo(self, job, type):
        """
        _defaultRetryAlgo_

        This is the default way to tell whether jobs have satisfied cooldown; they have waited for a certain
        amount of time
        """

        cooloffTime = self.config.RetryManager.coolOffTime.get(type, None)
        if not cooloffTime:
            logging.error('Unknown cooloffTime for type %s: passing' %(type))
            return

        currentTime = self.timestamp()
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




    def convertdatetime(self, t):
        return int(time.mktime(t.timetuple()))
          
    def timestamp(self):
        """
        generate a timestamp
        """
        t = datetime.datetime.now()
        return self.convertdatetime(t)
