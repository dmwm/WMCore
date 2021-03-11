#!/usr/bin/env python
# pylint: disable=W0613
"""
__RetryManagerPoller__

This component does the actualy retry logic. It allows to have
different algorithms for each job type.

The configuration for this component is as follows:

config.RetryManager.plugins is a dictionary with job types as keys and
plugin names as values, e.g. {'Processing' : 'SquaredAlgo'}. Job types
that don't appear in the keys of the dictionary will use the algorithm
defined for the key 'default' which defaults to DefaultRetryAlgo if
not specified.

For each plugin that appears in the values of config.RetryManager.plugins,
the params can be configured for each job type with the following syntax:

config.RetryManager.<AlgoName>.<JobType>.<ParamName> = <param>

If a particular job type doesn't appear under the corresponding algorithm
configuration, the component will use the params configured for 'default'
or the internal defaults if the default parameters aren't configured either.

An example configuration:

config.RetryManager.plugins = {'Processing' : 'PauseAlgo',
                               'Cleanup' : 'LinearAlgo',
                               'default' : 'SquaredAlgo'}
config.RetryManager.section_('PauseAlgo')
config.RetryManager.PauseAlgo.section_('Processing')
config.RetryManager.PauseAlgo.Processing.coolOffTime = {'submit' : 50, 'create' : 50, 'job' : 20}
config.RetryManager.PauseAlgo.Processing.pauseCount = 3
config.RetryManager.PauseAlgo.section_('default')
config.RetryManager.PauseAlgo.default.coolOffTime = {'submit' : 50, 'create' : 50, 'job' : 20}
config.RetryManager.PauseAlgo.default.pauseCount = 2
config.RetryManager.section_('LinearAlgo')
config.RetryManager.LinearAlgo.section_('Cleanup')
config.RetryManager.LinearAlgo.Cleanup.coolOffTime = {'submit' : 50, 'create' : 50, 'job' : 20}
config.RetryManager.LinearAlgo.section_('default')
config.RetryManager.LinearAlgo.default.coolOffTime = {'submit' : 50, 'create' : 50, 'job' : 20}
config.RetryManager.section_('SquaredAlgo')
config.RetryManager.SquaredAlgo.section_('default')
config.RetryManager.SquaredAlgo.default.coolOffTime = {'submit' : 50, 'create' : 50, 'job' : 20}

Note: It is possible to not specify any configuration at all and the
component won't crash but it won't do anything at all. All
jobs that get in cooloff would stay there forever.
Any parameter can be skipped and the component will use internal defaults.
"""

from future.utils import viewvalues

import datetime
import logging
import threading
import time
import traceback
from Utils.Timers import timeFunction
from WMCore.DAOFactory import DAOFactory
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.WMBS.Job import Job
from WMCore.WMException import WMException
from WMCore.WMFactory import WMFactory
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

__all__ = []


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


class RetryManagerException(WMException):
    """
    _RetryManagerException_

    It's totally awesome, except it's not.
    """


class RetryManagerPoller(BaseWorkerThread):
    """
    _RetryManagerPoller_

    Polls for Jobs in CoolOff State and attempts to retry them
    based on the requirements in the selected plugin
    """

    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        pluginPath = getattr(self.config.RetryManager, "pluginPath",
                             "WMComponent.RetryManager.PlugIns")
        self.pluginFactory = WMFactory("plugins", pluginPath)

        self.changeState = ChangeState(self.config)
        self.getJobs = self.daoFactory(classname="Jobs.GetAllJobs")


        # get needed plugins
        self.plugins = {}

        self.typePluginsAssoc = getattr(self.config.RetryManager, 'plugins', {})
        self.typePluginsAssoc.setdefault('default', 'DefaultRetryAlgo')

        for pluginName in viewvalues(self.typePluginsAssoc):
            try:
                plugin = self.pluginFactory.loadObject(classname=pluginName,
                                                       args=config)
                self.plugins[pluginName] = plugin
            except Exception as ex:
                msg = "Error loading plugin %s on path %s\n" % (pluginName, pluginPath)
                msg += str(ex)
                logging.error(msg)
                raise RetryManagerException(msg)

        return

    def terminate(self, params):
        """
        Run one more time through, then terminate

        """
        logging.debug("Terminating. doing one more pass before we die")
        self.algorithm(params)

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Performs the doRetries method, loading the appropriate
        plugin for each job and handling it.
        """
        logging.debug("Running retryManager algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.doRetries()
            myThread.transaction.commit()
        except WMException:
            if getattr(myThread, 'transaction', None) and \
                    getattr(myThread.transaction, 'transaction', None):
                myThread.transaction.rollback()
            raise
        except Exception as ex:
            msg = "Caught exception in RetryManager\n"
            msg += str(ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            logging.error(msg)
            if hasattr(myThread, 'transaction') \
                    and myThread.transaction is not None \
                    and hasattr(myThread.transaction, 'transaction') \
                    and myThread.transaction.transaction is not None:
                myThread.transaction.rollback()
            raise Exception(msg)

    def processRetries(self, jobs, cooloffType):
        """
        _processRetries_

        Actually does the dirty work of figuring out what to do with jobs
        """

        if len(jobs) < 1:
            # We got no jobs?
            return

        transitions = Transitions()
        oldstate = '%scooloff' % cooloffType
        if oldstate not in transitions:
            msg = 'Unknown job type %s' % cooloffType
            logging.error(msg)
            return
        propList = []

        newJobState = transitions[oldstate][0]

        jobList = self.loadJobsFromList(idList=jobs)

        # Now we should have the jobs
        propList = self.selectRetryAlgo(jobList, cooloffType)

        if len(propList) > 0:
            self.changeState.propagate(propList, newJobState, oldstate)

        return

    def loadJobsFromList(self, idList):
        """
        _loadJobsFromList_

        Load jobs in bulk
        """

        loadAction = self.daoFactory(classname="Jobs.LoadFromID")
        getTypeAction = self.daoFactory(classname="Jobs.GetType")

        binds = []
        for jobID in idList:
            binds.append({"jobid": jobID})

        results = loadAction.execute(jobID=binds)
        typeResults = getTypeAction.execute(jobID=idList)
        subTypes = {}

        for typeEntry in typeResults:
            subTypes[typeEntry['id']] = typeEntry['type']

        # You have to have a list
        if isinstance(results, dict):
            results = [results]

        listOfJobs = []
        for entry in results:
            # One job per entry
            tmpJob = Job(id=entry['id'])
            tmpJob.update(entry)
            tmpJob['jobType'] = subTypes[entry['id']]
            listOfJobs.append(tmpJob)

        return listOfJobs

    def selectRetryAlgo(self, jobList, cooloffType):
        """
        _selectRetryAlgo_

        Selects which retry algorithm to use
        """
        result = []

        if len(jobList) == 0:
            return result

        for job in jobList:
            try:
                if job['jobType'] in self.typePluginsAssoc:
                    pluginName = self.typePluginsAssoc[job['jobType']]
                else:
                    pluginName = self.typePluginsAssoc['default']
                plugin = self.plugins[pluginName]

                if plugin.isReady(job=job, cooloffType=cooloffType):
                    result.append(job)
            except Exception as ex:
                msg = "Exception while checking for cooloff timeout for job %i\n" % job['id']
                msg += str(ex)
                logging.error(msg)
                logging.debug("Job: %s\n", job)
                logging.debug("cooloffType: %s\n", cooloffType)
                raise RetryManagerException(msg)

        return result

    def doRetries(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Discover the jobs that are in create cooloff
        jobs = self.getJobs.execute(state='createcooloff')
        logging.info("Found %s jobs in createcooloff", len(jobs))
        self.processRetries(jobs, 'create')

        # Discover the jobs that are in submit cooloff
        jobs = self.getJobs.execute(state='submitcooloff')
        logging.info("Found %s jobs in submitcooloff", len(jobs))
        self.processRetries(jobs, 'submit')

        # Discover the jobs that are in run cooloff
        jobs = self.getJobs.execute(state='jobcooloff')
        logging.info("Found %s jobs in jobcooloff", len(jobs))
        self.processRetries(jobs, 'job')

        # Discover the jobs that are in paused, logging only purpose:
        jobs = self.getJobs.execute(state='jobpaused')
        logging.info("Found %s jobs in jobpaused", len(jobs))

        jobs = self.getJobs.execute(state='createpaused')
        logging.info("Found %s jobs in createpaused", len(jobs))

        jobs = self.getJobs.execute(state='submitpaused')
        logging.info("Found %s jobs in submitpaused", len(jobs))
