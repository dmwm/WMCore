#!/usr/bin/env python
"""
_BossAirAPI_

The interface for BossAir


Notes on convention:

BossAirAPI talks both inside and outside of the BossAir application family.
Interfaces geared toward the outside expect WMBS objects.  Interfaces
geared toward the inside expect RunJob objects.  Interior interfaces are
marked by names starting with '_' such as '_listRunning'
"""

__version__ = "$Id: BossLiteAPI.py,v 1.14 2010/06/28 19:05:14 spigafi Exp $"
__revision__ = "$Revision: 1.14 $"

import threading
import logging

from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory

from WMCore.BossAir.RunJob   import RunJob

from WMCore.WMConnectionBase import WMConnectionBase

from WMCore.WMException import WMException


class BossAirException(WMException):
    """
    I expect you'll be seeing this a lot

    """


class BossAirAPI(WMConnectionBase):
    """
    _BossAirAPI_

    The API layer for the BossAir prototype
    """



    def __init__(self, config):
        """
        __init__

        BossAir should work with the standard config
        structure of WMAgent
        """

        myThread = threading.currentThread()

        self.config  = config
        self.plugins = {}
        self.states  = []

        self.jobs    = []

        self.pluginDir  = config.BossAir.pluginDir
        # This is the default state jobs are created in
        self.newState   = getattr(config.BossAir, 'newState', 'New')


        # Create a factory to load plugins
        self.pluginFactory = WMFactory("plugins", self.pluginDir)

        self.daoFactory = DAOFactory(package = "WMCore.BossAir",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)


        self.deleteDAO     = self.daoFactory(classname = "DeleteJobs")
        self.stateDAO      = self.daoFactory(classname = "NewState")
        self.loadByWMBSDAO = self.daoFactory(classname = "LoadByWMBSID")
        self.updateDAO     = self.daoFactory(classname = "UpdateJobs")
        self.newJobDAO     = self.daoFactory(classname = "NewJobs")
        self.runningJobDAO = self.daoFactory(classname = "LoadRunning")
        self.loadJobsDAO   = self.daoFactory(classname = "LoadByStatus")
        self.completeDAO   = self.daoFactory(classname = "CompleteJob")
                                

        self.loadPlugin()

        return



    def loadPlugin(self):
        """
        _loadPlugin_

        Actually load the plugin and init the database
        """

        states = set()

        for name in self.config.BossAir.pluginNames:
            self.plugins[name] = self.pluginFactory.loadObject(classname = name,
                                                               args = self.config)
            for state in self.plugins[name].states:
                states.add(state)

        states = list(states)

        if not self.newState in states:
            states.append(self.newState)

        self.addStates(states = states)
        self.states = states

        return


    def addStates(self, states):
        """
        _addStates_
        
        Add States to bl_status table
        """
        existingTransaction = self.beginTransaction()


        self.stateDAO.execute(states = states, conn = self.getDBConn(),
                              transaction = self.existingTransaction())

        self.commitTransaction(existingTransaction)


        return

    def createNewJobs(self, wmbsJobs):
        """
        _createNewJobs_

        Create new jobs in the BossAir database
        Accepts WMBS Jobs
        """

        existingTransaction = self.beginTransaction()

        jobsToCreate = []

        # First turn wmbsJobs into runJobs
        for wmbsJob in wmbsJobs:
            runJob = RunJob()
            runJob.buildFromJob(job = wmbsJob)
            if not runJob.get('status', None):
                runJob['status'] = self.newState
            jobsToCreate.append(runJob)

        # Next insert them into the database
        self.newJobDAO.execute(jobs = jobsToCreate, conn = self.getDBConn(),
                               transaction = self.existingTransaction())  

        self.commitTransaction(existingTransaction)

        return


    def _listRunning(self):
        """
        _listRunningJobs_

        List all the currently running jobs
        """

        existingTransaction = self.beginTransaction()


        runningJobDicts = self.runningJobDAO.execute(conn = self.getDBConn(),
                                                     transaction = self.existingTransaction())

        runJobs = []
        for jDict in runningJobDicts:
            rj = RunJob()
            rj.update(jDict)
            runJobs.append(rj)

        self.commitTransaction(existingTransaction)

        return runJobs




    def _loadByStatus(self, status):
        """
        _loadByStatus_

        Load jobs by status
        """

        if status not in self.states:
            msg =  "Asked to load by status %s which is not loaded\n" % (status)
            msg += "This indicates that the wrong plugins are loaded\n"
            logging.error(msg)
            raise BossAirException(msg)


        existingTransaction = self.beginTransaction()


        loadJobs = self.loadJobsDAO.execute(status = status, conn = self.getDBConn(),
                                            transaction = self.existingTransaction())

        statusJobs = []
        for jDict in loadJobs:
            rj = RunJob()
            rj.update(jDict)
            statusJobs.append(rj)

        self.commitTransaction(existingTransaction)

        return statusJobs


    def _loadByID(self, jobs):
        """
        _loadByID_

        Load by running Job ID
        """
        existingTransaction = self.beginTransaction()

        loadJobsDAO = self.daoFactory(classname = "LoadByID")
        loadJobs = loadJobsDAO.execute(jobs = jobs, conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

        loadedJobs = []
        for jDict in loadJobs:
            rj = RunJob()
            rj.update(jDict)
            loadedJobs.append(rj)
            cache = {'id': rj['jobid'], 'plugin': rj['plugin'], 'user': rj['user']}
            if not cache in self.jobs:
                self.jobs.append(cache)

        self.commitTransaction(existingTransaction)

        return loadedJobs


    def _completeJobs(self, jobs):
        """
        _completeJobs_
        
        Complete jobs in the database
        Expects runJob input
        """

        idList = [x['id'] for x in jobs]

        existingTransaction = self.beginTransaction()

        completeDAO = self.daoFactory(classname = "CompleteJob")
        completeDAO.execute(jobs = idList, conn = self.getDBConn(),
                            transaction = self.existingTransaction())  

        self.commitTransaction(existingTransaction)

        return

    def _updateJobs(self, jobs):
        """
        _updateJobs_

        Update the job entries in the BossAir database
        """

        existingTransaction = self.beginTransaction()


        self.updateDAO.execute(jobs = jobs, conn = self.getDBConn(),
                               transaction = self.existingTransaction())  

        self.commitTransaction(existingTransaction)

        return

    def _deleteJobs(self, jobs):
        """
        _deleteJobs_

        Delete the job entries in the BossAir database
        """

        idList = [x['id'] for x in jobs]

        existingTransaction = self.beginTransaction()

        self.deleteDAO.execute(jobs = idList, conn = self.getDBConn(),
                               transaction = self.existingTransaction())  

        self.commitTransaction(existingTransaction)

        return

    def loadByWMBS(self, wmbsJobs, addToCache = False):
        """
        _loadByWMBS_

        Load BossAir info based on wmbs Jobs.
        """

        existingTransaction = self.beginTransaction()

        jobList = self.loadByWMBSDAO.execute(jobs = wmbsJobs)

        loadedJobs = []
        for job in jobList:
            rj = RunJob()
            rj.update(job)
            loadedJobs.append(rj)
            if addToCache:
                self.jobs.append({'id': rj['jobid'], 'plugin': rj['plugin'],
                                  'user': rj['user']})
        
        self.commitTransaction(existingTransaction)
        

        return loadedJobs

    def check(self):
        """
        _check_

        Perform checks of critical components, i.e. proxy validation, etc.
        """

        return



    def submit(self, jobs):
        """
        _submit_

        Submit jobs using the plugin

        Requires both plugin name and workflow user from submitter

        Deals internally in RunJob objects, but interfaces
        to the outside with WMBS Job analogs

        Returns (successes, failures)
        """

        self.check()

        successJobs  = []
        failureJobs  = []


        #TODO: Add plugin and user to input via JobSubmitter
        # IMPORTANT IMPORTANT IMPORTANT

        # Put job into RunJob format
        runJobs = []
        for job in jobs:
            rj = RunJob()
            rj.buildFromJob(job = job)
            runJobs.append(rj)
            self.jobs.append({'id': rj['jobid'], 'plugin': rj['plugin'], 'user': rj['user']})

        # Now figure out which plugin we need
        pluginDict = {}
        for job in runJobs:
            plugin = job['plugin']
            if not plugin in pluginDict.keys():
                pluginDict[plugin] = []
            pluginDict[plugin].append(job)

        for plugin in pluginDict.keys():
            if not plugin in self.plugins.keys():
                # Then we have a non-existant plugin
                msg =  "Given a plugin we don't have access to %s\n" % (plugin)
                msg += "Ignoring the jobs for this plugin for now"
                logging.error(msg)
            else:
                localSuccess, localFailure = self.plugins.submit(pluginDict.get(plugin))
                for job in localSuccess:
                    successJobs.append(job.buildWMBSJob())
                for job in localFailure:
                    failureJobs.append(job.buildWMBSJob())

        return successJobs, failureJobs



    def track(self):
        """
        _track_
        
        Track all running jobs
        Load job info from the cache (it should be there since we submitted the job)
        """

        jobsToLoad     = []
        jobsToChange   = []
        loadedJobs     = []
        jobsToComplete = []

        jobsToTrack = {}

        runningJobs = self._listRunning()

        for runningJob in runningJobs:
            foundJob = False
            for jCache in self.jobs:
                if jCache['id'] == runningJob['jobid']:
                    foundJob = True
                    runningJob['user'] = jCache['user'] 
                    loadedJobs.append(runningJob)
                    break
            if not foundJob:
                # Then we have a job that didn't enter through submit
                # How the hell did that happen?
                # Are we recovering?
                jobsToLoad.append(job)
            
        loadedJobs.extend(self.loadByWMBS(wmbsJobs = jobsToLoad, addToCache = True))
        
        for runningJob in loadedJobs:
            plugin = runningJob['plugin']
            if not plugin in jobsToTrack.keys():
                jobsToTrack[plugin] = []
            jobsToTrack[plugin].append(runningJob)

        for plugin in jobsToTrack.keys():
            if not plugin in self.plugins.keys():
                msg =  "Jobs tracking with non-existant plugin %s" % (plugin)
                msg += "They were submitted but can't be tracked?"
                msg += "That's too strange to continue"
                logging.error(msg)
                raise BossAirException(msg)
            else:
                # Then we send them to the plugins
                # Shoudl give you a lit of jobs to change and jobs to complete
                localChanges, localCompletes = self.plugins.track(jobsToTrack[plugin])
                jobsToChange.extend(localChanges)
                jobsToComplete.extend(localCompletes)

        self._updateJobs(jobs = jobsToChange)
        self._complete(jobs = jobsToComplete)

        return

    def _complete(self, jobs):
        """
        _complete_
        
        Complete jobs using plugin functions
        Requires jobs in RunJob format
        """

        # We should be insulated from bad plugins by track()
        jobsToComplete = {}

        for job in jobs:
            if not job['plugin'] in jobsToComplete.keys():
                jobsToComplete[job['plugin']] = []
            jobsToComplete[job['plugin']].append(job)

        for plugin in jobsToComplete.keys():
            self.plugins[plugin].complete(jobsToComplete[plugin])

        existingTransaction = self.beginTransaction()

        self.completeDAO.execute(jobs = idList)    

        self.commitTransaction(existingTransaction)

        return



    def kill(self, jobs):
        """
        _kill_
        
        Kill jobs using plugin functions
        """


        self.check()






        

        

        
        
