#!/usr/bin/env python
#pylint: disable-msg=E1103, W6501, E1101, C0103
#E1103: Use DB objects attached to thread
#W6501: Allow string formatting in error messages
#E1101: Create config sections
#C0103: Internal methods start with _
"""
_BossAirAPI_

The interface for BossAir


Notes on convention:

BossAirAPI talks both inside and outside of the BossAir application family.
Interfaces geared toward the outside expect WMBS objects.  Interfaces
geared toward the inside expect RunJob objects.  Interior interfaces are
marked by names starting with '_' such as '_listRunning'
"""

import threading
import logging
import subprocess

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

        WMConnectionBase.__init__(self, daoPackage = "WMCore.BossAir")

        myThread = threading.currentThread()

        self.config  = config
        self.plugins = {}
        self.states  = []

        self.jobs    = []

        self.pluginDir  = config.BossAir.pluginDir
        # This is the default state jobs are created in
        self.newState   = getattr(config.BossAir, 'newState', 'New')

        # Get any proxy info
        self.checkProxy = getattr(config.BossAir, 'checkProxy', False)
        self.cert       = getattr(config.BossAir, 'cert', None)


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
        self.monitorDAO    = self.daoFactory(classname = "LoadForMonitoring")
                                

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


    def _listRunning(self, complete = '1'):
        """
        _listRunningJobs_

        List all the currently running jobs
        """

        existingTransaction = self.beginTransaction()


        runningJobDicts = self.runningJobDAO.execute(complete = complete,
                                                     conn = self.getDBConn(),
                                                     transaction = self.existingTransaction())

        runJobs = []
        for jDict in runningJobDicts:
            rj = RunJob()
            rj.update(jDict)
            runJobs.append(rj)

        self.commitTransaction(existingTransaction)

        return runJobs




    def _loadByStatus(self, status, complete = '1'):
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


        loadJobs = self.loadJobsDAO.execute(status = status,
                                            complete = complete,
                                            conn = self.getDBConn(),
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
            if not rj in self.jobs:
                self.jobs.append(rj)

        self.commitTransaction(existingTransaction)

        return loadedJobs


    def _completeJobs(self, jobs):
        """
        _completeJobs_
        
        Complete jobs in the database
        Expects runJob input
        """

        if len(jobs) < 1:
            # Nothing to do
            return

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

        if len(jobs) < 1:
            # Nothing to do
            return

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

        if len(jobs) < 1:
            # Nothing to do
            return

        idList = [x['id'] for x in jobs]

        existingTransaction = self.beginTransaction()

        self.deleteDAO.execute(jobs = idList, conn = self.getDBConn(),
                               transaction = self.existingTransaction())  

        self.commitTransaction(existingTransaction)


        jobsToRemove = []
        for jCache in self.jobs:
            if jCache['id'] in idList:
                jobsToRemove.append(jCache)

        for jCache in jobsToRemove:
            self.jobs.remove(jCache)

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
                self.jobs.append(rj)
        
        self.commitTransaction(existingTransaction)

        if not len(loadedJobs) == len(wmbsJobs):
            logging.error("Mismatch in WMBS load: Some requested jobs not found!")
            idList = [x['jobid'] for x in loadedJobs]
            for job in wmbsJobs:
                if not job['id'] in idList:
                    logging.error("Could not retrieve job with WMBS ID %i from BossAir database" % (job['id']))
                    
        
        

        return loadedJobs

    def check(self):
        """
        _check_

        Perform checks of critical components, i.e. proxy validation, etc.
        """

        if self.checkProxy:
            command = 'voms-proxy-info'
            if self.cert is not None and self.cert != '' :
                command += ' --file ' + self.cert

            pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            output, err = pipe.communicate()

            try:
                output = output.split("timeleft  :")[1].strip()
            except IndexError:
                raise BossAirException("Missing Proxy", output.strip())
            
            if output == "0:00:00":
                raise BossAirException("Proxy Expired", output.strip())

        return



    def submit(self, jobs, info = None):
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
            if not job.get('location', False):
                rj['location'] = job.get('custom', {}).get('location', None)
            runJobs.append(rj)
            # Can't add to the cache in submit()
            # It's NOT the same bossAir instance
            #self.jobs.append(rj)

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
                msg =  "CRITICAL ERROR: Non-existant plugin!\n"
                msg += "Given a plugin %s that we don't have access to.\n" % (plugin)
                msg += "Ignoring the jobs for this plugin for now"
                logging.error(msg)
            else:
                try:
                    pluginInst   = self.plugins[plugin]
                    jobsToSubmit = pluginDict.get(plugin, [])
                    localSuccess, localFailure = pluginInst.submit(jobs = jobsToSubmit,
                                                               info = info)
                    for job in localSuccess:
                        successJobs.append(job.buildWMBSJob())
                        for job in localFailure:
                            failureJobs.append(job.buildWMBSJob())
                except WMException:
                    raise
                except Exception, ex:
                    msg =  "Unhandled exception while submitting jobs to plugin %s\n" % plugin
                    msg += str(ex)
                    logging.error(msg)
                    logging.debug("Jobs being submitted: %s\n" % (jobsToSubmit))
                    logging.debug("Job info: %s\n" % (info))
                    raise BossAirException(msg)

        # Create successful jobs in BossAir
        self.createNewJobs(wmbsJobs = successJobs)

        return successJobs, failureJobs



    def track(self, runJobIDs = None, wmbsIDs = None):
        """
        _track_
        
        Track all running jobs
        Load job info from the cache (it should be there since we submitted the job)

        OPTIONAL: You can submit a list of jobs to check, based either on wmbsIDs or
         on runjobIDs.  This takes a list of integer IDs.
        """

        jobsToChange   = []
        loadedJobs     = []
        jobsToComplete = []
        jobsToReturn   = []
        returnList     = []

        jobsToTrack = {}

        runningJobs = self._listRunning()

        if runJobIDs:
            for job in runningJobs:
                if not job['id'] in runJobIDs:
                    runningJobs.remove(job)
        if wmbsIDs:
            for job in runningJobs:
                if not job['jobid'] in wmbsIDs:
                    runningJobs.remove(job)
                
            

        if len(runningJobs) < 1:
            # Then we have no running jobs
            return returnList


        loadedJobs = self._buildRunningJobs(wmbsJobs = runningJobs, doRunJobs = True)
        
        for runningJob in loadedJobs:
            plugin = runningJob['plugin']
            if not plugin in jobsToTrack.keys():
                jobsToTrack[plugin] = []
            jobsToTrack[plugin].append(runningJob)

        for plugin in jobsToTrack.keys():
            if not plugin in self.plugins.keys():
                msg =  "Jobs tracking with non-existant plugin %s\n" % (plugin)
                msg += "They were submitted but can't be tracked?\n"
                msg += "That's too strange to continue\n"
                logging.error(msg)
                raise BossAirException(msg)
            try:
                # Then we send them to the plugins
                # Should give you a lit of jobs to change and jobs to complete
                pluginInst = self.plugins[plugin]
                localRunning, localChanges, localCompletes = pluginInst.track(jobs = jobsToTrack[plugin])
                jobsToReturn.extend(localRunning)
                jobsToChange.extend(localChanges)
                jobsToComplete.extend(localCompletes)
            except WMException:
                raise
            except Exception, ex:
                msg =  "Unhandled Exception while tracking jobs for plugin %s!\n" % plugin
                msg += str(ex)
                logging.error(msg)
                logging.debug("JobsToTrack: %s" % (jobsToTrack[plugin]))
                raise BossAirException(msg)

        self._updateJobs(jobs = jobsToChange)
        self._complete(jobs = jobsToComplete)


        # We should have a globalState variable for changed jobs
        # from the plugin
        # Return that to the calling function
        for rj in jobsToReturn:
            job = rj.buildWMBSJob()
            job['globalState'] = rj['globalState']
            returnList.append(job)

        return returnList

    def _complete(self, jobs):
        """
        _complete_
        
        Complete jobs using plugin functions
        Requires jobs in RunJob format
        """

        if len(jobs) < 1:
            # Nothing to do
            return

        # We should be insulated from bad plugins by track()
        jobsToComplete = {}
        idsToComplete  = []

        for job in jobs:
            if not job['plugin'] in jobsToComplete.keys():
                jobsToComplete[job['plugin']] = []
            jobsToComplete[job['plugin']].append(job)
            idsToComplete.append(job['id'])

        for plugin in jobsToComplete.keys():
            try:
                self.plugins[plugin].complete(jobsToComplete[plugin])
            except WMException:
                raise
            except Exception, ex:
                msg =  "Exception while completing jobs for plugin %s!\n" % plugin
                msg += str(ex)
                logging.error(msg)
                logging.debug("JobsToComplete: %s" % (jobsToComplete[plugin]))
                raise BossAirException(msg)

        existingTransaction = self.beginTransaction()

        self.completeDAO.execute(jobs = idsToComplete)    

        self.commitTransaction(existingTransaction)

        return


    def getComplete(self):
        """
        _getComplete_
        
        The tracker should call this: It's only
        interested in the jobs that are completed.
        """

        completeJobs = []

        completeRunJobs = self._listRunning(complete = '0')

        for rj in completeRunJobs:
            job = rj.buildWMBSJob()
            completeJobs.append(job)

        return completeJobs


    def removeComplete(self, jobs):
        """
        _removeComplete_

        Remove jobs from BossAir
        This is meant to operate ONLY on completed jobs
        It does not operate any plugin cleanup or
        batch system kills
        """

        jobsToRemove = self._buildRunningJobs(wmbsJobs = jobs)

        self._deleteJobs(jobs = jobsToRemove)

        return



    def kill(self, jobs):
        """
        _kill_
        
        Kill jobs using plugin functions
        """

        if len(jobs) < 1:
            # Nothing to do here
            return

        self.check()

        jobsToKill = {}

        # Now get a list of which jobs are running
        # ONLY kill running jobs
        loadedJobs = self._buildRunningJobs(wmbsJobs = jobs)


        
        for runningJob in loadedJobs:
            plugin = runningJob['plugin']
            if not plugin in jobsToKill.keys():
                jobsToKill[plugin] = []
            jobsToKill[plugin].append(runningJob)

        for plugin in jobsToKill.keys():
            if not plugin in self.plugins.keys():
                msg =  "Jobs tracking with non-existant plugin %s\n" % (plugin)
                msg += "They were submitted but can't be tracked?\n"
                msg += "That's too strange to continue\n"
                logging.error(msg)
                raise BossAirException(msg)
            else:
                # Then we send them to the plugins
                # Shoudl give you a lit of jobs to change and jobs to complete
                pluginInst = self.plugins[plugin]
                pluginInst.kill(jobs = jobsToKill[plugin])
                self._complete(jobs = jobsToKill[plugin])


        return



    def update(self, jobs):
        """
        _update_

        Overwrite the database with whatever you put into
        this function.
        """

        runJobs = self._buildRunningJobs(wmbsJobs = jobs)

        self._updateJobs(jobs = runJobs)

        return



    def monitor(self):
        """
        _monitor_

        Initiate the call to the monitoring DAO
        This should not be called by the standard Submitter/Status/Tracker
        system.  It is meant for outside calling.
        """


        existingTransaction = self.beginTransaction()


        results = self.monitorDAO.execute(conn = self.getDBConn(),
                                          transaction = self.existingTransaction())  

        self.commitTransaction(existingTransaction)


        return results



    def _buildRunningJobs(self, wmbsJobs, doRunJobs = False):
        """
        _buildRunningJobs_
        
        This is a utility function that exists to build a runJob set,
        first by looking in the cache, then by loading it and
        putting it in the cache.

        This is to unify the places where we have to deal with WMBS
        input.

        You can also use this to load running jobs from the cache automatically
        """

        jobsToLoad = []
        loadedJobs = []

        if doRunJobs:
            # Then overwrite the id with the jobid so you
            # can use the same search mechanism and load mechanism
            # This makes it structurally identical to a wmbs job
            for wmbsJob in wmbsJobs:
                wmbsJob['id'] = wmbsJob['jobid']

        for wmbsJob in wmbsJobs:
            foundJob = False
            for jCache in self.jobs:
                if jCache['jobid'] == wmbsJob['id'] and jCache['retry_count'] == wmbsJob['retry_count']:
                    rj = RunJob()
                    rj.buildFromJob(wmbsJob)
                    # Overwrite empty keys
                    for key in rj.keys():
                        if rj[key] == None:
                            rj[key] = jCache.get(key, None)
                    foundJob = True
                    loadedJobs.append(rj)

                    # Update the cache
                    self.jobs.remove(jCache)
                    self.jobs.append(rj)
                    
                    break

                
            if not foundJob:
                # Then we have a job that didn't enter through submit
                # How the hell did that happen?
                # Are we recovering?
                jobsToLoad.append(wmbsJob)


        if len(jobsToLoad) > 0:
            # Then we have jobs to load
            loadedJobs.extend(self.loadByWMBS(wmbsJobs = jobsToLoad, addToCache = True))
            


        return loadedJobs


        


        









        

        

        
        
