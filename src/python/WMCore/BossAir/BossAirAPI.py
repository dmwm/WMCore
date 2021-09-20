#!/usr/bin/env python
# pylint: disable=E1103, E1101, C0103
# E1103: Use DB objects attached to thread
# E1101: Create config sections
# C0103: Internal methods start with _
"""
_BossAirAPI_

The interface for BossAir


Notes on convention:

BossAirAPI talks both inside and outside of the BossAir application family.
Interfaces geared toward the outside expect WMBS objects.  Interfaces
geared toward the inside expect RunJob objects.  Interior interfaces are
marked by names starting with '_' such as '_listRunning'
"""
import os.path
import threading
import logging
import subprocess
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.BossAir.RunJob import RunJob
from WMCore.WMConnectionBase import WMConnectionBase
from WMCore.WMException import WMException
from WMCore.FwkJobReport.Report import Report
from WMCore.WMExceptions import WM_JOB_ERROR_CODES


class BossAirException(WMException):
    """
    I expect you'll be seeing this a lot

    """


class BossAirAPI(WMConnectionBase):
    """
    _BossAirAPI_

    The API layer for the BossAir prototype
    """

    def __init__(self, config, insertStates=False):
        """
        __init__

        BossAir should work with the standard config
        structure of WMAgent
        """

        WMConnectionBase.__init__(self, daoPackage="WMCore.BossAir")

        myThread = threading.currentThread()

        self.config = config
        self.plugins = {}
        self.states = []

        self.jobs = []

        self.pluginDir = config.BossAir.pluginDir
        # This is the default state jobs are created in
        self.newState = getattr(config.BossAir, 'newState', 'New')

        # Get any proxy info
        self.checkProxy = getattr(config.BossAir, 'checkProxy', False)
        self.cert = getattr(config.BossAir, 'cert', None)

        self.stateMachine = ChangeState(self.config)

        # Create a factory to load plugins
        self.pluginFactory = WMFactory("plugins", self.pluginDir)

        self.daoFactory = DAOFactory(package="WMCore.BossAir",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.deleteDAO = self.daoFactory(classname="DeleteJobs")
        self.stateDAO = self.daoFactory(classname="NewState")
        self.loadByWMBSDAO = self.daoFactory(classname="LoadByWMBSID")
        self.updateDAO = self.daoFactory(classname="UpdateJobs")
        self.newJobDAO = self.daoFactory(classname="NewJobs")
        self.runningJobDAO = self.daoFactory(classname="LoadRunning")
        self.completeJobDAO = self.daoFactory(classname="LoadComplete")
        self.loadJobsDAO = self.daoFactory(classname="LoadByStatus")
        self.completeDAO = self.daoFactory(classname="CompleteJob")
        self.monitorDAO = self.daoFactory(classname="JobStatusForMonitoring")

        self.states = None
        self.loadPlugin(insertStates)

        return

    def loadPlugin(self, insertStates):
        """
        _loadPlugin_

        Actually load the plugin and init the database
        """

        states = set()

        for name in self.config.BossAir.pluginNames:
            self.plugins[name] = self.pluginFactory.loadObject(classname=name,
                                                               args=self.config)
            for state in self.plugins[name].states:
                states.add(state)

        if self.newState not in states:
            states.add(self.newState)

        if insertStates:
            # Add states only if we're not
            # doing a secondary instantiation
            self.addStates(states=states)

        self.states = states

        return

    def addStates(self, states):
        """
        _addStates_

        Add States to bl_status table. Meant to be done only
        once in an agent lifetime.
        """
        existingTransaction = self.beginTransaction()

        self.stateDAO.execute(states=states, conn=self.getDBConn(),
                              transaction=self.existingTransaction())

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
            runJob.buildFromJob(job=wmbsJob)
            if runJob.get('status') not in self.states:
                runJob['status'] = self.newState
            jobsToCreate.append(runJob)

        # Next insert them into the database
        self.newJobDAO.execute(jobs=jobsToCreate, conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return

    def _listRunJobs(self, active=True):
        """
        _listRunJobs_

        List runjobs, either active or complete
        """

        if active:
            runJobDicts = self.runningJobDAO.execute(conn=self.getDBConn(),
                                                     transaction=self.existingTransaction())
        else:
            runJobDicts = self.completeJobDAO.execute(conn=self.getDBConn(),
                                                      transaction=self.existingTransaction())

        runJobs = []
        for jDict in runJobDicts:
            rj = RunJob()
            rj.update(jDict)
            runJobs.append(rj)

        return runJobs

    def _loadByStatus(self, status, complete='1'):
        """
        _loadByStatus_

        Load jobs by status
        """

        if status not in self.states:
            msg = "Asked to load by status %s which is not loaded\n" % (status)
            msg += "This indicates that the wrong plugins are loaded\n"
            logging.error(msg)
            raise BossAirException(msg)

        loadJobs = self.loadJobsDAO.execute(status=status,
                                            complete=complete,
                                            conn=self.getDBConn(),
                                            transaction=self.existingTransaction())
        statusJobs = []
        for jDict in loadJobs:
            rj = RunJob()
            rj.update(jDict)
            statusJobs.append(rj)

        return statusJobs

    def _loadByID(self, jobs):
        """
        _loadByID_

        Load by running Job ID
        """
        loadJobsDAO = self.daoFactory(classname="LoadByID")
        loadJobs = loadJobsDAO.execute(jobs=jobs, conn=self.getDBConn(),
                                       transaction=self.existingTransaction())

        loadedJobs = []
        for jDict in loadJobs:
            rj = RunJob()
            rj.update(jDict)
            loadedJobs.append(rj)

        return loadedJobs

    def _updateJobs(self, jobs):
        """
        _updateJobs_

        Update the job entries in the BossAir database
        """

        if len(jobs) < 1:
            # Nothing to do
            return

        existingTransaction = self.beginTransaction()

        self.updateDAO.execute(jobs=jobs, conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        jobsWithLocation = [job for job in jobs if job.get('location') is not None]
        if jobsWithLocation:
            self.stateMachine.recordLocationChange(jobsWithLocation)

        self.commitTransaction(existingTransaction)

        return

    def _deleteJobs(self, jobs):
        """
        _deleteJobs_

        Delete the job entries in the BossAir database
        NOTE: only used by unit tests
        """

        if len(jobs) < 1:
            # Nothing to do
            return

        idList = [x['id'] for x in jobs]

        existingTransaction = self.beginTransaction()

        self.deleteDAO.execute(jobs=idList, conn=self.getDBConn(),
                               transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return

    def loadByWMBS(self, wmbsJobs):
        """
        _loadByWMBS_

        Load BossAir info based on wmbs Jobs.
        """

        if len(wmbsJobs) < 1:
            return []

        jobList = self.loadByWMBSDAO.execute(jobs=wmbsJobs, conn=self.getDBConn(),
                                             transaction=self.existingTransaction())

        loadedJobs = []
        for job in jobList:
            rj = RunJob()
            rj.buildFromJob(job)
            loadedJobs.append(rj)

        if len(loadedJobs) != len(wmbsJobs):
            logging.error("Could not load all jobs in BossAir for WMBS input!")
            idList = [x['jobid'] for x in loadedJobs]
            for job in wmbsJobs:
                if job['id'] not in idList:
                    logging.error("Failed to retrieve wmbs_id %i and WMBS job info: %s", job['id'], job)

        return loadedJobs

    def check(self):
        """
        _check_

        Perform checks of critical components, i.e. proxy validation, etc.
        """

        if self.checkProxy:
            command = 'voms-proxy-info'
            if self.cert is not None and self.cert != '':
                command += ' --file ' + self.cert

            pipe = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, dummyerr = pipe.communicate()

            try:
                output = output.split("timeleft  :")[1].strip()
            except IndexError:
                raise BossAirException("Missing Proxy", output.strip())

            if output == "0:00:00":
                raise BossAirException("Proxy Expired", output.strip())

        return

    def submit(self, jobs, info=None):
        """
        _submit_

        Submit jobs using the plugin

        Requires both plugin name and workflow user from submitter

        Deals internally in RunJob objects, but interfaces
        to the outside with WMBS Job analogs

        Returns (successes, failures)
        """
        self.check()

        successJobs = []
        failureJobs = []

        # TODO: Add plugin and user to input via JobSubmitter
        # IMPORTANT IMPORTANT IMPORTANT

        # Put job into RunJob format
        pluginDict = {}

        for job in jobs:
            rj = RunJob()
            rj.buildFromJob(job=job)
            if not job.get('location', False):
                rj['location'] = job.get('custom', {}).get('location', None)
            plugin = rj['plugin']
            pluginDict.setdefault(plugin, [])
            pluginDict[plugin].append(rj)
            # Can't add to the cache in submit()
            # It's NOT the same bossAir instance
            # self.jobs.append(rj)

        for plugin in list(pluginDict):  # .clear() makes pluginDict change size during iteration!
            if plugin not in self.plugins:
                # Then we have a non-existant plugin
                msg = "CRITICAL ERROR: Non-existant plugin!\n"
                msg += "Given a plugin %s that we don't have access to.\n" % (plugin)
                msg += "Ignoring the jobs for this plugin for now"
                logging.error(msg)
                continue
            try:
                pluginInst = self.plugins[plugin]
                jobsToSubmit = pluginDict.get(plugin, [])
                logging.debug("About to submit %i jobs to plugin %s", len(jobsToSubmit), plugin)
                localSuccess, localFailure = pluginInst.submit(jobs=jobsToSubmit,
                                                               info=info)
                for job in localSuccess:
                    successJobs.append(job.buildWMBSJob())
                for job in localFailure:
                    failureJobs.append(job.buildWMBSJob())
            except WMException:
                raise
            except Exception as ex:
                msg = "Unhandled exception while submitting jobs to plugin: %s\n" % plugin
                msg += str(ex)
                logging.error(msg)
                logging.debug("Jobs being submitted: %s\n", jobsToSubmit)
                logging.debug("Job info: %s\n", info)
                raise BossAirException(msg)
            finally:
                # make sure we release this memory
                pluginDict.clear()
                del jobsToSubmit[:]

        # Create successful jobs in BossAir
        try:
            logging.debug("About to create %i new jobs in BossAir", len(successJobs))
            self.createNewJobs(wmbsJobs=successJobs)
        except WMException:
            raise
        except Exception as ex:
            msg = "Unhandled error in creation of %i new jobs.\n" % len(successJobs)
            msg += str(ex)
            logging.error(msg)
            logging.debug("Job: %s", successJobs)
            raise BossAirException(msg)

        return successJobs, failureJobs

    def track(self, runJobIDs=None, wmbsIDs=None):
        """
        _track_

        Track all running jobs
        Load job info from the cache (it should be there since we submitted the job)

        OPTIONAL: You can submit a list of jobs to check, based either on wmbsIDs or
         on runjobIDs.  This takes a list of integer IDs.
        """

        jobsToChange = []
        jobsToComplete = []
        jobsToReturn = []
        returnList = []

        jobsToTrack = {}

        runningJobs = self._listRunJobs(active=True)

        if runJobIDs:
            for job in runningJobs:
                if job['id'] not in runJobIDs:
                    runningJobs.remove(job)
        if wmbsIDs:
            for job in runningJobs:
                if job['jobid'] not in wmbsIDs:
                    runningJobs.remove(job)

        if len(runningJobs) < 1:
            # Then we have no running jobs
            return returnList

        logging.info("About to start building running jobs")

        loadedJobs = self._buildRunningJobsFromRunJobs(runJobs=runningJobs)

        logging.info("About to look for %i loadedJobs.", len(loadedJobs))

        for runningJob in loadedJobs:
            plugin = runningJob['plugin']
            if plugin not in jobsToTrack:
                jobsToTrack[plugin] = []
            jobsToTrack[plugin].append(runningJob)

        for plugin in jobsToTrack:
            if plugin not in self.plugins:
                msg = "Jobs tracking with non-existant plugin %s\n" % (plugin)
                msg += "They were submitted but can't be tracked?\n"
                msg += "That's too strange to continue\n"
                logging.error(msg)
                raise BossAirException(msg)
            try:
                # Then we send them to the plugins
                # Should give you a lit of jobs to change and jobs to complete
                pluginInst = self.plugins[plugin]
                localRunning, localChanges, localCompletes = pluginInst.track(jobs=jobsToTrack[plugin])
                jobsToReturn.extend(localRunning)
                jobsToChange.extend(localChanges)
                jobsToComplete.extend(localCompletes)
                logging.info("Executing/changing/completing %i/%i/%i jobs in plugin %s.", len(localRunning),
                             len(localChanges), len(localCompletes), plugin)
            except WMException:
                raise
            except Exception as ex:
                msg = "Unhandled exception while tracking jobs for plugin %s!\n" % plugin
                msg += str(ex)
                logging.error(msg)
                logging.debug("JobsToTrack: %s", jobsToTrack[plugin])
                raise BossAirException(msg)

        logging.info("About to change %i jobs", len(jobsToChange))
        logging.debug("JobsToChange: %s", jobsToChange)
        logging.info("About to complete %i jobs", len(jobsToComplete))
        logging.debug("JobsToComplete: %s", jobsToComplete)

        self._updateJobs(jobs=jobsToChange)
        self._complete(jobs=jobsToComplete)

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
            return

        # We should be insulated from bad plugins by track()
        jobsToComplete = {}

        for job in jobs:
            if job['plugin'] not in jobsToComplete:
                jobsToComplete[job['plugin']] = []
            jobsToComplete[job['plugin']].append(job)

        try:
            for plugin in jobsToComplete:
                self.plugins[plugin].complete(jobsToComplete[plugin])
        except WMException:
            raise
        except Exception as ex:
            msg = "Exception while completing jobs!\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("JobsToComplete: %s", jobsToComplete)
            raise BossAirException(msg)
        finally:
            # If the complete code fails, label the jobs as finished anyway
            # We want to avoid cyclic repetition of failed jobs
            # If they don't have a FWJR, the Accountant will catch it.
            self._completeKill(jobs)

        return

    def _completeKill(self, jobs):
        """
        __completeKill_

        Mark jobs killed in BossAir as completed
        Requires jobs in RunJob format
        """
        if len(jobs) < 1:
            return

        idsToComplete = [job['id'] for job in jobs]

        existingTransaction = self.beginTransaction()
        self.completeDAO.execute(jobs=idsToComplete, conn=self.getDBConn(),
                                 transaction=self.existingTransaction())
        self.commitTransaction(existingTransaction)

        return

    def getComplete(self):
        """
        _getComplete_

        The tracker should call this: It's only
        interested in the jobs that are completed.
        """

        completeJobs = []

        completeRunJobs = self._listRunJobs(active=False)

        for rj in completeRunJobs:
            job = rj.buildWMBSJob()
            completeJobs.append(job)

        return completeJobs

    def kill(self, jobs, workflowName=None, killMsg=None, errorCode=71300):
        """
        _kill_

        Kill jobs using plugin functions:

        Only active jobs (status = 1) will be killed. If workflowName is given,
        then kill all its jobs in one shot.
        An optional killMsg can be sent; this will be written into the job FWJR.
        The errorCode will be the one specified and if no killMsg is provided then
        a standard message associated with the exit code will be used.
        If a previous FWJR exists, this error will be appended to it.
        """
        if not jobs:
            return
        jobsToKill = {}

        # Now get a list of which jobs are in the batch system
        # only kill jobs present there
        loadedJobs = self._buildRunningJobs(wmbsJobs=jobs)

        for runningJob in loadedJobs:
            plugin = runningJob['plugin']
            jobsToKill.setdefault(plugin, [])
            jobsToKill[plugin].append(runningJob)

        for plugin in jobsToKill:
            if plugin not in self.plugins:
                msg = "Jobs tracking with non-existant plugin %s\n" % (plugin)
                msg += "They were submitted but can't be tracked?\n"
                msg += "That's too strange to continue\n"
                logging.error(msg)
                raise BossAirException(msg)
            else:
                # Then we send them to the plugins
                try:
                    pluginInst = self.plugins[plugin]
                    if workflowName:
                        # jobs are completed regardless whether the kill succeeded or not
                        self._completeKill(jobs=jobsToKill[plugin])
                        pluginInst.killWorkflowJobs(workflow=workflowName)
                    else:
                        # raise an exception if it fails to kill jobs, such that the same
                        # jobs are retried again in the next cycle
                        pluginInst.kill(jobs=jobsToKill[plugin], raiseEx=True)
                        self._completeKill(jobs=jobsToKill[plugin])

                    # Register the killed jobs
                    for job in jobsToKill[plugin]:
                        if job.get('cache_dir') is None or job.get('retry_count') is None:
                            continue
                        # Try to save an error report as the jobFWJR
                        if not os.path.isdir(job['cache_dir']):
                            # Then we have a bad cache directory
                            logging.error("Could not write a kill FWJR due to non-existant cache_dir for job %i\n", job['id'])
                            logging.debug("cache_dir: %s\n", job['cache_dir'])
                            continue
                        reportName = os.path.join(job['cache_dir'], 'Report.%i.pkl' % job['retry_count'])
                        errorReport = Report()
                        if os.path.exists(reportName) and os.path.getsize(reportName) > 0:
                            # Then there's already a report there.  Add messages
                            errorReport.load(reportName)
                        # Build a better job message
                        if killMsg:
                            reportedMsg = killMsg
                        else:
                            reportedMsg = WM_JOB_ERROR_CODES[errorCode]
                            reportedMsg += '\n Job last known status was: %s' % job.get('globalState', 'Unknown')
                        errorReport.addError("JobKilled", errorCode, "JobKilled", reportedMsg)
                        try:
                            errorReport.save(filename=reportName)
                        except IOError as ioe:
                            logging.warning('Cannot write report %s because of %s', reportName, ioe)
                except RuntimeError:
                    logging.warning("Plugin failed to remove jobs. It will be retried in the next cycle.")
                except WMException:
                    raise
                except Exception as ex:
                    msg = "Unhandled exception while calling kill method for plugin %s\n" % plugin
                    msg += str(ex)
                    logging.error(msg)
                    logging.debug("Interrupted while killing following jobs: %s\n", jobsToKill[plugin])
                    raise BossAirException(msg)
        return

    def update(self, jobs):
        """
        _update_

        Overwrite the database with whatever you put into
        this function.
        """

        runJobs = self._buildRunningJobs(wmbsJobs=jobs)

        self._updateJobs(jobs=runJobs)

        return

    def monitor(self, commonState=True):
        """
        _monitor_

        Initiate the call to the monitoring DAO
        This should not be called by the standard Submitter/Status/Tracker
        system.  It is meant for outside calling.
        """
        results = self.monitorDAO.execute(commonState, conn=self.getDBConn(),
                                          transaction=self.existingTransaction())

        return results

    def updateJobInformation(self, workflow, **kwargs):
        """
        _updateJobInformation_

        Update the information of jobs in a particular workflow,
        the data will be updated according the keyword arguments which
        will be interpreted by the individual plugins accordingly.
        """
        for plugin in self.plugins:
            try:
                pluginInst = self.plugins[plugin]
                pluginInst.updateJobInformation(workflow, **kwargs)
            except WMException:
                raise
            except Exception as ex:
                msg = "Unhandled exception while calling update method for plugin %s\n" % plugin
                msg += str(ex)
                logging.error(msg)
                raise BossAirException(msg)
        return

    def updateSiteInformation(self, jobs, siteName, excludeSite):
        """
        _updateSiteInformation_

        Modify condor classAd for all Idle jobs for a site if it has gone Down, Draining or Aborted.
        Kill all jobs if the site is the only site for the job.
        """
        jobkill = []
        for plugin in self.plugins:
            try:
                pluginInst = self.plugins[plugin]
                tempjoblist = pluginInst.updateSiteInformation(jobs, siteName, excludeSite)
                if tempjoblist is not None:
                    jobkill.extend(tempjoblist)
            except WMException:
                raise
            except Exception as ex:
                msg = "Unhandled exception while calling update method for plugin %s\n" % plugin
                msg += str(ex)
                logging.error(msg)
                raise BossAirException(msg)
        return jobkill

    def _buildRunningJobsFromRunJobs(self, runJobs):
        """
        _buildRunningJobsFromRunJobs_

        Same as _buildRunningJobs_, but taking runJobs as input
        """
        finalJobs = []

        loadedJobs = self._loadByID(jobs=runJobs)

        for loadJob in loadedJobs:
            runJob = None
            for rj in runJobs:
                if rj['id'] == loadJob['id']:
                    runJob = rj
                    break
            # We should have two instances of the job
            for key in runJob:
                # Fill one from the other
                # runJob, being most recent, should be on top
                if runJob[key] is None:
                    runJob[key] = loadJob.get(key, None)
            finalJobs.append(runJob)

        return finalJobs

    def _buildRunningJobs(self, wmbsJobs):
        """
        _buildRunningJobs_

        Build running jobs by loading information from the database and
        compiling it into a runJob object.  This overwrites any information
        from the database with the info from the WMBS Job
        """
        runJobsLoaded = self.loadByWMBS(wmbsJobs=wmbsJobs)

        return runJobsLoaded
