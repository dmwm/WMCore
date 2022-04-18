#!/usr/bin/env python
"""
_BossAirPlugin_

Base class for BossAir plugins
"""
from __future__ import division
from builtins import range

import os
import logging
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException
from datetime import datetime
from datetime import timedelta
from random import randint
import multiprocessing
import pickle

def processWorker(myinput, tmp):
    try:
        while True:
            jj, report, lcreport = myinput.get()

            if jj == 'STOP':
                return

            targetDir = jj['cache_dir']
            outfile = os.path.join(targetDir,"Report.0.pkl")

            if os.path.isfile(outfile):
                continue

            taskName = targetDir.split('/')[5]
            if jj['cache_dir'].count("Production/LogCollect") > 0:
                if lcreport is not None:
                    lcreport.task = "/" + taskName + "/Production/LogCollect"
                    with open(outfile, 'wb') as f:
                        logging.debug('Process worker is dumping the LogCollect report to ' + f.name)
                        pickle.dump(lcreport, f)
                    continue
                else:
                    msg = "Parameter lcFakeReport is mandatory if you are using logCollect jobs"
                    raise BossAirPluginException(msg)

            #ensure each lfn of each output file in the job is unique by adding the jobid
            jobid = str(jj['id'])

            if hasattr(report, 'cmsRun1') and hasattr(report.cmsRun1.output, 'output'):
                tmpname = report.cmsRun1.output.output.files.file0.lfn.split('.root')[0]
                tmpname = tmpname + jobid
                report.cmsRun1.output.output.files.file0.lfn = tmpname + '.root'

            if hasattr(report, 'logArch1') and hasattr(report.logArch1, 'output'):
                tmpname = report.logArch1.output.logArchive.files.file0.lfn.split('.tar.gz')[0]
                tmpname = tmpname + jobid
                report.logArch1.output.logArchive.files.file0.lfn = tmpname + '.root'

            #get target diretory and set task name
            report.task = "/" + taskName + "/Production"

            #pickle the report again
            with open(outfile, 'wb') as f:
                logging.debug('Process worker is dumping the report to ' + f.name)
                pickle.dump(report, f)
    except Exception as ex:
        logging.exception(ex)


class MockPlugin(BasePlugin):
    """
    Implementation of a plugin which make fake submission to the Grid
    """
    @staticmethod
    def stateMap():
        """
        For a given name, return a global state
        """

        stateDict = {'New': 'Pending',
                     'Timeout': 'Error',
                     'Submitted': 'Pending',
                     'Waiting': 'Pending',
                     'Ready': 'Pending',
                     'Scheduled': 'Pending',
                     'Running': 'Running',
                     'Done(failed)': 'Running',
                     'Done': 'Running',
                     'Aborted': 'Error',
                     'Cleared': 'Complete',
                     'Cancelled by user': 'Complete',
                     'Cancelled': 'Error'
        }

        # This call is optional but needs to for testing
        BasePlugin.verifyState(stateDict)
        return stateDict

    def __init__(self, config):

        BasePlugin.__init__(self, config)
        self.config = config

        self.pool = []
        self.createdReport = []
        self.myinput = None
        if getattr(config.BossAir, 'MockPlugin', None) == None:
            msg = "Missing required config.BossAir.MockPlugin section"
            raise BossAirPluginException( msg )

        self.nProcess = getattr(config.BossAir.MockPlugin, 'mockPluginProcesses', 4)
        self.jobRunTime = getattr(config.BossAir.MockPlugin, 'jobRunTime', 120) #default job running time is two hours
        logging.info('Job Running time set to minutes %s' % self.jobRunTime)

        self.fakeReport = getattr(config.BossAir.MockPlugin, 'fakeReport', None)
        if self.fakeReport == None:
            msg = 'config.BossAir.MockPlugin.fakeReport is a required parameter'
            raise BossAirPluginException( msg )
        elif not os.path.isfile(self.fakeReport):
            msg = 'Cannot find %s file' % self.fakeReport
            raise BossAirPluginException( msg )

        self.jobsScheduledEnd = {}

        self.states = [ 'New',
                        'Timeout',
                        'Submitted',
                        'Waiting',
                        'Ready',
                        'Scheduled',
                        'Running',
                        'Done(failed)',
                        'Done',
                        'Aborted',
                        'Cleared',
                        'Cancelled by user',
                        'Cancelled']


    def submit(self, jobs, info = None):
        """
        The submit does nothing. It simply return all jobs as successful
        """

        #All jobs are succsessfully submitted ;)
        return jobs, []



    def track(self, jobs, info = None, currentTime = None):
        """
        Label the jobs as done

        """
        runningList = []
        completeList = []
        changeList = []

        #Create the workers if they do not exists
        if self.myinput == None:
            self.myinput  = multiprocessing.Queue()
            self.start( self.myinput )

        #for each job we will need to modify the default Report (the output of each job).
        with open(self.fakeReport, "rb") as f:
            report = pickle.load(f)

        lcreport = getattr(self.config.BossAir.MockPlugin, 'lcFakeReport', None)
        if lcreport != None:
            with open(lcreport, "rb") as f:
                lcreport = pickle.load(f)

        for jj in jobs:
            if jj['id'] not in self.jobsScheduledEnd:
                self._scheduleJob(jj, currentTime)
            oldState = jj['status']
            jobEnded = datetime.now() > self.jobsScheduledEnd[jj['id']]
            if jobEnded:
                jj['globalState'] = MockPlugin.stateMap()['Done']
                jj['status'] = 'Done'
                completeList.append(jj)
            else:
                jj['globalState'] = MockPlugin.stateMap()['Running']
                jj['status'] = 'Running'
                runningList.append(jj)
            if oldState != jj['status']:
                changeList.append(jj)

            if not jj['id'] in self.createdReport:
                #Copy a fake Report in the directory using workers
                self.myinput.put((jj, report, lcreport))
                self.createdReport.append(jj['id'])

        return runningList, changeList, completeList

    def kill(self, jobs, raiseEx=False):
        """
        _kill_

        Do nothing
        """
        pass

    def complete(self, jobs):
        """
        We do not need to do anything. The report has been copied in the track
        """

        return

    def _scheduleJob(self, job, currentTime = None):
        """
        Schedule the endtime of the job
        """
        if currentTime:
            # someone told us when to start
            nowt = currentTime
        else:
            nowt = datetime.now()
        #Compute some random (between 0 and 20% of the total running time)
        randlen = randint(0, int(self.jobRunTime * 20 / 100))
        totlen = randlen + self.jobRunTime
        self.jobsScheduledEnd[job['id']] = nowt + timedelta(minutes = totlen)
        logging.debug('Scheduled end time of job %s to %s' % (job['id'], self.jobsScheduledEnd[job['id']]))



    def start( self, myinput ):
        """
        _start_

        Start the mulitp.
        """
        if len(self.pool) == 0:
            # Starting things up
            for x in range(self.nProcess):
                logging.debug("Starting process %i" %x)
                p = multiprocessing.Process(target = processWorker,
                                           args = (myinput, 0))
                p.start()
                self.pool.append(p)

    def close(self, input, result):
        """
        _close_

        Kill all connections and terminate
        """
        logging.debug("Ready to close all %i started processes " \
                        % len(self.pool) )
        for x in self.pool:
            try:
                logging.debug("Shutting down %s " % str(x))
                input.put( ('-1', 'STOP', 'control') )
            except Exception as ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.error(msg)

        for proc in self.pool:
            proc.terminate()

        self.pool = []
        logging.debug('Slave stopped!')
        return


    def __del__(self):
        self.close(self.myinput, None)

    def updateSiteInformation(self, jobs, siteName, excludeSite) :
        """
        almost do nothing
        """
        return jobs
