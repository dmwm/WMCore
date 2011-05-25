#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The Poller that polls couch for job information
Right now only pushes to dashboard
Although it can push to any monitoring system
"""
__all__ = []


import time
import socket
import os.path
import logging


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.WMException                     import WMException
from WMCore.JobStateMachine.ChangeState     import ChangeState

from WMCore.Services.Dashboard.DashboardAPI import apmonSend, apmonFree


class DashboardReporterException(WMException):
    """
    _DashboardReporterException_

    Something's wrong in talking to couch and pushing
    the information out.
    """

    pass



class DashboardReporterPoller(BaseWorkerThread):
    """
    _DashboardReporterPoller_

    Poll couch and then push the information out
    """



    def __init__(self, config):
        BaseWorkerThread.__init__(self)
        self.config = config
        
        destHost = config.DashboardReporter.dashboardHost
        destPort = config.DashboardReporter.dashboardPort
        destPass = getattr(config.DashboardReporter, 'dashboardPass', '')

        self.serverreport = [destHost + ':' + str(destPort)]

        self.changeState = ChangeState(self.config)
        #self.dashboard   = DashboardInterface(destHost = destHost,
        #                                      destPort = destPort,
        #                                      destPasswd = destPass)

        #self.apmonsender = ApmonIf() 
        self.taskCache = []
        self.agentName = 'WMAgent'
        if hasattr(config, 'Agent'):
            self.agentName = "%s@%s" % (getattr(config.Agent, 'agentName', 'WMAgent'),
                                        socket.getfqdn(socket.gethostname()))
        return


    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("Terminating. Doing one more pass before we die.\n")
        self.algorithm(params)


    def algorithm(self, parameters = None):
        """
        _algorithm_

        Run the code that polls couch
        """
        
        try:
            self.pollCouch()
        except WMException:
            raise
        except Exception, ex:
            msg =  "Unhandled exception in DashboardReporter\n"
            msg += str(ex)
            logging.error(msg)
            raise DashboardReporterException(msg)

        return

    def pollCouch(self):
        """
        _pollCouch_

        Poll the couch database, pull out all information, send it wherever.
        """

        transitions = self.changeState.listTransitionsForDashboard()

        submittedList = []
        completedList = []

        # Sort the job transitions into categories
        for job in transitions:
            newState = job['newState'].lower()
            oldState = job['oldState'].lower()
            if newState == 'executing':
                submittedList.append(job)
            elif oldState == 'executing' or oldState == 'complete':
                if newState == 'success':
                    # Then the job passed
                    job['finalState'] = 'success'
                else:
                    # Then the job failed
                    job['finalState'] = 'failed'
                completedList.append(job)

        # Now process those lists
        self.handleSubmitted(jobs = submittedList)
        self.handleCompleted(jobs = completedList)

        return


    def handleSubmitted(self, jobs):
        """
        _handleSubmitted_

        Handle the submitted jobs:
        Send them to Dashboard via UDP
        """
        logging.info("Handling jobs to be submitted: %s" % jobs)

        for job in jobs:
            taskName = job['requestName']
            if not taskName in self.taskCache:
                self.addTask(name = taskName, user = job.get('user', None))
            logging.info("Sending info for task %s" % str(job))

            package = {}
            package['jobId']           = '%s_%i' % (job['name'], job['retryCount'])
            package['taskId']          = 'wmagent_%s' % taskName
            package['GridJobID']       = 'NotAvailable'
            package['retryCount']      = job['retryCount']
            package['MessageTS']       = time.time()
            package['MessageType']     = 'JobMeta'
            package['JobType']         = job['taskType']
            package['StatusValue']     = 'submitted'
            package['scheduler']       = 'BossAir'
            package['StatusEnterTime'] = job.get('timestamp', time.time())
                        
            logging.info("Sending: %s" % str(package))
            result = apmonSend( taskid = package['taskId'], jobid = package['jobId'], params = package, logr = logging, apmonServer = self.serverreport)

            if result != 0:
                msg =  "Error %i sending info for submitted job %s via UDP\n" % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s, pass %s" \
                              % (self.config.DashboardReporter.dashboardHost,
                                 self.config.DashboardReporter.dashboardPort,
                                 getattr(self.config.DashboardReporter, 'dashboardPass', '')))
            apmonFree()
            #self.apmonsender.free()    
        return

    def handleCompleted(self, jobs):
        """
        _handleCompleted_
        
        Handle the completed jobs:
        Send them to Dashboard via UDP
        """
        logging.info("Handling jobs to be completed: %s" % jobs)        

        for job in jobs:
            package = {}
            package['jobId']           = '%s_%i' % (job['name'], job['retryCount'])
            package['taskId']          = 'wmagent_%s' % job['requestName']
            package['GridJobID']       = job['name']
            package['retryCount']      = job['retryCount']
            package['MessageTS']       = time.time()
            package['MessageType']     = 'JobStatus'
            package['StatusValue']     = job['finalState']
            package['StatusEnterTime'] = job['timestamp']
            package['JobExitCode']     = job['exitCode']

            logging.info("Sending completed info: %s" % str(package))
            result = apmonSend( taskid = package['taskId'], jobid = package['jobId'], params = package, logr = logging, apmonServer = self.serverreport)

            if result != 0:
                msg =  "Error %i sending info for completed job %s via UDP\n" % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s, pass %s" \
                              % (self.config.DashboardReporter.dashboardHost,
                                 self.config.DashboardReporter.dashboardPort,
                                 getattr(self.config.DashboardReporter, 'dashboardPass', '')))
            apmonFree()

            self.handleSteps(job = job)
        return


    def handleSteps(self, job):
        """
        _handleSteps_

        Handle the post-processing step information
        """

        performanceSteps = job['performance']
        for stepName in performanceSteps.keys():
            performance = performanceSteps[stepName]
            package = {}
            package['jobId']                  = '%s_%i' % (job['name'], job['retryCount'])
            package['taskId']                 = 'wmagent_%s' % job['requestName']
            package['stepName']               = stepName
            package['PeakValueRss'] 	      = performance['memory'].get('PeakValueRss', None)
            package['PeakValueVsize'] 	      = performance['memory'].get('PeakValueVsize', None)
            package['writeTotalMB']           = performance['storage'].get('writeTotalMB', None)
            package['readPercentageOps']      = performance['storage'].get('readPercentageOps', None)
            package['readAveragekB'] 	      = performance['storage'].get('readAveragekB', None)
            package['readTotalMB'] 	      = performance['storage'].get('readTotalMB', None)
            package['readNumOps']  	      = performance['storage'].get('readNumOps', None)
            package['readCachePercentageOps'] = performance['storage'].get('readCachePercentageOps', None)
            package['readMBSec']              = performance['storage'].get('readMBSec', None)
            package['readMaxMSec']            = performance['storage'].get('readMaxMSec', None)
            package['readTotalSecs'] 	      = performance['storage'].get('readTotalSecs', None) 
            package['writeTotalSecs'] 	      = performance['storage'].get('writeTotalSecs', None) 
            package['TotalJobCPU']            = performance['cpu'].get('TotalJobCPU', None)
            package['TotalEventCPU'] 	      = performance['cpu'].get('TotalEventCPU', None)
            package['AvgEventCPU'] 	      = performance['cpu'].get('AvgEventCPU', None)
            package['AvgEventTime'] 	      = performance['cpu'].get('AvgEventTime', None)
            package['MinEventCPU']            = performance['cpu'].get('MinEventCPU', None)
            package['MaxEventTime'] 	      = performance['cpu'].get('MaxEventTime', None)
            package['TotalJobTime'] 	      = performance['cpu'].get('TotalJobTime', None)
            package['MinEventTime'] 	      = performance['cpu'].get('MinEventTime', None)
            package['MaxEventCPU']            = performance['cpu'].get('MaxEventCPU', None)
            
            logging.debug("Sending performance info: %s" % str(package))
            result = apmonSend( taskid = package['taskId'], jobid = package['jobId'], params = package, logr = logging, apmonServer = self.serverreport)
        
            if result != 0:
                msg =  "Error %i sending info for completed job %s via UDP\n" % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s, pass %s" \
                              % (self.config.DashboardReporter.dashboardHost,
                                 self.config.DashboardReporter.dashboardPort,
                                 getattr(self.config.DashboardReporter, 'dashboardPass', '')))
        apmonFree()

        return


    def addTask(self, name, user):
        """
        _addTask_

        Add a task to the Dashboard
        """

        package = {}
        package['MessageType']    = 'TaskMeta'
        package['MessageTS']      = time.time()
        package['taskId']         = 'wmagent_%s' % name
        package['jobId']          = 'taskMeta'
        package['JSTool']         = 'WMAgent'
        package['JSToolVersion']  = '0.7.8'
        package['TaskType']       = 'reprocessing'
        package['CMSUser']        = user
        package['Workflow']       = name
        package['AgentName']      = self.agentName

        logging.info("Sending info for task %s" % str(name))

        logging.debug("Sending task info: %s" % str(package))
        result = apmonSend( taskid = package['taskId'], jobid = package['jobId'], params = package, logr = logging, apmonServer = self.serverreport)


        if result != 0:
            msg =  "Error %i sending info for new task %s via UDP\n" % (result, name)
            msg += "Ignoring"
            logging.error(msg)
            logging.debug("Package sent: %s\n" % package)
            logging.debug("Host info: host %s, port %s, pass %s" \
                          % (self.config.DashboardReporter.dashboardHost,
                             self.config.DashboardReporter.dashboardPort,
                             getattr(self.config.DashboardReporter, 'dashboardPass', '')))
        else:
            self.taskCache.append(name)
        
        #self.apmonsender.free()
        apmonFree() 
        
