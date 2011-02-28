#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The Poller that polls couch for job information
Right now only pushes to dashboard
Although it can push to any monitoring system
"""
__all__ = []


import time
import os.path
import logging


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.WMException                     import WMException
from WMCore.JobStateMachine.ChangeState     import ChangeState

from WMComponent.DashboardReporter.DashboardInterface import DashboardInterface

from WMComponent.DashboardReporter.DashboardAPI import apmonSend, apmonFree


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
                self.addTask(name = taskName)
            logging.info("Sending info for task %s" % str(job))
            
            package = {}
            #package['jobId']         = 'wmagent_%s/%s_%i' % (taskName, job['name'], job['retryCount'])
            package['jobId']         = '%s_%i' % (job['name'], job['retryCount'])
            package['taskId']        = 'wmagent_%s' % taskName
            package['GridJobID']     = 'NotAvailable'
            package['retryCount']    = job['retryCount']
            package['MessageTS']     = time.time()
            package['MessageType']   = 'JobMeta'
            package['scheduler']     = 'NotAvailable'
            package['sid']           = 'NotAvailable'
            package['localId']       = 'NotAvailable'
            package['RBname']        = 'NotAvailable'
            package['bossId']        = 'NotAvailable'
            package['TargetSE']      = 'NotAvailable'
            package['TargetCE']      = 'NotAvailable'
            
            #result = self.dashboard.send(package = job)
            logging.info("Sending: %s" % str(package))
            result = apmonSend( taskid = package['taskId'], jobid = package['jobId'], params = package, logr = logging, apmonServer = self.serverreport)
            #sendToML(package)

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
            #package['jobId']             = 'wmagent_%s/%s_%i' % (job['requestName'], job['name'], job['retryCount'])
            package['jobId']         = '%s_%i' % (job['name'], job['retryCount'])
            package['taskId']            = 'wmagent_%s' % job['requestName']
            package['GridJobID']         = 'NotAvailable'
            package['retryCount']        = job['retryCount']
            package['MessageTS']         = time.time()
            package['MessageType']       = 'JobStatus'
            package['StatusValue']       = job['finalState']
            package['StatusDestination'] = 'NotAvailable'
            package['StatusReason']      = 'NotAvailable'
            package['StatusEnterTime']   = 'NotAvailable'
            
            #result = self.dashboard.send(package = job)
            logging.info("Sending: %s" % str(package))
            result = apmonSend( taskid = package['taskId'], jobid = package['jobId'], params = package, logr = logging, apmonServer = self.serverreport)

            #result = self.apmonsender.sendToML(package)

            if result != 0:
                msg =  "Error %i sending info for completed job %s via UDP\n" % (result, job['name'])
                msg += "Ignoring"
                logging.error(msg)
                logging.debug("Package sent: %s\n" % package)
                logging.debug("Host info: host %s, port %s, pass %s" \
                              % (self.config.DashboardReporter.dashboardHost,
                                 self.config.DashboardReporter.dashboardPort,
                                 getattr(self.config.DashboardReporter, 'dashboardPass', '')))
            #self.apmonsender.free()    
            apmonFree()
        return


    def addTask(self, name):
        """
        _addTask_

        Add a task to the Dashboard
        """

        package = {}
        package['MessageType']        = 'TaskMeta'
        package['MessageTS']          = time.time()
        package['taskId']             = 'wmagent_%s' % name
        package['jobId']              = 'taskMeta'
        package['ApplicationVersion'] = 'NotAvailable'
        package['datasetFull']        = 'NotAvailable'
        package['Executable']         = 'NotAvailable'
        package['JSTool']             = 'WMAgent'
        package['JSToolVersion']      = 'NotAvailable'
        package['TaskType']           = 'WMAgentTesting'
        package['GridName']           = 'NotAvailable'
        package['CMSUser']            = 'NotAvailable'
        package['user']               = 'NotAvailable'
        package['VO']                 = 'NotAvailable'
        package['JSToolUI']           = 'NotAvailable'
        package['Workflow']           = 'NotAvailable'
        package['ProductionTeam']     = 'NotAvailable'

        logging.info("Sending info for task %s" % str(name))

        #result = self.dashboard.send(package = package)
        logging.info("Sending: %s" % str(package))
        #result = self.apmonsender.sendToML(package)
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
        
