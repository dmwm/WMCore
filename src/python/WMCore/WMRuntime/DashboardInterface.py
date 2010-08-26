#!/usr/bin/env python


# This is the interface to the Dashboard that the monitor will use

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DashboardInterface.py,v 1.1 2010/05/17 20:43:42 mnorman Exp $"


import threading
import os
import time
import logging
import socket
import subprocess
#import popen2


from WMCore.WMSpec.WMStep     import WMStepHelper

from WMCore.WMSpec.WMWorkload import getWorkloadFromTask

from WMCore.WMRuntime.Tools.Plugins.ApMonLite.ApMonDestMgr import ApMonDestMgr


def generateDashboardID(job, workload, task):
    """
    _generateDashboardID_

    Generate a global job ID for the dashboard

    """

    jobId = job['name'].replace("_", "-")
    agentName = getattr(workload.data, 'WMAgentName', 'WMAgentPrimary')
    
    jobName = "ProdAgent_%s_%s" %(
        agentName, jobId )

    workflowId = task.getPathName().replace('/', '-')
    workflowId = workflowId.replace("_", "-")
    taskName = "ProdAgent_%s_%s" % ( workflowId,
                                     agentName)
    subCount = job.get('retry_count', 0)
    jobName = "%s_%s" % (jobName, subCount)
    
    return taskName, jobName



def getGridJobID(default = 'None'):
    """
    _getGridJobID_

    Get the grid job ID from local os
    """

    GridJobIDPriority = ['EDG_WL_JOBID', 'GLITE_WMS_JOBID',
                         'CONDOR_JOBID', 'GLOBUS_GRAM_JOB_CONTACT',
                         'ARCSUBMITTER_JOBID']

    gridJobID = None
    for envVar in GridJobIDPriority:
        gridJobID = os.environ.get(envVar, None)
        if gridJobID != None:
            break

    if not gridJobID:
        gridJobID = default

    return gridJobID


def getSyncCE(default = socket.gethostname()):
    """
    _getSyncCE_

    Extract the SyncCE from GLOBUS_GRAM_JOB_CONTACT if available for OSG,
    otherwise broker info for LCG

    """
    result = socket.gethostname()

    if os.environ.has_key('GLOBUS_GRAM_JOB_CONTACT'):
        #  //
        # // OSG, Sync CE from Globus ID
        #//
        val = os.environ['GLOBUS_GRAM_JOB_CONTACT']
        try:
            host = val.split("https://", 1)[1]
            host = host.split(":")[0]
            result = host
        except:
            pass
        return result

    # Stu says LCG may have the globus gram contact
    
    #if os.environ.has_key('EDG_WL_JOBID'):
    #    #  //
    #    # // LCG, Sync CE from edg command
    #    #//
    #    command = "glite-brokerinfo getCE"
    #    pop = popen2.Popen3(command)
    #    pop.wait()
    #    exitCode = pop.poll()
    #    if exitCode:
    #        return result
    #
    #    content = pop.fromchild.read()
    #    result = content.strip()
    #    return result

    if os.environ.has_key('NORDUGRID_CE'):
        #  //
        # // ARC, Sync CE from env. var. submitted with the job by JobSubmitter
        #//
        return os.environ['NORDUGRID_CE']

    return result




class DashboardInfo(dict):
    """
    An object to let you assemble the information needed for a Dashboard Report
    
    """


    def __init__(self, task, job):
        """
        Init some stuff

        """


        self.task         = task
        self.workload     = getWorkloadFromTask(task)
        self.job          = job
        self.publisher    = None
        self.destinations = {}

        dict.__init__(self)


        self.setdefault("Application", None)
        self.setdefault("ApplicationVersion", None)
        self.setdefault("GridJobID", None)
        self.setdefault("LocalBatchID", None)
        self.setdefault("GridUser", None)
        self.setdefault("User" , self.workload.getOwner().get('name', 'WMAgent'))
        self.setdefault("JSTool","WMAgent")
        self.setdefault("NbEvPerRun", 0)
        self.setdefault("NodeName", None)
        self.setdefault("Scheduler", None)
        self.setdefault("TaskType", self.task.taskType())
        self.setdefault("NSteps", 0)
        self.setdefault("VO", "CMS")
        self.setdefault("TargetCE", None)
        self.setdefault("RBname", None)
        self.setdefault("JSToolUI" , None) # Can't set here, see bug #64232


        taskName, jobName = generateDashboardID(job = self.job,
                                                workload = self.workload,
                                                task = self.task)

        self.taskName = taskName
        self.jobName  = jobName

        self.setdefault("taskName", taskName)
        self.setdefault("jobName", jobName)
        



        return


    def jobStart(self):
        """
        _jobStart_

        Fill with basic information upon job start
        """

        self["JobStarted"] = time.time()
        self["GridJobID"]  = getGridJobID()
        self['SyncCE']     = getSyncCE()
        self.publish()

        return


    def jobEnd(self):
        """
        _jobEnd_

        Fill with jobEnding info
        """


        self['JobFinished'] = time.time()
        self.publish()
        return
    

    def stepStart(self, step):
        """
        _stepStart_

        Fill with the step-based information
        """

        helper = WMStepHelper(step)
        self['ExeStart']     = helper.name()
        self['ExeStartTime'] = time.time()

        # Some absolute crap that's hard-coded in that we should get rid of.
        if helper.stepType().lower() == 'cmssw':
            # Add the version, etc.
            self["ApplicationVersion"] = getattr(step.application.setup,
                                                 'cmsswVersion', None)
        self.publish()

        return


    def stepEnd(self, step, stepReport):
        """
        _stepEnd_

        Fill with step-ending information
        """

        helper = WMStepHelper(step)
        self['ExeEnd']        = helper.name()
        self['ExeFinishTime'] = time.time()
        self['ExeExitStatus'] = stepReport.stepSuccessful(stepName = helper.name())
        self.publish()


        return


    def jobKilled(self):
        """
        _jobKilled_
        
        What if the job is killed?
        """

        # Then the job failed
        self['JobExitStatus'] = 99999
        self['JobFinished']   = time.time()
        self.publish()

        return


    def stepKilled(self, step):
        """
        _stepKilled_

        Fill with step-ending information assuming utter failure
        """

        helper = WMStepHelper(step)
        self['ExeEnd']        = helper.name()
        self['ExeFinishTime'] = time.time()
        self['ExeExitStatus'] = 99999
        self.publish()


        return


    def periodicUpdate(self):
        """
        _periodicUpdate_
        
        One day this will do something useful.
        But not yet
        """

        self.publish()

        return


    def addDestination(self, host, port):
        """
        _addDestination_

        Add a publishing destination to the Publisher
        """

        if self.publisher == None:
            self._InitPublisher()
        self.destinations[host] = port
        self.publisher.newDestination(host, port)



    def publish(self, redundancy = 1):
        """
        _publish_

        Publish information in this object to the Dashboard
        using the ApMon interface and the destinations stored in this
        instance.

        redunancy is the amount to times to publish this information

        """
        if self.publisher == None:
            self._InitPublisher()
      
        
        self.publisher.connect()
        toPublish = {}
        toPublish.update(self)
        for key, value in toPublish.items():
            if value == None:
                del toPublish[key]
        
        for i in range(1, redundancy+1):
            self.publisher.send(**toPublish)
            
        self.publisher.disconnect()
        return





    def _InitPublisher(self):
        """
        _InitPublisher_

        *private*
        
        Initialise the ApMonDestMgr instance, verifying that the task and
        job attributes are set

        """
        if self.taskName == None:
            msg = "Error: You must set the task id before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError, msg
        if self.jobName == None:
            msg = "Error: You must set the job id before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError, msg
        self.publisher = ApMonDestMgr(clusterName = self.taskName, nodeName = self.jobName)
        return






    
