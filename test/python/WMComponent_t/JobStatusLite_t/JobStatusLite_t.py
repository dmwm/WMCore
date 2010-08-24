#!/usr/bin/env python

"""
JobStatusLite unit test 
"""





from WMQuality.TestInit import TestInit

from WMCore.Agent.HeartbeatAPI              import HeartbeatAPI


# Import JobStatusLite modules
from WMComponent.JobStatusLite.JobStatusPoller import JobStatusPoller
from WMComponent.JobStatusLite.StatusScheduling import StatusScheduling

# Import BossLite Objects
from WMCore.BossLite.DbObjects.Job          import Job
from WMCore.BossLite.DbObjects.Task         import Task
from WMCore.BossLite.DbObjects.RunningJob   import RunningJob
from WMCore.BossLite.API.BossLiteAPI        import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched   import BossLiteAPISched
from WMCore.BossLite.DbObjects.BossLiteDBWM import BossLiteDBWM
from WMCore.BossLite.Common.Exceptions      import DbError
from WMCore.BossLite.Common.Exceptions      import SchedulerError


#import threading
import unittest
import os
from nose.plugins.attrib import attr

def fakeTask( db, numjob ):
    """
    This procedure create a fakeTask
    """

    taskParams = { 'name' : 'testTask_submit', \
                   'globalSandbox' : '/etc/redhat-release' \
                 }

    jobParams = { 'executable' : '/bin/hostname', \
                  'arguments' : '-f' \
                }

    task = Task( taskParams )
    task.create( db )
    task.exists( db )
    i = 0
    for j in xrange( numjob ):
        jobParams['name'] = 'Fake_job_%s' % str(j)
        jobParams['standardError'] = 'hostname-%s.err' % str(j)
        jobParams['standardOutput'] = 'hostname-%s.out' % str(j)
        jobParams['outputFiles'] = [ jobParams['standardOutput'] ]
        jobParams['wmbsJobId'] = i
        i += 1
        
        job = Job( parameters = jobParams )
        job.newRunningInstance( db )
        task.addJob( job )
    task.save( db )
    
    return taskParams['name']


class JobStatusLite_t( unittest.TestCase ):
    """
    Test cases for JobStatusLite module
    """

    def setUp( self ):
        """
        setup needed for the tests
        """

        self.testInit = TestInit( __file__ )
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema( customModules = ["WMCore.BossLite", "WMCore.Agent.Database"], \
                                 useDefault = False \
                               )

        # Set heartbeat
        self.componentName = 'JobStatusLite'
        self.heartbeatAPI  = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()


    def tearDown( self ):
        """
        Tear down database
        """

        self.testInit.clearDatabase()

        return

    def fillDatabase( self, numtask = 2, numjob = 3, \
                      status = 'R', pstatus = 'handled'):
        """
        generate some fake tasks/jobs/runjobs
        """

        print "Populating database"
        db = BossLiteDBWM()
        totaljobadded = 0
        names = []
        for t in xrange( numtask ):
            try:
                task = Task()
                task.data['name'] = 'task_%s' % str(t)
                names.append( 'task_%s' % str(t) )
                task.create( db )
                i = 0
                for j in xrange( 1, 1 + numjob ):
                    jobstatic = { \
                                   'name':   '%s_job_%s' % (str(t), str(j)), \
                                   'jobId':  j, \
                                   'taskId': task.exists( db ), \
                                   'wmbsJobId': i, \
                                   'submissionNumber': 1, \
                                   'closed': 'N' \
                                }
                    i += 1
                    job = Job( parameters = jobstatic )
                    job.create( db )
                    job.save( db )
                    job_run = { \
                                'jobId':      job.data['jobId'], \
                                'taskId':     task.exists( db ), \
                                'submission': 1, \
                                'schedulerId':  'id_scheduler', \
                                'processStatus': pstatus, \
                                'closed':     'N', \
                                'status':     status \
                               }
                    runJob = RunningJob( parameters = job_run )
                    runJob.create( db )
                    runJob.save( db )
                    totaljobadded += 1
            except Exception, ex:
                print "ERROR: '%s'" % str( ex )
                print "\ttask_" + str( t )
        print "..finished."
        return names, totaljobadded


    def createConfig( self ):
        """
        generate an example of configuration for the JobStatusLite component
        """

        config = self.testInit.getConfiguration()

        config.component_('JobStatusLite')
        config.Agent.agentName  = 'testAgent'
        config.Agent.componentName = 'JobStatusLite'
        config.JobStatusLite.namespace     = \
                   'WMComponent.JobStatusLite.JobStatusLite'
        config.JobStatusLite.componentDir  = os.getcwd()
        config.JobStatusLite.logLevel      = 'INFO'
        config.JobStatusLite.pollInterval  = 10
        config.JobStatusLite.queryInterval = 10
        config.JobStatusLite.jobLoadLimit  = 500
        config.JobStatusLite.maxJobQuery   = 100
        config.JobStatusLite.taskLimit     = 30
        config.JobStatusLite.maxJobsCommit = 100
        config.JobStatusLite.processes     = 3

        return config


    def submit( self ):
        """
        Simple submission operation
        """
        
        myBossLiteAPI = BossLiteAPI()

        taskname = fakeTask( myBossLiteAPI.db, 3 )
        task = myBossLiteAPI.loadTaskByName( taskname )

        mySchedConfig =  { 'name' : 'SchedulerGLite' }
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, \
                                       schedulerConfig = mySchedConfig )
        
        mySchedAPI.submit( task['id'] )
        
        return taskname

    def kill( self, taskid ):
        """
        Simple kill operation
        """
        myBossLiteAPI = BossLiteAPI()

        mySchedConfig =  { 'name' : 'SchedulerGLite' }
        mySchedAPI = BossLiteAPISched( bossLiteSession = myBossLiteAPI, \
                                       schedulerConfig = mySchedConfig )

        task = mySchedAPI.kill( taskid )

        return task['name']
    @attr('integration')
    def testA_PollingFailed( self ):
        """
        testing the polling of failed jobs
        """

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 3, 'A', 'handled' )[0]
        print "Calling JobStatusPoller for Aborted jobs"
        obj1 = JobStatusPoller( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed" 
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskadded[0] )
        for job in task.jobs:
            self.assertEqual( job.runningJob['processStatus'], 'failed' )
        print "..finished."
        
    @attr('integration')
    def testB_PollingSuccess( self ):
        """
        testing the polling of success jobs
        """

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 3, 'SD', 'handled' )[0]
        print "Calling JobStatusPoller for Done jobs"
        obj1 = JobStatusPoller( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed"
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskadded[0] )
        for job in task.jobs:
            self.assertEqual( job.runningJob['processStatus'], \
                              'output_requested' )
        print "..finished."
        
    @attr('integration')
    def testC_PollingNew( self ):
        """
        testing the polling of new jobs
        """

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 3, 'S', 'not_handled' )[0]
        print "Calling JobStatusPoller for new jobs"
        obj1 = JobStatusPoller( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed"
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskadded[0] )
        for job in task.jobs:
            self.assertEqual( job.runningJob['processStatus'], 'handled' )
        print "..finished."
    
    @attr('integration')
    def testD_PollingKilled( self ):
        """
        testing the polling of killed jobs
        """

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 3, 'K', 'handled' )[0]
        print "Calling JobStatusPoller for Killed jobs"
        obj1 = JobStatusPoller( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed"
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskadded[0] )
        for job in task.jobs:
            self.assertEqual( job.runningJob['processStatus'], 'failed' )
        print "..finished."

    @attr('integration')
    def testE_GroupAssignment( self ):
        """
        testing the group assignment for sub-processes
        """

        config = self.createConfig()
        numjob = self.fillDatabase( 5, 50 )[1]
        print "Calling StatusScheduling"
        obj1 = StatusScheduling( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed"
        db = BossLiteDBWM()

        ## check if jobs have been assigned to a group
        result = db.executeSQL( \
                               "SELECT j.group_id, j.job_id, j.task_id " +\
                               "FROM jt_group j JOIN bl_runningjob b ON " +\
                               "(j.task_id = b.task_id and " +\
                               "j.job_id = b.job_id) " +\
                               "order by j.group_id, j.task_id, j.job_id;" \
                              )
        raws = result[0].fetchall()
        self.assertEqual( len(raws), numjob )
        for tupla in raws:
            group = tupla[0]
            self.assertNotEqual( group, 0 )
        print "..finished."
    
    @attr('integration')
    def testF_StatusCheck( self ):
        """
        testing the status check
        """

        config = self.createConfig()
        print "Real task submission..."
        taskname = self.submit()
        print "...done!"
        print "Calling StatusScheduling"
        obj1 = StatusScheduling( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed"
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskname )
        ## need to kill the submitted jobs
        ## we don't care if it is wrong of it is right
        try:
            self.kill( task['id'] )
        except SchedulerError, se:
            pass
        print "..finished."


if __name__ == '__main__':
    unittest.main()

