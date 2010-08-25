#!/usr/bin/env python

"""
JobStatusLite unit test 
"""

__revision__ = "$Id: JobStatusLite_t.py,v 1.8 2010/08/17 20:56:00 meloam Exp $"
__version__ = "$Revision: 1.8 $"


from WMQuality.TestInit import TestInit

# Import JobStatusLite modules
from WMComponent.JobStatusLite.JobStatusPoller import JobStatusPoller
from WMComponent.JobStatusLite.StatusScheduling import StatusScheduling

# Import BossLite Objects
from WMCore.BossLite.DbObjects.Job         import Job
from WMCore.BossLite.DbObjects.Task        import Task
from WMCore.BossLite.DbObjects.RunningJob  import RunningJob
from WMCore.BossLite.API.BossLiteAPI       import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched  import BossLiteAPISched
from WMCore.BossLite.DbObjects.BossLiteDBWM import BossLiteDBWM
from WMCore.BossLite.Common.Exceptions     import DbError

#import threading
import unittest
import os


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
    for j in xrange( numjob ):
        jobParams['name'] = 'Fake_job_%s' % str(j)
        jobParams['standardError'] = 'hostname-%s.err' % str(j)
        jobParams['standardOutput'] = 'hostname-%s.out' % str(j)
        jobParams['outputFiles'] = [ jobParams['standardOutput'] ]
        
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
        self.testInit.setSchema( customModules = ["WMCore.BossLite"], \
                                 useDefault = False \
                               )

    def tearDown( self ):
        """
        Tear down database
        """

        self.testInit.clearDatabase()

        return

    def fillDatabase( self, numtask = 2, numjob = 50, \
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
                for j in xrange( 1, 1 + numjob ):
                    jobstatic = { \
                                   'name':   '%s_job_%s' % (str(t), str(j)), \
                                   'jobId':  j, \
                                   'taskId': task.exists( db ), \
                                   'submissionNumber': 1, \
                                   'closed': 'N' \
                                }
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
        config.JobStatusLite.namespace     = \
                   'WMComponent.JobStatusLite.JobStatusLite'
        config.JobStatusLite.componentDir  = os.getcwd()
        config.JobStatusLite.ComponentDir  = '/tmp/application/JobStatusLite'
        config.JobStatusLite.logLevel      = 'INFO'
        config.JobStatusLite.pollInterval  = 60
        config.JobStatusLite.queryInterval = 12
        config.JobStatusLite.jobLoadLimit  = 500
        config.JobStatusLite.maxJobQuery   = 50
        config.JobStatusLite.taskLimit     = 30
        config.JobStatusLite.maxJobsCommit = 100
        config.JobStatusLite.processes     = 3

        return config


    def submit( self ):
        """
        Simple submission operation
        """
        
        myBossLiteAPI = BossLiteAPI()

        taskname = fakeTask( myBossLiteAPI.db, 10 )
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

    def testA_PollingFailed( self ):
        """
        testing the polling of failed jobs
        """
        raise RuntimeError, "This test takes too long to run: see - http://vpac05.phy.vanderbilt.edu:8010/builders/Unit%20Tests%20Mysql/builds/137/steps/test/logs/stdio --MELO"
        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 10, 'A', 'handled' )[0]
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

    def testB_PollingSuccess( self ):
        """
        testing the polling of success jobs
        """
        raise RuntimeError, "This test takes too long to run: see - http://vpac05.phy.vanderbilt.edu:8010/builders/Unit%20Tests%20Mysql/builds/139/steps/test/logs/stdio --MELO"

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 10, 'SD', 'handled' )[0]
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

    def testC_PollingNew( self ):
        """
        testing the polling of new jobs
        """
        raise RuntimeError, "This test takes too long to run: see - http://vpac05.phy.vanderbilt.edu:8010/builders/Unit%20Tests%20Mysql/builds/139/steps/test/logs/stdio --MELO"

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 10, 'S', 'not_handled' )[0]
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

    def testD_PollingKilled( self ):
        """
        testing the polling of killed jobs
        """

        config = self.createConfig()
        taskadded = self.fillDatabase( 1, 10, 'K', 'handled' )[0]
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

    def testF_StatusCheck( self ):
        """
        testing the status check
        """

        config = self.createConfig()
        taskname = self.submit()
        print "Calling StatusScheduling"
        obj1 = StatusScheduling( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed"
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskname )
        #for job in task.jobs:
        #    print job.runningJob
        ## need to kill the submitted jobs
        self.kill( task['id'] )
        print "..finished."

if __name__ == '__main__':
    unittest.main()

