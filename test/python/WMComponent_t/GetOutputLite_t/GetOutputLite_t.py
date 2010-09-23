#!/usr/bin/env python

"""
GetOutputLite unit test 
"""





from WMQuality.TestInit import TestInit

from WMCore.Agent.HeartbeatAPI              import HeartbeatAPI


# Import GetOutputLite modules
from WMComponent.GetOutputLite.GetOutputPoller import GetOutputPoller

# Import BossLite Objects
from WMCore.BossLite.DbObjects.Job          import Job
from WMCore.BossLite.DbObjects.Task         import Task
from WMCore.BossLite.DbObjects.RunningJob   import RunningJob
from WMCore.BossLite.API.BossLiteAPI        import BossLiteAPI
from WMCore.BossLite.API.BossLiteAPISched   import BossLiteAPISched
from WMCore.BossLite.DbObjects.BossLiteDBWM import BossLiteDBWM
from WMCore.BossLite.Common.Exceptions      import SchedulerError


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


class GetOutputLite_t( unittest.TestCase ):
    """
    Test cases for GetOutputLite module
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
        self.componentName = 'GetOutputLite'
        self.heartbeatAPI  = HeartbeatAPI(self.componentName)
        self.heartbeatAPI.registerComponent()

    def cleanDir( self, typestat ):
        """
        Clean directories
        """

        os.system( 'rm -rf test_%s_job_*' % typestat )

        return

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
                task.data['startDirectory'] =  'gsiftp://' + os.getenv('HOSTNAME')
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
                    jobrun = { \
                                'jobId':      job.data['jobId'], \
                                'taskId':     task.exists( db ), \
                                'submission': 1, \
                                'schedulerId':  'id_scheduler', \
                                'processStatus': pstatus, \
                                'closed':     'N', \
                                'status':     status \
                               }
                    runJob = RunningJob( parameters = jobrun )
                    runJob.create( db )
                    runJob.save( db )
                    totaljobadded += 1
            except Exception, ex:
                print "ERROR: '%s'" % str( ex )
                print "\ttask_" + str( t )
        print "..finished."
        return names[0], totaljobadded


    def createConfig( self ):
        """
        generate an example of configuration for the GetOutputLite component
        """

        config = self.testInit.getConfiguration()

        config.component_('GetOutputLite')
        config.Agent.agentName  = 'testAgent'
        config.Agent.componentName = 'GetOutputLite'
        config.GetOutputLite.namespace     = \
                   'WMComponent.GetOutputLite.GetOutputLite'
        config.GetOutputLite.componentDir  = os.getcwd()
        config.GetOutputLite.logLevel      = 'DEBUG'
        config.GetOutputLite.processes     = 3
        config.GetOutputLite.loadlimit       = 200
        config.GetOutputLite.pollInterval    = 10

        return config

    def hackingAborted( self, taskname ):
        """
        Adjusting to simulate aborted jobs
        """
        bliteapi = BossLiteAPI()

        task = bliteapi.loadTaskByName( taskname )
        for jj in task.jobs:
            os.system( 'mkdir %s/test_aborted_job_%i/' % (os.getcwd(), jj['id']) )
            jj['outputDirectory'] = 'gsiftp://localhost%s/test_aborted_job_%i/' % (os.getcwd(), jj['id'])
            jj.runningJob['status'] = 'A'
            jj.runningJob['processStatus'] = 'handled'

        bliteapi.updateDB( task )

    def hackingDone( self, taskname ):
        """
        Adjusting to simulate done jobs
        """
        bliteapi = BossLiteAPI()

        task = bliteapi.loadTaskByName( taskname )
        for jj in task.jobs:
            os.system( 'mkdir %s/test_done_job_%i/' % (os.getcwd(), jj['id']) )
            jj['outputDirectory'] = 'gsiftp://localhost%s/test_done_job_%i/' % (os.getcwd(), jj['id'])

        bliteapi.updateDB( task )

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

    def testA_PollingFailed( self ):
        """
        testing the polling of failed jobs
        """
        config = self.createConfig()
        print "Real task submission..."
        taskname = self.submit( )
        print "...done!"
        print "Simulating Aborted jobs!"
        self.hackingAborted( taskname )
        print "...done!"
        print "Calling GetOutputPoller for Aborted jobs"
        obj1 = GetOutputPoller( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        print "Checking if jobs were processed" 
        bliteapi = BossLiteAPI()
        task = bliteapi.loadTaskByName( taskname )
        for jj in task.jobs:
            self.assertEqual( os.path.exists('%s/test_aborted_job_%i/loggingInfo.%i.log' % (os.getcwd(), jj['id'], jj['submissionNumber'])), True )
        ## need to kill the submitted jobs
        ## we don't care if it is wrong of it is right
        try:
            self.kill( task['id'] )
        except SchedulerError, se:
            pass
        self.cleanDir( 'aborted' )
        print "..finished."


    def testB_PollingDone( self ):
        config = self.createConfig()
        print "Fill database..."
        taskname, totjob = self.fillDatabase( numtask = 1, status = 'SD' )
        print "...%i jobs: done!" % totjob
        print "Simulating Aborted jobs!"
        print "...done!"
        print "Calling GetOutputPoller for Aborted jobs"
        obj1 = GetOutputPoller( config )
        obj1.setup( None )
        obj1.algorithm( None )
        obj1.terminate( None )
        print "..finished."
        self.cleanDir( 'done' )
        print "..finished."
        


if __name__ == '__main__':
    unittest.main()

