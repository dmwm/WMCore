import unittest
from WMCore.BossAir.Plugins.MockPlugin import MockPlugin
from WMCore.BossAir.Plugins.BasePlugin import BossAirPluginException
from WMCore.Configuration import Configuration
from WMQuality.TestInit import TestInit
from datetime import datetime
from datetime import timedelta
import os


TEST_JOB_LEN = 60
config = Configuration()
config.section_('BossAir')
config.BossAir.section_('MockPlugin')
config.BossAir.MockPlugin.jobRunTime = TEST_JOB_LEN
config.BossAir.MockPlugin.fakeReport = os.path.join(os.path.dirname(__file__), 'FakeReport.pkl')
config.BossAir.MockPlugin.fakeReport = os.path.join(os.path.dirname(__file__), 'LogCollectFakeReport.pkl')


jobList = [{'status': 'Done', 'bulkid': None, 'cms_site_name': None, 'cache_dir': '/data/wmagent/osb/JobCache/mmascher_crab_MyAnalysis_110609_114309/Analysis/JobCollection_23_0/job_46', 'taskType': None, 'id': 1L, 'plugin': 'MockPlugin', 'gridid': None, 'userdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni', 'jobid': 46L, 'priority': None, 'retry_count': 0L, 'sandbox': None, 'globalState': 'Running', 'location': None, 'packageDir': None, 'status_time': None}]



class MockPluginTest(unittest.TestCase):
    def setUp(self):
        self.testinit = TestInit(__file__)
        self.workdir = self.testinit.generateWorkDir()
        jobList[0]['cache_dir'] = self.workdir

    def tearDown(self):
        self.testinit.delWorkDir()

    def testInit(self):
        wrongconfig = Configuration()
        wrongconfig.section_('BossAir')
        self.assertRaises( BossAirPluginException, MockPlugin, wrongconfig )

        wrongconfig.BossAir.section_('MockPlugin')
        self.assertRaises( BossAirPluginException, MockPlugin, wrongconfig )
        #The config does not contain fakeReport parameter
        self.assertRaises( BossAirPluginException, MockPlugin, wrongconfig )

        #The fakeReport does not exist
        wrongconfig.BossAir.MockPlugin.fakeReport = 'asdf'
        self.assertRaises( BossAirPluginException, MockPlugin, wrongconfig )

    def testTrack(self):
        mp = MockPlugin(config)

        #Check that the job has been scheduled
        self.assertEquals({}, mp.jobsScheduledEnd)

        # Don't be racy
        currentTime = datetime.now()
        #id is the only required parameter in the job dictionary
        res = mp.track( jobList, currentTime )
        self.assertTrue( 1L in mp.jobsScheduledEnd )
        #check scheduled end (N.B. this includes 20% of random time)
        scheduledEnd = mp.jobsScheduledEnd[1L]
        timeTillJob = scheduledEnd - currentTime
        self.assertTrue( timeTillJob >= timedelta(minutes = TEST_JOB_LEN - 1), \
                         "Time till Job %s !>= Delta %s" % (timeTillJob, \
                         timedelta(minutes = TEST_JOB_LEN - 1)))
        self.assertTrue( timeTillJob <= timedelta(minutes = TEST_JOB_LEN*120/100 + 1), \
                         "Time till Job %s !<= Delta %s" % (timeTillJob, \
                         timedelta(minutes = TEST_JOB_LEN * 120/100 + 1)) )
        #the job is running
        self.assertEquals( 'Running', res[0][0]['status'])
        self.assertEquals( 'Running', res[1][0]['status'])
        self.assertEquals( [], res[2])

        #the job is not running anymore
        mp.jobsScheduledEnd[1L] = datetime(1900,1,1)
        res = mp.track( jobList )
        self.assertEquals( [], res[0])
        self.assertEquals( 'Done', res[1][0]['status'])
        self.assertEquals( 'Done', res[2][0]['status'])

        del mp



if __name__ == '__main__':
    unittest.main()
