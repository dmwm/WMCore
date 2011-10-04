import unittest
from MockPlugin import MockPlugin
from WMCore.BossAir.Plugins.BasePlugin import BossAirPluginException
from WMCore.Configuration import Configuration
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
    def testInit(self):
        wrongconfig = Configuration()
        wrongconfig.section_('BossAir')
        try:
            mp = MockPlugin(wrongconfig)
        except BossAirPluginException:
            #The config does not contain MockPlugin section
            pass
        else:
            fail('Expected exception')


        wrongconfig.BossAir.section_('MockPlugin')
        try:
            mp = MockPlugin(wrongconfig)
        except BossAirPluginException:
            #The config does not contain fakeReport parameter
            pass
        else:
            fail('Expected exception')


        wrongconfig.BossAir.MockPlugin.fakeReport = 'asdf'
        try:
            mp = MockPlugin(wrongconfig)
        except BossAirPluginException:
            #The fakeReport does not exist
            pass
        else:
            fail('Expected exception')

        mp = MockPlugin(config)


    def testTrack(self):
        mp = MockPlugin(config)

        #Check that the job has been scheduled
        self.assertEquals({}, mp.jobsScheduledEnd)
        #id is the only required parameter in the job dictionary
        res = mp.track( jobList )
        self.assertTrue( mp.jobsScheduledEnd.has_key(1L) )
        #check scheduled end (N.B. this includes 20% of random time)
        scheduledEnd = mp.jobsScheduledEnd[1L]
        self.assertTrue( (scheduledEnd - datetime.now()) > timedelta(minutes = TEST_JOB_LEN) )
        self.assertTrue( (scheduledEnd - datetime.now()) < timedelta(minutes = TEST_JOB_LEN*120/100) )
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



if __name__ == '__main__':
    unittest.main()
