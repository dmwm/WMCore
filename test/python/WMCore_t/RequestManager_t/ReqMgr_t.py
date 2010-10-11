from WMCore.Services.Requests import JSONRequests
import WMCore_t.RequestManager_t.FakeRequests as FakeRequests
import unittest
import simplejson as json
import WMCore.RequestManager.RequestMaker.WMWorkloadCache as WMWorkloadCache
from httplib import HTTPException
import urllib
import time

class TestReqMgr(unittest.TestCase):
    def setUp(self):
        reqMgrHost = 'http://cmssrv49.fnal.gov:8585'
        self.jsonSender = JSONRequests(reqMgrHost)
        self.jsonSender.delete('/reqMgr/user/me')
        #self.requestTypes = ['ReReco', 'StoreResults', 'CmsGen', 'Reco']
        #self.requestTypes = ['ReReco', 'MonteCarlo']
        self.requestTypes = ['ReReco', 'MonteCarlo', 'FileProcessing']

        self.assertFalse('me' in self.jsonSender.get('/reqMgr/user')[0])
        self.assertEqual(self.jsonSender.put('/reqMgr/user/me?email=me@my.com')[1], 200)
        self.assertTrue('me' in self.jsonSender.get('/reqMgr/user')[0])

        self.jsonSender.delete('/reqMgr/group/PeopleLikeMe')
        self.assertFalse('PeopleLikeMe' in self.jsonSender.get('/reqMgr/group')[0])
        self.assertEqual(self.jsonSender.put('/reqMgr/group/PeopleLikeMe')[1], 200)
        self.assertTrue( 'PeopleLikeMe' in self.jsonSender.get('/reqMgr/group')[0])

        self.jsonSender.put('/reqMgr/group/PeopleLikeMe/me')
        self.assertTrue('me' in self.jsonSender.get('/reqMgr/group/PeopleLikeMe')[0])
        self.assertTrue('PeopleLikeMe' in self.jsonSender.get('/reqMgr/group?user=me')[0])

        self.jsonSender.delete(urllib.quote('/reqMgr/team/White Sox'))
        self.assertFalse('White Sox' in self.jsonSender.get('/reqMgr/team')[0])
        self.assertEqual(self.jsonSender.put(urllib.quote('/reqMgr/team/White Sox'))[1], 200)
        self.assertTrue('White Sox' in self.jsonSender.get('/reqMgr/team')[0])

        # some foreign key stuff to dealwith
        #self.assertFalse('CMSSW_X_Y_Z' in self.jsonSender.get('/reqMgr/version')[0])
        self.assertTrue(self.jsonSender.put('/reqMgr/version/CMSSW_X_Y_Z')[1] == 200)
        self.assertTrue('CMSSW_X_Y_Z' in self.jsonSender.get('/reqMgr/version')[0])

    def testRequest(self):
        for requestType in self.requestTypes:
            requestSchema = FakeRequests.fakeRequest(requestType)
            print str(requestSchema)
            requestName = requestSchema['RequestName']
            self.assertRaises(HTTPException, self.jsonSender.delete, '/reqMgr/request/'+requestName)
            self.assertEqual(self.jsonSender.put('/reqMgr/request/'+requestName, requestSchema)[1], 200)

            self.assertEqual(self.jsonSender.get('/reqMgr/request/'+requestName)[0]['RequestName'], requestName)
            self.assertTrue(requestName in self.jsonSender.get('/reqMgr/user/me')[0])

            # jusrt use the ReRecoRequest for these tests
            self.assertTrue(self.jsonSender.put('/reqMgr/request/%s?priority=5' % requestName)[1] == 200)
            # default priority of group and user of 1
            self.assertEqual(self.jsonSender.get('/reqMgr/request/'+requestName)[0]['RequestPriority'], 5+2)
            self.jsonSender.put('/reqMgr/request/%s?status=assignment-approved' % requestName)
            # only certain transitions allowed
            #self.assertEqual(self.jsonSender.put('/reqMgr/request/%s?status=running' % requestName)[1], 400)
            self.assertRaises(HTTPException, self.jsonSender.put,'/reqMgr/request/%s?status=running' % requestName)
            request = self.jsonSender.get('/reqMgr/request/'+requestName)[0]
            self.assertEqual(request['RequestStatus'], 'assignment-approved')

            self.assertTrue(self.jsonSender.put(urllib.quote('/reqMgr/assignment/White Sox/'+requestName))[1] == 200)
            requestsAndSpecs = self.jsonSender.get(urllib.quote('/reqMgr/assignment/White Sox'))[0]
            self.assertTrue(requestName in requestsAndSpecs.keys())
            workloadHelper = WMWorkloadCache.loadFromURL(requestsAndSpecs[requestName])
            self.assertEqual(workloadHelper.getOwner()['Requestor'], "me")
            self.assertTrue(self.jsonSender.get('/reqMgr/assignment?request='+requestName)[0] == ['White Sox'])

            agentUrl = 'http://cmssrv96.fnal.gov/workqueue'
            self.jsonSender.put('/reqMgr/workQueue/%s?url=%s'% (requestName, urllib.quote(agentUrl)) )
            self.assertEqual(self.jsonSender.get('/reqMgr/workQueue/'+requestName)[0][0], agentUrl)
            request = self.jsonSender.get('/reqMgr/request/'+requestName)[0]
            self.assertEqual(request['RequestStatus'], 'acquired')

            self.jsonSender.post('/reqMgr/request/%s?events_written=10&files_merged=1' % requestName)
            self.jsonSender.post('/reqMgr/request/%s?events_written=20&files_merged=2&percent_success=99.9' % requestName)
            request = self.jsonSender.get('/reqMgr/request/'+requestName)[0]
            self.assertEqual(len(request['RequestUpdates']), 2)
            self.assertEqual(request['RequestUpdates'][0]['files_merged'], 1)
            self.assertEqual(request['RequestUpdates'][1]['events_written'], 20)
            self.assertEqual(request['RequestUpdates'][1]['percent_success'], 99.9)

            message = "The sheriff is near"
            jsonMessage = json.dumps(message)
            self.jsonSender.put('/reqMgr/message/'+requestName, message)
            #messages = self.jsonSender.get('/reqMgr/message/'+requestName)
            #self.assertEqual(messages[0][0][0], message)
            for status in ['running', 'completed']:
              self.jsonSender.put('/reqMgr/request/%s?status=%s' % (requestName, status))


    def tearDown(self):
        for requestType in self.requestTypes:
            try: 
                self.jsonSender.delete('/reqMgr/request/Test'+requestType)
            except Exception, ex:
                print "EXCEPTION"
                print ex.result
        self.jsonSender.delete('/reqMgr/user/me')
        self.jsonSender.delete('/reqMgr/group/PeopleLikeMe')
        self.jsonSender.delete('/reqMgr/version/CMSSW_X_Y_Z')

if __name__=='__main__':
    unittest.main()
