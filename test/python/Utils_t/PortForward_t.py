#!/usr/bin/env python
"""
Unittests for PortForward
"""



import unittest

from Utils.PortForward import portForward, PortForward


class RequestHandler(object):
    def __init__(self, config=None, logger=None):
        super(RequestHandler, self).__init__()
        if not config:
            config = {}

    @portForward(8443)
    def request(self, url, params=None, headers=None, verb='GET',
                verbose=0, ckey=None, cert=None, doseq=True,
                encode=False, decode=False, cookie=None, uri=None):
        return url


class PortForwardTests(unittest.TestCase):
    """
    Unittest for PortForward decorator and class
    """

    def __init__(self, *args, **kwargs):
        super(PortForwardTests, self).__init__(*args, **kwargs)
        self.urlResultList = []
        self.urlTestList = ['https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Reprocessing/Cert_190456-195530_8TeV_08Jun2012ReReco_Collisions12_JSON.txt',
                            'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1',
                            'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?startkey=%5B%22announced%22%2C+0%5D&endkey=%5B%22announced%22%2C+1616016936%5D&descending=false&stale=update_after&include_docs=false',
                            'https://cmsweb.cern.ch:8443/reqmgr2/js/?f=utils.js&f=ajax_utils.js&f=md5.js&f=task_splitting.js',
                            'https://cmsweb.cern.ch:443/wmstatsserver/data/filtered_requests?mask=RequestStatus&mask=RequestType&mask=RequestPriority&mask=Campaign&mask=RequestNumEvents',
                            'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1',
                            'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?startkey=%5B%22announced%22%2C+0%5D&endkey=%5B%22announced%22%2C+1616016936%5D&descending=false&stale=update_after&include_docs=false',
                            'https://cmsweb.cern.ch/reqmgr2/js/?f=utils.js&f=ajax_utils.js&f=md5.js&f=task_splitting.js',
                            'https://cmsweb.cern.ch/wmstatsserver/data/filtered_requests?mask=RequestStatus&mask=RequestType&mask=RequestPriority&mask=Campaign&mask=RequestNumEvents',
                            b'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1',
                            b'https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?startkey=%5B%22announced%22%2C+0%5D&endkey=%5B%22announced%22%2C+1616016936%5D&descending=false&stale=update_after&include_docs=false',
                            b'https://cmsweb.cern.ch/reqmgr2/js/?f=utils.js&f=ajax_utils.js&f=md5.js&f=task_splitting.js',
                            b'https://cmsweb.cern.ch/wmstatsserver/data/filtered_requests?mask=RequestStatus&mask=RequestType&mask=RequestPriority&mask=Campaign&mask=RequestNumEvents']

        self.urlExpectedtList = ['https://cms-service-dqm.web.cern.ch/cms-service-dqm/CAF/certification/Collisions12/8TeV/Reprocessing/Cert_190456-195530_8TeV_08Jun2012ReReco_Collisions12_JSON.txt',
                                 'https://cmsweb.cern.ch:8443/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1',
                                 'https://cmsweb.cern.ch:8443/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?startkey=%5B%22announced%22%2C+0%5D&endkey=%5B%22announced%22%2C+1616016936%5D&descending=false&stale=update_after&include_docs=false',
                                 'https://cmsweb.cern.ch:8443/reqmgr2/js/?f=utils.js&f=ajax_utils.js&f=md5.js&f=task_splitting.js',
                                 'https://cmsweb.cern.ch:443/wmstatsserver/data/filtered_requests?mask=RequestStatus&mask=RequestType&mask=RequestPriority&mask=Campaign&mask=RequestNumEvents',
                                 'https://cmsweb.cern.ch:8443/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1',
                                 'https://cmsweb.cern.ch:8443/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?startkey=%5B%22announced%22%2C+0%5D&endkey=%5B%22announced%22%2C+1616016936%5D&descending=false&stale=update_after&include_docs=false',
                                 'https://cmsweb.cern.ch:8443/reqmgr2/js/?f=utils.js&f=ajax_utils.js&f=md5.js&f=task_splitting.js',
                                 'https://cmsweb.cern.ch:8443/wmstatsserver/data/filtered_requests?mask=RequestStatus&mask=RequestType&mask=RequestPriority&mask=Campaign&mask=RequestNumEvents',
                                 b'https://cmsweb.cern.ch:8443/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1',
                                 b'https://cmsweb.cern.ch:8443/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bystatusandtime?startkey=%5B%22announced%22%2C+0%5D&endkey=%5B%22announced%22%2C+1616016936%5D&descending=false&stale=update_after&include_docs=false',
                                 b'https://cmsweb.cern.ch:8443/reqmgr2/js/?f=utils.js&f=ajax_utils.js&f=md5.js&f=task_splitting.js',
                                 b'https://cmsweb.cern.ch:8443/wmstatsserver/data/filtered_requests?mask=RequestStatus&mask=RequestType&mask=RequestPriority&mask=Campaign&mask=RequestNumEvents']

    def testDecorator(self):
        requesHandler = RequestHandler()
        self.urlResultList = []
        for url in self.urlTestList:
            self.urlResultList.append(requesHandler.request(url))
        self.assertListEqual(self.urlResultList, self.urlExpectedtList)

    def testCallClass(self):
        portForwarder = PortForward(8443)
        self.urlResultList = []
        for url in self.urlTestList:
            self.urlResultList.append(portForwarder(url))
        self.assertListEqual(self.urlResultList, self.urlExpectedtList)


if __name__ == '__main__':
    unittest.main()
