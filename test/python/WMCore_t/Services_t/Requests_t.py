#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-
"""
Created on Aug 6, 2009

@author: meloam
"""
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()

import json
import os
import shutil
import tempfile
import time
import unittest
from http.client import HTTPException

import nose

import WMCore.Services.Requests as Requests
from WMCore.DataStructs.Job import Job as DataStructsJob
from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.Run import Run
from WMCore.Services.Requests import JSONRequests
from WMCore.WMInit import getWMBASE
from WMQuality.TestInit import TestInit
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig


class testRequestExceptions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.request_dict = {'req_cache_path': self.tmp}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test404Error(self):
        endp = "http://cmsweb.cern.ch"
        url = "/thispagedoesntexist/"
        req = Requests.Requests(endp, self.request_dict)
        for v in ['GET', 'POST']:
            self.assertRaises(HTTPException, req.makeRequest, url, verb=v)
        try:
            req.makeRequest(url, verb='GET')
        except HTTPException as e:
            # print e
            self.assertEqual(e.status, 404)

    def test404Error_with_pycurl(self):
        endp = "http://cmsweb.cern.ch"
        url = "/thispagedoesntexist/"
        idict = dict(self.request_dict)
        idict.update({'pycurl': 1})
        req = Requests.Requests(endp, idict)
        for v in ['GET', 'POST']:
            self.assertRaises(HTTPException, req.makeRequest, url, verb=v)
        try:
            req.makeRequest(url, verb='GET')
        except HTTPException as e:
            # print e
            self.assertEqual(e.status, 404)


# comment out so we don't ddos someone else's server
#    def test408Error(self):
#        endp = "http://bitworking.org/projects/httplib2/test"
#        url = "/timeout/timeout.cgi"
#        self.request_dict['timeout'] = 1
#        req = Requests.Requests(endp, self.request_dict) # takes 3 secs to respond
#        self.assertRaises(HTTPException, req.makeRequest, url, verb='GET',
#                          incoming_headers = {'cache-control': 'no-cache'})



class testRepeatCalls(RESTBaseUnitTest):
    def initialize(self):
        self.config = DefaultConfig()
        self.config.UnitTests.templates = getWMBASE() + '/src/templates/WMCore/WebTools'
        self.config.Webtools.section_('server')
        self.config.Webtools.server.socket_timeout = 1
        self.urlbase = self.config.getServerUrl()
        self.cache_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.cache_path, ignore_errors=True)
        self.rt.stop()

    def test10Calls(self):
        fail_count = 0
        req = Requests.Requests(self.urlbase, {'req_cache_path': self.cache_path})

        for i in range(0, 5):
            time.sleep(i)
            print('test %s starting at %s' % (i, time.time()))
            try:
                result = req.get('/', incoming_headers={'Cache-Control': 'no-cache'})
                self.assertEqual(False, result[3])
                self.assertEqual(200, result[1])
            except HTTPException as he:
                print('test %s raised a %s error' % (i, he.status))
                fail_count += 1
            except Exception as e:
                print('test %s raised an unexpected exception of type %s' % (i, type(e)))
                print(e)
                fail_count += 1
        if fail_count > 0:
            raise Exception('Test did not pass!')

    def test10Calls_with_pycurl(self):
        fail_count = 0
        idict = {'req_cache_path': self.cache_path, 'pycurl': 1}
        req = Requests.Requests(self.urlbase, idict)

        for i in range(0, 5):
            time.sleep(i)
            print('test %s starting at %s' % (i, time.time()))
            try:
                result = req.get('/', incoming_headers={'Cache-Control': 'no-cache'}, decode=True)
                self.assertEqual(False, result[3])
                self.assertEqual(200, result[1])
            except HTTPException as he:
                print('test %s raised a %s error' % (i, he.status))
                fail_count += 1
            except Exception as e:
                print('test %s raised an unexpected exception of type %s' % (i, type(e)))
                print(e)
                fail_count += 1
        if fail_count > 0:
            raise Exception('Test did not pass!')

    def testRecoveryFromConnRefused(self):
        """Connections succeed after server down"""
        import socket
        self.rt.stop()
        req = Requests.Requests(self.urlbase, {'req_cache_path': self.cache_path, 'pycurl': False})
        headers = {'Cache-Control': 'no-cache'}
        self.assertRaises(socket.error, req.get, '/', incoming_headers=headers)

        # now restart server and hope we can connect
        self.rt.start(blocking=False)
        result = req.get('/', incoming_headers=headers)
        self.assertEqual(result[3], False)
        self.assertEqual(result[1], 200)

    def testRecoveryFromConnRefused_with_pycurl(self):
        """Connections succeed after server down"""
        import pycurl
        self.rt.stop()
        idict = {'req_cache_path': self.cache_path, 'pycurl': 1}
        req = Requests.Requests(self.urlbase, idict)
        headers = {'Cache-Control': 'no-cache'}
        self.assertRaises(pycurl.error, req.get, '/', incoming_headers=headers, decode=True)

        # now restart server and hope we can connect
        self.rt.start(blocking=False)
        result = req.get('/', incoming_headers=headers, decode=True)
        self.assertEqual(result[3], False)
        self.assertEqual(result[1], 200)


class testJSONRequests(unittest.TestCase):
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        tmp = self.testInit.generateWorkDir()
        self.request = Requests.JSONRequests(idict={'req_cache_path': tmp})

    def roundTrip(self, data):
        encoded = self.request.encode(data)
        # print encoded
        # print encoded.__class__.__name__
        decoded = self.request.decode(encoded)
        # print decoded.__class__.__name__
        self.assertEqual(data, decoded)

    def roundTripLax(self, data):
        encoded = self.request.encode(data)
        decoded = self.request.decode(encoded)
        datakeys = list(data.keys())

        for k in decoded.keys():
            assert k in datakeys
            datakeys.pop(datakeys.index(k))
            # print 'the following keys were dropped\n\t',datakeys

    def testSet1(self):
        self.roundTrip(set([]))

    def testSet2(self):
        self.roundTrip(set([1, 2, 3, 4, Run(1)]))

    def testSet3(self):
        self.roundTrip(set(['a', 'b', 'c', 'd']))

    def testSet4(self):
        self.roundTrip(set([1, 2, 3, 4, 'a', 'b']))

    def testRun1(self):
        self.roundTrip(Run(1))

    def testRun2(self):
        self.roundTrip(Run(1, 1))

    def testRun3(self):
        self.roundTrip(Run(1, 2, 3))

    def testMask1(self):
        self.roundTrip(Mask())

    def testMask2(self):
        mymask = Mask()
        mymask['FirstEvent'] = 9999
        mymask['LastEvent'] = 999
        self.roundTrip(mymask)

    def testMask3(self):
        mymask = Mask()
        mymask['FirstEvent'] = 9999
        mymask['LastEvent'] = 999
        myjob = DataStructsJob()
        myjob["mask"] = mymask
        self.roundTrip(myjob)

    def testMask4(self):
        self.roundTrip({'LastRun': None, 'FirstRun': None, 'LastEvent': None,
                        'FirstEvent': None, 'LastLumi': None, 'FirstLumi': None})

    def testMask5(self):
        mymask = Mask()
        mymask['FirstEvent'] = 9999
        mymask['LastEvent'] = 999
        myjob = DataStructsJob()
        myjob["mask"] = mymask
        self.roundTripLax(myjob)

    def testMask6(self):
        mymask = Mask()
        myjob = DataStructsJob()
        myjob["mask"] = mymask
        self.roundTripLax(myjob)

    def testSpecialCharacterPasswords(self):
        url = 'http://username:p@ssw:rd@localhost:6666'
        req = JSONRequests(url)
        # the url is sanitized before persisted in the object
        self.assertEqual(req['host'], "http://localhost:6666")
        self.assertEqual(req.additionalHeaders['Authorization'], 'Basic dXNlcm5hbWU6cEBzc3c6cmQ=')


class TestRequests(unittest.TestCase):
    def testGetKeyCert(self):
        """test existance of key/cert"""
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            raise nose.SkipTest('Only run if an X509 proxy is present')
        os.environ.pop('X509_HOST_CERT', None)
        os.environ.pop('X509_HOST_KEY', None)
        os.environ.pop('X509_USER_CERT', None)
        os.environ.pop('X509_USER_KEY', None)
        req = Requests.Requests('https://cmsweb.cern.ch')
        key, cert = req.getKeyCert()
        self.assertNotEqual(None, key)
        self.assertNotEqual(None, cert)

    def testSecureWithProxy(self):
        """https with proxy"""
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            raise nose.SkipTest('Only run if an X509 proxy is present')
        os.environ.pop('X509_HOST_CERT', None)
        os.environ.pop('X509_HOST_KEY', None)
        os.environ.pop('X509_USER_CERT', None)
        os.environ.pop('X509_USER_KEY', None)
        req = Requests.Requests('https://cmsweb.cern.ch')
        out = req.makeRequest('/auth/trouble')
        self.assertEqual(out[1], 200)
        self.assertNotEqual(out[0].find('passed basic validation'), -1)
        self.assertNotEqual(out[0].find('certificate is a proxy'), -1)

    def testSecureWithProxy_with_pycurl(self):
        """https with proxy with pycurl"""
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            raise nose.SkipTest('Only run if an X509 proxy is present')
        os.environ.pop('X509_HOST_CERT', None)
        os.environ.pop('X509_HOST_KEY', None)
        os.environ.pop('X509_USER_CERT', None)
        os.environ.pop('X509_USER_KEY', None)
        req = Requests.Requests('https://cmsweb.cern.ch', {'pycurl': 1})
        out = req.makeRequest('/phedex/datasvc/json/prod/groups', decoder=True)
        self.assertEqual(out[1], 200)
        if not isinstance(json.loads(out[0]), dict):
            msg = 'wrong data type'
            raise Exception(msg)
        out = req.makeRequest('/auth/trouble', decoder=True)
        self.assertEqual(out[1], 200)
        self.assertNotEqual(out[0].find('passed basic validation'), -1)
        self.assertNotEqual(out[0].find('certificate is a proxy'), -1)

    def testSecureNoAuth(self):
        """https with no client authentication"""
        req = Requests.Requests('https://cmsweb.cern.ch')
        out = req.makeRequest('')
        self.assertEqual(out[1], 200)
        # we should get an html page in response
        self.assertNotEqual(out[0].find('html'), -1)

    def testSecureNoAuth_with_pycurl(self):
        """https with no client authentication"""
        req = Requests.Requests('https://cmsweb.cern.ch', {'pycurl': 1})
        out = req.makeRequest('', decoder=True)
        self.assertEqual(out[1], 200)
        # we should get an html page in response
        self.assertNotEqual(out[0].find('html'), -1)

    def testSecureOddPort(self):
        """https with odd port"""
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            raise nose.SkipTest('Only run if an X509 proxy is present')
        os.environ.pop('X509_HOST_CERT', None)
        os.environ.pop('X509_HOST_KEY', None)
        os.environ.pop('X509_USER_CERT', None)
        os.environ.pop('X509_USER_KEY', None)
        req = Requests.Requests('https://cmsweb.cern.ch:443')
        out = req.makeRequest('/auth/trouble')
        self.assertEqual(out[1], 200)
        self.assertNotEqual(out[0].find('passed basic validation'), -1)

    def testSecureOddPort_with_pycurl(self):
        """https with odd port"""
        proxy = os.environ.get('X509_USER_PROXY')
        if not proxy:
            raise nose.SkipTest('Only run if an X509 proxy is present')
        os.environ.pop('X509_HOST_CERT', None)
        os.environ.pop('X509_HOST_KEY', None)
        os.environ.pop('X509_USER_CERT', None)
        os.environ.pop('X509_USER_KEY', None)
        req = Requests.Requests('https://cmsweb.cern.ch:443', {'pycurl': 1})
        out = req.makeRequest('/auth/trouble', decoder=True)
        self.assertEqual(out[1], 200)
        self.assertNotEqual(out[0].find('passed basic validation'), -1)

    def testNoCache(self):
        """Cache disabled"""
        req = Requests.Requests('https://cmssdt.cern.ch/SDT/', {'cachepath': None})
        out = req.makeRequest('/', decoder=True)
        self.assertEqual(out[3], False)
        self.assertTrue('html' in out[0])

if __name__ == "__main__":
    unittest.main()
