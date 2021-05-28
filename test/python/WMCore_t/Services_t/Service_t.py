"""
"""
from builtins import object
from future import standard_library
standard_library.install_aliases()

from io import BytesIO
import logging
import logging.config
import os
import shutil
import socket
import tempfile
import time
import unittest
from http.client import BadStatusLine, IncompleteRead, HTTPException

import cherrypy
from nose.plugins.attrib import attr

from WMCore.Services.Requests import Requests
from WMCore.Services.Service import Service, isfile, cache_expired
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit


class CrappyServer(object):
    def truncated(self):
        cherrypy.response.headers['Content-Length'] = 500
        return "Hello World!"

    truncated.exposed = True


class SlowServer(object):
    def slow(self):
        time.sleep(300)
        return "Hello World!"

    slow.exposed = True


class CrappyRequest(Requests):
    def makeRequest(self, uri=None, data=None, verb='GET', incoming_headers=None,
                    encoder=True, decoder=True, contentType=None):
        # METAL \m/
        raise BadStatusLine(666)


class RegularServer(object):
    def regular(self):
        return "This is silly."

    regular.exposed = True


class BackupServer(object):
    def regular(self):
        return "This is nuts."

    regular.exposed = True


class ServiceTest(unittest.TestCase):
    def setUp(self):
        """
        Setup for unit tests
        """
        self.testInit = TestInit(__file__)
        self.testDir = self.testInit.generateWorkDir()
        testname = self.id().split('.')[-1]

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='service_unittests.log',
                            filemode='w')

        logger_name = 'Service%s' % testname.replace('test', '', 1)

        self.logger = logging.getLogger(logger_name)

        # self.cache_path = tempfile.mkdtemp()
        test_dict = {'logger': self.logger,
                     'endpoint': 'https://github.com/dmwm'}

        self.myService = Service(test_dict)

        test_dict['endpoint'] = 'http://cmssw-test.cvs.cern.ch/cgi-bin/cmssw.cgi'
        self.myService2 = Service(test_dict)
        self.testUrl = 'http://cern.ch'

        self.port = 8888
        cherrypy.config.update({'server.socket_port': self.port})

    def tearDown(self):
        self.testInit.delWorkDir()
        # There was old code here to see if the test passed and send a message to
        # self.logger.info It broke in 2.7, so if needed find a supported way to do it
        return

    def testIsFile(self):
        """
        Test the `isfile` utilitarian function
        """
        f = tempfile.NamedTemporaryFile(prefix="testIsFile", delete=True)
        self.assertTrue(isfile(f))
        f.close()
        self.assertTrue(isfile(f))

        strio = BytesIO()
        self.assertTrue(isfile(strio))
        strio.close()
        self.assertTrue(isfile(strio))

        self.assertFalse(isfile("/data/srv/alan.txt"))
        self.assertFalse(isfile(1))
        self.assertFalse(isfile(None))

    def testCacheExpired(self):
        """
        Test the `cache_expired` utilitarian function. Delta is in hours
        """
        # file-like object is always considered expired
        fcache = tempfile.NamedTemporaryFile(prefix="testIsFile", delete=True)
        self.assertTrue(cache_expired(fcache, delta=0))
        self.assertTrue(cache_expired(fcache, delta=100))
        fcache.close()
        self.assertTrue(cache_expired(fcache, delta=0))
        self.assertTrue(cache_expired(fcache, delta=100))

        # path to a file that does not exist, always expired
        newfile = fcache.name + 'testCacheExpired'
        self.assertTrue(cache_expired(newfile, delta=0))
        self.assertTrue(cache_expired(newfile, delta=100))

        # now create and write something to it
        with open(newfile, 'w') as f:
            f.write("whatever")

        self.assertFalse(cache_expired(newfile, delta=1))
        time.sleep(1)
        self.assertTrue(cache_expired(newfile, delta=0))
        self.assertFalse(cache_expired(newfile, delta=1))

    def testClear(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/WMCore/blob/master/setup.py#L11')
        self.assertTrue(os.path.exists(f.name))
        f.close()

        self.myService.clearCache('testClear')
        self.assertFalse(os.path.exists(f.name))

    def testClearAndRepopulate(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/WMCore/blob/master/setup.py#L11')
        self.assertTrue(os.path.exists(f.name))
        f.close()

        self.myService.clearCache('testClear')
        self.assertFalse(os.path.exists(f.name))

        f = self.myService.refreshCache('testClear', '/WMCore/blob/master/setup.py#L11')
        self.assertTrue(os.path.exists(f.name))
        f.close()

    def testCachePath(self):
        cache_path = tempfile.mkdtemp()
        myConfig = {'logger': self.logger,
                'endpoint': 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cachepath': cache_path,
                'req_cache_path': '%s/requests' % cache_path
                }
        service = Service(myConfig)
        # We append hostname to the cachepath, so that we can talk to two
        # services on different hosts
        self.assertEqual(service['cachepath'],
                         '%s/cmssw.cvs.cern.ch' % myConfig['cachepath'])
        shutil.rmtree(cache_path, ignore_errors=True)

    def testCacheLifetime(self):
        """Cache deleted if created by Service - else left alone"""
        myConfig = {'logger': self.logger,
                'endpoint': 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 24}
        os.environ.pop('TMPDIR', None)  # Mac sets this by default
        service = Service(myConfig)
        cache_path = service['cachepath']
        self.assertTrue(os.path.isdir(cache_path))
        del service
        self.assertFalse(os.path.exists(cache_path))

        cache_path = tempfile.mkdtemp()
        myConfig['cachepath'] = cache_path
        service = Service(myConfig)
        del service
        self.assertTrue(os.path.isdir(cache_path))

    def testCachePermissions(self):
        """Raise error if pre-defined cache permission loose"""
        cache_path = tempfile.mkdtemp()
        sub_cache_path = os.path.join(cache_path, 'cmssw.cvs.cern.ch')
        os.makedirs(sub_cache_path, 0o777)
        myConfig = {'logger': self.logger,
                'endpoint': 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100,
                'cachepath': cache_path}
        self.assertRaises(AssertionError, Service, myConfig)  # it has to be 0o700

    def testCacheDuration(self):
        myConfig = {'logger': self.logger,
                'endpoint': 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100
                }
        service = Service(myConfig)
        self.assertEqual(service['cacheduration'], myConfig['cacheduration'])

    def testNoCacheDuration(self):
        myConfig = {'logger': self.logger,
                'endpoint': 'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': None,
                # 'cachepath' : self.cache_path,
                # 'req_cache_path': '%s/requests' % self.cache_path
                }
        service = Service(myConfig)
        self.assertEqual(service['cacheduration'], myConfig['cacheduration'])

    def testSocketTimeout(self):
        myConfig = {'logger': self.logger,
                'endpoint': 'https://github.com/dmwm',
                'cacheduration': None,
                'timeout': 10,
                }
        service = Service(myConfig)
        service.getData('%s/socketresettest' % self.testDir, '/WMCore/blob/master/setup.py#L11')
        self.assertEqual(service['timeout'], myConfig['timeout'])

    def testStaleCache(self):
        myConfig = {'logger': self.logger,
                'endpoint': 'https://github.com/dmwm',
                'usestalecache': True,
                }
        service = Service(myConfig)
        service.getData('%s/socketresettest' % self.testDir, '/WMCore/blob/master/setup.py#L11')
        self.assertEqual(service['usestalecache'], myConfig['usestalecache'])

    def testUsingStaleCache(self):
        myConfig = {'logger': self.logger,
                'endpoint': 'https://cmssdt.cern.ch/SDT/',
                'cacheduration': 0.0005,  # cache file lasts 1.8 secs
                'timeout': 10,
                'usestalecache': True,
                # 'cachepath' : self.cache_path,
                # 'req_cache_path': '%s/requests' % self.cache_path
                }
        service = Service(myConfig)
        cache = 'stalecachetest'

        # Start test from a clear cache
        service.clearCache(cache)

        cachefile = service.cacheFileName(cache)

        self.logger.info('1st call to refreshCache - should fail, there is no cache file')
        self.assertRaises(HTTPException, service.refreshCache, cache, '/lies')

        cacheddata = 'this data is mouldy'
        with open(cachefile, 'w') as f:
            f.write(cacheddata)

        self.logger.info('2nd call to refreshCache - should pass, data comes from the valid cache')
        data = service.refreshCache(cache, '/lies').read()
        self.assertEqual(cacheddata, data)

        # power nap to avoid letting the cache expire
        time.sleep(1)
        self.logger.info('3rd call to refreshCache - should pass, cache is still valid')
        data = service.refreshCache(cache, '/lies').read()
        self.assertEqual(cacheddata, data)

        # sleep a while longer so the cache dies out
        time.sleep(2)
        self.logger.info('4th call to refreshCache - should fail, cache is dead now')
        self.assertRaises(HTTPException, service.refreshCache, cache, '/lies')

        # touch/renew the file again
        cacheddata = 'foo'
        with open(cachefile, 'w') as f:
            f.write(cacheddata)

        # disable usage of stale cache, so doesn't call the endpoint if cache is valid
        service['usestalecache'] = False
        self.logger.info('5th call to refreshCache - should pass, cache is still valid')
        data = service.refreshCache(cache, '/lies').read()
        self.assertEqual(cacheddata, data)

        # consider the cache dead
        service['cacheduration'] = 0
        time.sleep(1)
        self.logger.info('6th call to refreshCache - should fail, cache is dead now')
        self.assertRaises(HTTPException, service.refreshCache, cache, '/lies')

    def testCacheFileName(self):
        """Hash url + data to get cache file name"""
        hashes = {}
        inputdata = [{}, {'fred': 'fred'},
                     {'fred': 'fred', 'carl': [1, 2]},
                     {'fred': 'fred', 'carl': ["1", "2"]},
                     {'fred': 'fred', 'carl': ["1", "2"], 'jim': {}}
                     ]
        for data in inputdata:
            thishash = self.myService.cacheFileName('bob', inputdata=data)
            thishash2 = self.myService2.cacheFileName('bob', inputdata=data)
            self.assertNotEqual(thishash, thishash2)
            self.assertTrue(thishash not in hashes, '%s is not unique' % thishash)
            self.assertTrue(thishash2 not in hashes,
                            '%s is not unique' % thishash2)
            hashes[thishash], hashes[thishash2] = None, None

    def testNoCache(self):
        """Cache disabled"""
        myConfig = {'logger': self.logger,
                'endpoint': 'https://github.com/dmwm',
                'cachepath': None,
                }
        service = Service(myConfig)

        self.assertEqual(service['cachepath'], myConfig['cachepath'])
        self.assertEqual(service['requests']['cachepath'], myConfig['cachepath'])
        self.assertEqual(service['requests']['req_cache_path'], myConfig['cachepath'])

        out = service.refreshCache('shouldntbeused', '/').read()
        self.assertTrue('html' in out)

    @attr("integration")
    def notestTruncatedResponse(self):
        """
        _TruncatedResponse_

        """
        cherrypy.tree.mount(CrappyServer())
        cherrypy.engine.start()
        FORMAT = '%(message)s'
        logging.basicConfig(format=FORMAT)
        dummyLogger = logging.getLogger('john')
        test_dict = {'logger': self.logger, 'endpoint': 'http://127.0.0.1:%i/truncated' % self.port,
                     'usestalecache': True}
        myService = Service(test_dict)
        self.assertRaises(IncompleteRead, myService.getData, 'foo', '')
        cherrypy.engine.exit()
        cherrypy.engine.stop()

    @attr("integration")
    def notestSlowResponse(self):
        """
        _SlowResponse_

        """
        cherrypy.tree.mount(SlowServer())
        cherrypy.engine.start()
        FORMAT = '%(message)s'
        logging.basicConfig(format=FORMAT)
        dummyLogger = logging.getLogger('john')
        test_dict = {'logger': self.logger, 'endpoint': 'http://127.0.0.1:%i/slow' % self.port,
                     'usestalecache': True}
        myService = Service(test_dict)
        startTime = int(time.time())
        self.assertRaises(socket.timeout, myService.getData, 'foo', '')
        self.assertTrue(int(time.time()) - startTime < 130,
                        "Error: Timeout took too long")
        cherrypy.engine.exit()
        cherrypy.engine.stop()

    def testBadStatusLine(self):
        """
        _BadStatusLine_

        """
        FORMAT = '%(message)s'
        logging.basicConfig(format=FORMAT)
        dummyLogger = logging.getLogger('john')
        test_dict = {'logger': self.logger, 'endpoint': 'http://127.0.0.1:%i/badstatus' % self.port,
                     'usestalecache': True}
        myService = Service(test_dict)
        # Have to fudge the status line in the Request object as cherrypy won't
        # Allow bad statuses to be raised
        myService['requests'] = CrappyRequest('http://bad.com', {})
        self.assertRaises(BadStatusLine, myService.getData, 'foo', '')

    @attr("integration")
    def notestZ_InterruptedConnection(self):
        """
        _InterruptedConnection_

        What happens if we shut down the server while
        the connection is still active?

        Confirm that the cache works as expected
        """

        cherrypy.tree.mount(RegularServer(), "/reg1")
        cherrypy.engine.start()
        FORMAT = '%(message)s'
        logging.basicConfig(format=FORMAT)
        dummyLogger = logging.getLogger('john')
        test_dict = {'logger': self.logger, 'endpoint': 'http://127.0.0.1:%i/reg1/regular' % self.port,
                     'usestalecache': True, "cacheduration": 0.005}
        myService = Service(test_dict)
        self.assertRaises(HTTPException, myService.getData, 'foo', 'THISISABADURL')

        data = myService.refreshCache('foo', '')
        dataString = data.read()
        self.assertEqual(dataString, "This is silly.")
        data.close()

        # Now stop the server and confirm that it is down
        cherrypy.server.stop()
        self.assertRaises(socket.error, myService.forceRefresh, 'foo', '')

        # Make sure we can still read from the cache
        data = myService.refreshCache('foo', '')
        dataString = data.read()
        self.assertEqual(dataString, "This is silly.")
        data.close()

        # Mount a backup server
        del cherrypy.tree.apps['/reg1']
        cherrypy.tree.mount(BackupServer(), "/reg1")

        # Expire cache
        time.sleep(30)
        self.assertRaises(socket.error, myService.forceRefresh, 'foo', '')

        # get expired cache results while the server is down
        data = myService.refreshCache('foo', '')
        dataString = data.read()
        self.assertEqual(dataString, "This is silly.")
        data.close()

        # Restart server
        cherrypy.server.start()

        # Confirm new server is in place
        data = myService.refreshCache('foo', '')
        dataString = data.read()
        self.assertEqual(dataString, "This is nuts.")
        data.close()

        cherrypy.engine.exit()
        cherrypy.engine.stop()

        return


if __name__ == '__main__':
    unittest.main()
