"""
"""
import unittest
import os
import logging
import logging.config
import socket
import time
import tempfile
import shutil
from httplib import HTTPException
from httplib import BadStatusLine, IncompleteRead

from nose.plugins.attrib import attr

from WMCore.Services.Service import Service
from WMCore.Services.Requests import Requests
from WMCore.Algorithms import Permissions
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
import cherrypy


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
    def makeRequest(self, uri=None, data={}, verb='GET', incoming_headers={},
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

        #self.cache_path = tempfile.mkdtemp()
        test_dict = {'logger': self.logger,
                     'endpoint': 'https://github.com/dmwm'}

        self.myService = Service(test_dict)

        test_dict['endpoint'] = 'http://cmssw-test.cvs.cern.ch/cgi-bin/cmssw.cgi'
        self.myService2 = Service(test_dict)
        self.testUrl = 'http://cern.ch'

        self.port = 8888
        cherrypy.config.update({'server.socket_port': self.port})


    def tearDown(self):
        testname = self.id().split('.')[-1]
        #shutil.rmtree(self.cache_path, ignore_errors = True)
        self.testInit.delWorkDir()

        if self._exc_info()[0] == None:
            self.logger.info('test "%s" passed' % testname)
        else:
            self.logger.info('test "%s" failed' % testname)

    def testClear(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/WMCore/blob/master/setup.py#L11')
        assert os.path.exists(f.name)
        f.close()

        self.myService.clearCache('testClear')
        assert not os.path.exists(f.name)

    def testClearAndRepopulate(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/WMCore/blob/master/setup.py#L11')
        assert os.path.exists(f.name)
        f.close()

        self.myService.clearCache('testClear')
        assert not os.path.exists(f.name)

        f = self.myService.refreshCache('testClear', '/WMCore/blob/master/setup.py#L11')
        assert os.path.exists(f.name)
        f.close()


    def testCachePath(self):
        cache_path = tempfile.mkdtemp()
        dict = {'logger': self.logger,
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cachepath' : cache_path,
                'req_cache_path': '%s/requests' % cache_path
                }
        service = Service(dict)
        # We append hostname to the cachepath, so that we can talk to two
        # services on different hosts
        self.assertEqual(service['cachepath'],
                         '%s/cmssw.cvs.cern.ch' % dict['cachepath'] )
        shutil.rmtree(cache_path, ignore_errors = True)

    @attr("integration")
    def testCacheLifetime(self):
        """Cache deleted if created by Service - else left alone"""
        dict = {'logger': self.logger,
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100}
        os.environ.pop('TMPDIR', None) # Mac sets this by default
        service = Service(dict)
        cache_path = service['cachepath']
        self.assertTrue(os.path.isdir(cache_path))
        del service
        self.assertFalse(os.path.exists(cache_path))

        cache_path = tempfile.mkdtemp()
        dict['cachepath'] = cache_path
        service = Service(dict)
        del service
        self.assertTrue(os.path.isdir(cache_path))
        Permissions.owner_readwriteexec(cache_path)

    def testCachePermissions(self):
        """Raise error if pre-defined cache permission loose"""
        cache_path = tempfile.mkdtemp()
        sub_cache_path = os.path.join(cache_path, 'cmssw.cvs.cern.ch')
        os.makedirs(sub_cache_path, 0o777)
        dict = {'logger': self.logger,
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100,
                'cachepath' : cache_path}
        self.assertRaises(AssertionError, Service, dict)

    def testCacheDuration(self):
        dict = {'logger': self.logger,
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100,
                #'cachepath' : self.cache_path,
                #'req_cache_path': '%s/requests' % self.cache_path
                }
        service = Service(dict)
        self.assertEqual( service['cacheduration'] ,  dict['cacheduration'] )

    def testNoCacheDuration(self):
        dict = {'logger': self.logger,
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': None,
                #'cachepath' : self.cache_path,
                #'req_cache_path': '%s/requests' % self.cache_path
                }
        service = Service(dict)
        self.assertEqual( service['cacheduration'] ,  dict['cacheduration'] )

    def testSocketTimeout(self):
        dict = {'logger': self.logger,
                'endpoint':'https://github.com/dmwm',
                'cacheduration': None,
                'timeout': 10,
                #'cachepath' : self.cache_path,
                #'req_cache_path': '%s/requests' % self.cache_path
                }
        service = Service(dict)
        deftimeout = socket.getdefaulttimeout()
        service.getData('%s/socketresettest' % self.testDir, '/WMCore/blob/master/setup.py#L11')
        assert deftimeout == socket.getdefaulttimeout()

    @attr("integration")
    def testStaleCache(self):

        dict = {'logger': self.logger,
                'endpoint':'http://cmssw.cvs.cern.ch',
                'cacheduration': 0.0002,
                'maxcachereuse': 0.001,
                'timeout': 10,
                'usestalecache': True,
                #'cachepath' : self.cache_path,
                #'req_cache_path': '%s/requests' % self.cache_path
                }
        service = Service(dict)
        cache = 'stalecachetest'

        # Start test from a clear cache
        service.clearCache(cache)

        cachefile = service.cacheFileName(cache)

        # first check that the exception raises when the file doesn't exist
        self.logger.info('first call to refreshCache - should fail')

        self.assertRaises(HTTPException, service.refreshCache, cache, '/lies')

        cacheddata = 'this data is mouldy'
        f = open(cachefile, 'w')
        f.write(cacheddata)
        f.close()

        self.logger.info('second call to refreshCache - should pass')
        data = service.refreshCache(cache, '/lies').read()
        self.assertEquals(cacheddata, data)

        # sleep a while so the file expires in the cache
        # FIXME: RACY
        time.sleep(2)
        self.logger.info('third call to refreshCache - should return stale cache')
        data = service.refreshCache(cache, '/lies').read()
        self.assertEquals(cacheddata, data)

        # sleep a while longer so the cache is dead
        # FIXME: RACY
        time.sleep(5)
        self.logger.info('fourth call to refreshCache - cache should be dead')
        self.assertRaises(HTTPException, service.refreshCache, cache, '/lies')

        # touch the file and expire it
        f = open(cachefile, 'w')
        f.write('foo')
        f.close()
        time.sleep(2)

        self.logger.info('fifth call to refreshCache - do not use stale cache')
        # now our service cache is less permissive, the following should fail
        service['usestalecache'] = False
        self.assertRaises(HTTPException, service.refreshCache, cache, '/lies')

        service.cacheFileName(cache)

    def testCacheFileName(self):
        """Hash url + data to get cache file name"""
        hashes = {}
        inputdata = [{}, {'fred' : 'fred'},
                     {'fred' : 'fred', 'carl' : [1, 2]},
                     {'fred' : 'fred', 'carl' : ["1", "2"]},
                     {'fred' : 'fred', 'carl' : ["1", "2"], 'jim' : {}}
                     ]
        for data in inputdata:
            thishash = self.myService.cacheFileName('bob', inputdata = data)
            thishash2 = self.myService2.cacheFileName('bob', inputdata = data)
            self.assertNotEqual(thishash, thishash2)
            self.assert_(thishash not in hashes, '%s is not unique' % thishash)
            self.assert_(thishash2 not in hashes,
                         '%s is not unique' % thishash2)
            hashes[thishash], hashes[thishash2] = None, None

    def testNoCache(self):
        """Cache disabled"""
        dict = {'logger': self.logger,
                'endpoint':'https://github.com/dmwm',
                'cachepath' : None,
                }
        service = Service(dict)

        self.assertEqual( service['cachepath'] ,  dict['cachepath'] )
        self.assertEqual( service['requests']['cachepath'] ,  dict['cachepath'] )
        self.assertEqual( service['requests']['req_cache_path'] ,  dict['cachepath'] )

        out = service.refreshCache('shouldntbeused', '/').read()
        self.assertTrue('html' in out)

    @attr("integration")
    def testTruncatedResponse(self):
        """
        _TruncatedResponse_

        """
        cherrypy.tree.mount(CrappyServer())
        cherrypy.engine.start()
        FORMAT = '%(message)s'
        logging.basicConfig(format=FORMAT)
        logger = logging.getLogger('john')
        test_dict = {'logger': self.logger,'endpoint':'http://127.0.0.1:%i/truncated' % self.port,
                     'usestalecache': True}
        myService = Service(test_dict)
        self.assertRaises(IncompleteRead, myService.getData, 'foo', '')
        cherrypy.engine.exit()
        cherrypy.engine.stop()

    @attr("integration")
    def testSlowResponse(self):
        """
        _SlowResponse_

        """
        cherrypy.tree.mount(SlowServer())
        cherrypy.engine.start()
        FORMAT = '%(message)s'
        logging.basicConfig(format=FORMAT)
        logger = logging.getLogger('john')
        test_dict = {'logger': self.logger,'endpoint':'http://127.0.0.1:%i/slow' % self.port,
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
        logger = logging.getLogger('john')
        test_dict = {'logger': self.logger,'endpoint':'http://127.0.0.1:%i/badstatus' % self.port,
                     'usestalecache': True}
        myService = Service(test_dict)
        # Have to fudge the status line in the Request object as cherrypy won't
        # Allow bad statuses to be raised
        myService['requests'] = CrappyRequest('http://bad.com', {})
        self.assertRaises(BadStatusLine, myService.getData, 'foo', '')

    @attr("integration")
    def testZ_InterruptedConnection(self):
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
        logger = logging.getLogger('john')
        test_dict = {'logger': self.logger,'endpoint':'http://127.0.0.1:%i/reg1/regular' % self.port,
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
