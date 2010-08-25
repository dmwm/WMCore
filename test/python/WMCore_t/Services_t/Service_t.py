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
from WMCore.Services.Service import Service

class ServiceTest(unittest.TestCase):
    def setUp(self):
        """
        Setup for unit tests
        """
        testname = self.id().split('.')[-1]
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='service_unittests.log',
                    filemode='w')
        
        logger_name = 'Service%s' % testname.replace('test', '', 1)
        
        self.logger = logging.getLogger(logger_name)
        
        self.cache_path = tempfile.mkdtemp()
        dict = {'logger': self.logger, 
                'cachepath' : self.cache_path,
                'req_cache_path': '%s/requests' % self.cache_path,
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi'}
        
        self.myService = Service(dict)

        dict['endpoint'] = 'http://cmssw-test.cvs.cern.ch/cgi-bin/cmssw.cgi'
        self.myService2 = Service(dict)
        self.testUrl = 'http://cern.ch'

    def tearDown(self):
        testname = self.id().split('.')[-1]
        shutil.rmtree(self.cache_path, ignore_errors = True)

        if self._exc_info()[0] == None:
            self.logger.info('test "%s" passed' % testname)
        else:
            self.logger.info('test "%s" failed' % testname)
            
    def testClear(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/COMP/WMCORE/src/python/WMCore/Services/Service.py?view=markup')
        assert os.path.exists(f.name)
        f.close()
        
        self.myService.clearCache('testClear')
        assert not os.path.exists(f.name)

    def testClearAndRepopulate(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/COMP/WMCORE/src/python/WMCore/Services/Service.py?view=markup')
        assert os.path.exists(f.name)
        f.close()
        
        self.myService.clearCache('testClear')
        assert not os.path.exists(f.name)

        f = self.myService.refreshCache('testClear', '/COMP/WMCORE/src/python/WMCore/Services/Service.py?view=markup')
        assert os.path.exists(f.name)
        f.close()

        
    def testCachePath(self):
        dict = {'logger': self.logger, 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cachepath' : self.cache_path,
                'req_cache_path': '%s/requests' % self.cache_path}
        service = Service(dict)
        # We append hostname to the cachepath, so that we can talk to two
        # services on different hosts
        self.assertEqual(service['cachepath'], 
                         '%s/cmssw.cvs.cern.ch' % dict['cachepath'] )
    
    def testCacheDuration(self):
        dict = {'logger': self.logger, 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100,
                'cachepath' : self.cache_path,
                'req_cache_path': '%s/requests' % self.cache_path}
        service = Service(dict)
        self.assertEqual( service['cacheduration'] ,  dict['cacheduration'] )
        
    def testNoCacheDuration(self):
        dict = {'logger': self.logger, 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': None,
                'cachepath' : self.cache_path,
                'req_cache_path': '%s/requests' % self.cache_path}
        service = Service(dict)
        self.assertEqual( service['cacheduration'] ,  dict['cacheduration'] )
        
    def testSocketTimeout(self):
        dict = {'logger': self.logger, 
                'endpoint':'http://cmssw.cvs.cern.ch/',
                'cacheduration': None,
                'timeout': 10,
                'cachepath' : self.cache_path,
                'req_cache_path': '%s/requests' % self.cache_path}
        service = Service(dict)
        deftimeout = socket.getdefaulttimeout()
        service.getData('/tmp/socketresettest', '/cgi-bin/cmssw.cgi')
        assert deftimeout == socket.getdefaulttimeout()

    def testStaleCache(self):
        
        dict = {'logger': self.logger, 
                'endpoint':'http://cmssw.cvs.cern.ch',
                'cacheduration': 0.0002,
                'maxcachereuse': 0.001,
                'timeout': 10,
                'usestalecache': True,
                'cachepath' : self.cache_path,
                'req_cache_path': '%s/requests' % self.cache_path}
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
        time.sleep(2)
        self.logger.info('third call to refreshCache - should return stale cache')
        data = service.refreshCache(cache, '/lies').read()
        self.assertEquals(cacheddata, data)
        
        # sleep a while longer so the cache is dead
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


if __name__ == '__main__':
    unittest.main()
