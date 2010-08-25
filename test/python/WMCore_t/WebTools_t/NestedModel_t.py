import unittest
from cherrypy import HTTPError

#decorator import for RESTServer setup
from WMQuality.WebTools.RESTServerSetup import serverSetup, getDefaultServerURL 
from WMQuality.WebTools.RESTClientAPI import methodTest

class NestedModelTest(unittest.TestCase):
    
    def setUp(self):
        self.dasFlag = False
        self.restModel = 'WMCore.WebTools.NestedModel'
        self.urlbase = getDefaultServerURL()
        
    def tearDown(self):
        self.dasFlag = None
        self.restModel = None
        self.urlbase = None
        
    @serverSetup
    def testOuterFooPass(self):
        
        print "test foo"
        
        verb ='GET'
        url = self.urlbase + '/foo'
        output={'code':200, 'data':'"foo"'}
        expireTime =3600
        methodTest(verb, url, output=output, expireTime=expireTime)
        
        url = self.urlbase + '/foo/test'
        output={'code':200, 'data':'"foo test"'}
        methodTest(verb, url, output=output, expireTime=expireTime)
        
        url = self.urlbase + '/foo'
        input = {'message': 'test'}
        output={'code':200, 'data':'"foo test"'}
        methodTest(verb, url, input= input, output=output, expireTime=expireTime)
        
    @serverSetup    
    def testInnerPingPass(self):
        
        verb ='GET'
        url = self.urlbase + '/foo/ping'
        output={'code':200, 'data':'"ping"'}
        expireTime =3600
        
        methodTest(verb, url, output=output, expireTime=expireTime)
    
    @serverSetup    
    def testOuterFooError(self):
        
        verb ='GET'
        url = self.urlbase + '/foo/123/567'
        output={'code':400}
        methodTest(verb, url, output=output)
        
    
    @serverSetup    
    def testInnerPingError(self):
        
        verb ='GET'
        url = self.urlbase + '/foo/123/ping'
        output={'code':400}
        methodTest(verb, url, output=output)
        
        url = self.urlbase + '/foo/ping/123'
        methodTest(verb, url, output=output)
        
        
if __name__ == "__main__":
    unittest.main() 