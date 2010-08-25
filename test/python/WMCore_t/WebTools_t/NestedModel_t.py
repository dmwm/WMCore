import unittest
from cherrypy import HTTPError

#decorator import for RESTServer setup
from RESTServerSetup import serverSetup 
from RESTClientAPI import methodTest

class NestedModelTest(unittest.TestCase):
    
    def setUp(self):
        self.dasFlag = False
        self.restModel = 'WMCore.WebTools.NestedModel'
    
    def tearDown(self):
        self.dasFlag = None
        self.restModel = None
    
    @serverSetup
    def testOuterFooPass(self):
        
        print "test foo"
        
        verb ='GET'
        url ='/rest/foo'
        output={'code':200, 'data':'"foo"'}
        expireTime =3600
        methodTest(verb, url, output=output, expireTime=expireTime)
        
        url ='/rest/foo/test'
        output={'code':200, 'data':'"foo test"'}
        methodTest(verb, url, output=output, expireTime=expireTime)
        
        url ='/rest/foo'
        input = {'message': 'test'}
        output={'code':200, 'data':'"foo test"'}
        methodTest(verb, url, input= input, output=output, expireTime=expireTime)
        
    @serverSetup    
    def testInnerPingPass(self):
        
        verb ='GET'
        url ='/rest/foo/ping'
        output={'code':200, 'data':'"ping"'}
        expireTime =3600
        
        methodTest(verb, url, output=output, expireTime=expireTime)
    
    @serverSetup    
    def testOuterFooError(self):
        
        verb ='GET'
        url ='/rest/foo/123/567'
        output={'code':400}
        methodTest(verb, url, output=output)
        
    
    @serverSetup    
    def testInnerPingError(self):
        
        verb ='GET'
        url ='/rest/foo/123/ping'
        output={'code':400}
        methodTest(verb, url, output=output)
        
        url ='/rest/foo/ping/123'
        methodTest(verb, url, output=output)
        
        
if __name__ == "__main__":
    unittest.main() 