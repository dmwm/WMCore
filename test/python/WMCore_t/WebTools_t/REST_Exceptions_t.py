from WMCore.WebTools.RESTModel import RESTModel, restexpose
from cherrypy import HTTPError
import unittest, logging, json
from WMQuality.WebTools.RESTServerSetup import cherrypySetup, DefaultConfig
from WMQuality.WebTools.RESTClientAPI import makeRequest, methodTest

class REST_Exceptions_t(RESTModel):
    def __init__(self, config):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        RESTModel.__init__(self, config)

        self.methods['GET'] = {}
        self.methods['GET']['generic_exception'] = {'args': [],
                                         'call': self.generic_exception,
                                         'version': 1}

        self._addMethod('GET', 'specific_400_exception', self.specific_400_exception)
        self._addMethod('GET', 'specific_500_exception', self.specific_500_exception)
        self._addMethod('GET', 'specific_404_exception', self.specific_404_exception)
        self._addMethod('GET', 'not_serialisable', self.not_serialisable)

    @restexpose
    def generic_exception(self):
        """
        Raise an exception - this will result in a 500 Server Error from the RESTAPI
        """
        assert 1 == 2, "1 does not equal 2"

    def specific_400_exception(self):
        """
        Raise an HTTP Error, this will be preserved and propagated to the client
        """
        raise HTTPError(400, 'I threw a 400')

    def specific_500_exception(self):
        """
        Raise an HTTP Error, this will be preserved and propagated to the client
        """
        raise HTTPError(500, 'I threw a 500')

    def specific_404_exception(self):
        """
        Raise an HTTP Error, this will be preserved and propagated to the client
        """
        raise HTTPError(404, 'I threw a 404')

    def not_serialisable(self):
        """
        Raise an exception in the formatter (complex numbers aren't json serialisable
        by default), this is caught and turned into a 500 Server Error by the RESTAPI
        """
        return complex(1,2)

test_config = DefaultConfig('WMCore_t.WebTools_t.REST_Exceptions_t')
test_config.Webtools.access_log_level = logging.WARNING
test_config.Webtools.error_log_level = logging.WARNING

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
# Disabling tests because the decorator doesn't work right
class RESTTestFAIL():

    def setUp(self):
        self.config = test_config
        self.dasFlag = False
        self.urlbase = self.config.getServerUrl()

    def tearDown(self):
        self.dasFlag = None
        self.urlbase = None

    @cherrypySetup(test_config)
    def testGenericException(self):
        """
        Method will raise an AssertionError and return 500
        """
        url = self.urlbase + 'generic_exception'
        response, expires = methodTest('GET', url, output={'code':500})
        assert json.loads(response)['message'] == "Server Error", 'got: %s' % json.loads(response)['message']
        assert json.loads(response)['type'] == "AssertionError", 'got: %s' % json.loads(response)['type']

    @cherrypySetup(test_config)
    def testSpecific400Exception(self):
        """
        Method will raise an HTTPError and return 400
        """
        url = self.urlbase + 'specific_400_exception'
        response, expires = methodTest('GET', url, output={'code':400})
        assert json.loads(response)['message'] == "I threw a 400", 'got: %s' % json.loads(response)['message']

    @cherrypySetup(test_config)
    def testSpecific404Exception(self):
        """
        Method will raise an HTTPError and return 404
        """
        url = self.urlbase + 'specific_404_exception'
        response, expires = methodTest('GET', url, output={'code':404})
        assert json.loads(response)['message'] == "I threw a 404", 'got: %s' % json.loads(response)['message']

    @cherrypySetup(test_config)
    def testSpecific500Exception(self):
        """
        Method will raise an HTTPError and return 500
        """
        url = self.urlbase + 'specific_500_exception'
        response, expires = methodTest('GET', url, output={'code':500})
        assert json.loads(response)['message'] == "I threw a 500", 'got: %s' % json.loads(response)['message']

    @cherrypySetup(test_config)
    def testNotSerialisableException(self):
        """
        Method will raise an EncodeError and return 500
        """
        url = self.urlbase + 'not_serialisable'
        response, expires = methodTest('GET', url, output={'code':500})
        assert json.loads(response)['message'] == "Server Error", 'got: %s' % json.loads(response)['message']
        assert json.loads(response)['type'] == "TypeError", 'got: %s' % json.loads(response)['type']

if __name__ == "__main__":
    unittest.main()
