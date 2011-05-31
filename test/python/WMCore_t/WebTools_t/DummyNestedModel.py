from WMCore.WebTools.NestedModel import NestedModel

class DummyNestedModel(NestedModel):
    def __init__(self, config):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        NestedModel.__init__(self, config)
        self.methods = {'GET':{
                           'foo': {
                                    'default':{'default_data':1234,
                                               'call':self.foo,
                                               'version': 1,
                                               'args': ['message'],
                                               'expires': 3600,
                                               'validation': []},
                                    'ping':{'default_data':1234,
                                           'call':self.ping,
                                           'version': 1,
                                           'args': [],
                                           'expires': 3600,
                                           'validation': []}}
                           }
                    }

    def foo(self, message = None):
        """
        Return a different simple message
        """
        if message:
            return 'foo ' + message
        else:
            return 'foo'

    def ping(self):
        """
        Return a simple message
        """
        return 'ping'
