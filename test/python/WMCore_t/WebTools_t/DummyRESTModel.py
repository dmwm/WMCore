from builtins import map, range, object, str, bytes

from WMCore.WebTools.RESTModel import RESTModel, restexpose
from cherrypy import HTTPError

DUMMY_ROLE = "dummy"
DUMMY_GROUP = "dummies"
DUMMY_SITE = "dummyHome"

class DummyDAO1(object):
    """
    A DAO that has no arguments and does nothing but return 123
    """
    def execute(self):
        return 123

class DummyDAO2(object):
    """
    A DAO that takes a single argument
    """
    def execute(self, num):
        return {'num': num}

class DummyDAO3(object):
    """
    A DAO with keyword arguments
    TODO: use this
    """
    def execute(self, num, thing=None):
        return {'num': num, 'thing': thing}

class DummyDAOFac(object):
    """
    Something that replicates a Factory that loads our dummy DAO classes
    """
    def __call__(self, classname='DummyDAO1'):
        dao = None
        if classname == 'DummyDAO1':
            dao = DummyDAO1()
        elif classname == 'DummyDAO2':
            dao = DummyDAO2()
        elif classname == 'DummyDAO3':
            dao = DummyDAO3()
        return dao

class DummyRESTModel(RESTModel):
    def __init__(self, config):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        RESTModel.__init__(self, config)
        self.defaultExpires = config.default_expires

        self.methods = {'GET':{
                               'ping': {'default_data':1234,
                                        'call':self.ping,
                                        'version': 1,
                                        'args': [],
                                        'expires': 3600,
                                        'validation': []},
                               #'list1': {'call':self.list1,
                               #         'version': 1}
                               },
                        'POST':{
                               'echo': {'call':self.echo,
                                        'version': 1,
                                        'args': ['message'],
                                        'validation': []},
                               }
                         }

        self._addMethod('GET', 'gen', self.gen)

        self._addMethod('GET', 'list', self.list, args=['input_int', 'input_str'],
                              validation=[self.val_0,
                                          self.val_1,
                                          self.val_2,
                                          self.val_3,
                                          self.val_4 ],
                              version=2)

        self._addMethod('GET', 'list1', self.list1)
        self._addMethod('GET', 'list2', self.list2, args=['num0', 'num1', 'num2'])
        self._addMethod('GET', 'list3', self.list3, args=['a', 'b'])
        self._addMethod('POST', 'list3', self.list3, args=['a', 'b'])
        self._addMethod('PUT', 'list1', self.list1, secured = True,
                        security_params = {'role':DUMMY_ROLE,
                                           'group': DUMMY_GROUP,
                                           'site':DUMMY_SITE})

        # a will take list of numbers. i.e. a[1,2,3]
        self._addMethod('GET', 'listTypeArgs', self.listTypeArgs, args=['aList'],
                       validation = [self.listTypeValidate])

        self.daofactory = DummyDAOFac()
        self._addDAO('GET', 'data1', 'DummyDAO1', [])
        self._addDAO('GET', 'data2', 'DummyDAO2', ['num'])
        self._addDAO('GET', 'data3', 'DummyDAO3', ['num', 'thing'])

    @restexpose
    def ping(self):
        """
        Return a simple message
        """
        return 'ping'

    @restexpose
    def echo(self, *args, **kwargs):
        """
        Echo back the arguments sent to the call. If sanitise needed to be called
        explicitly (e.g. method not added via _addMethod) method signature of callee
        should be (*args, **kwargs).
        """
        input_data = self._sanitise_input(args, kwargs, 'echo')
        return input_data

    def gen(self):
        """Generator method which produce generator dicts"""
        data = ({'idx':i} for i in range(10))
        return data

    def list(self, input_int, input_str):
        return {'input_int':input_int, 'input_str':input_str}

    def list1(self):
        """ test no argument case, return 'No argument' string """
        return 'No argument'

    def list2(self, num0, num1, num2):
        """ test multiple argment string return the dictionary of key: value pair of
            the arguments """
        return {'num0': num0, 'num1': num1, 'num2': num2}

    def list3(self, *args, **kwargs):
        """ test sanitise without any validation specified """
        return kwargs

    def listTypeArgs(self, aList):
        """ test whether it handles ?aList=1&aList=2 types of query """
        return aList

    def listTypeValidate(self, request_input):
        if not isinstance(request_input["aList"], list):
            request_input["aList"] = [int(request_input["aList"])]
        else:
            request_input["aList"] = list(map(int, request_input["aList"]))
        return request_input

    def val_0(self, request_input):
        # checks whether request_input is right number
        if len(request_input) != 2:
            raise HTTPError(400, 'val_0 failed: request_input length is not 2 -- (%s)' % len(request_input))
        return request_input

    def val_1(self, request_input):
        # Convert the request_input data to an int (will be a string), ignore if it
        # fails as the next validation will kill that, and it makes the unit test
        # trickier...
        try:
            request_input['input_int'] = int(request_input['input_int'])
        except:
            pass
        # Checks its first request_input contains a int
        try:
            assert isinstance(request_input['input_int'], type(123))
        except AssertionError:
            raise AssertionError('val_1 failed: %s not int' % type(request_input['input_int']))
        return request_input

    def val_2(self, request_input):
        # Checks its second request_input is a string
        try:
            assert isinstance(request_input['input_str'], (str, bytes))
        except AssertionError:
            raise HTTPError(400, 'val_2 failed: %s not string or unicode' % type(request_input['input_str']))
        return request_input

    def val_3(self, request_input):
        # Checks the int is 123
        try:
            assert request_input['input_int'] == 123
        except AssertionError:
            raise HTTPError(400, 'val_3 failed: %s != 123' % request_input['input_int'])

        return request_input

    def val_4(self, request_input):
        # Checks the str is 'abc'
        try:
            assert request_input['input_str'] == 'abc'
        except AssertionError:
            raise HTTPError(400, 'val_4 failed: %s != "abc"' % request_input['input_str'])

        return request_input
