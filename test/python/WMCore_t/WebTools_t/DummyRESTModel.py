from WMCore.WebTools.RESTModel import RESTModel
from cherrypy import HTTPError

class DummyDAO1:
    """
    A DAO that has no arguments and does nothing but return 123
    """
    def execute(self):
        return 123

class DummyDAO2:
    """
    A DAO that takes a single argument
    """
    def execute(self, num):
        return {'num': num}
    
class DummyDAO3:
    """
    A DAO with keyword arguments 
    TODO: use this
    """
    def execute(self, num, thing=None):
        return {'num': num, 'thing': thing}

class DummyDAOFac:
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
        self.methods['GET'] = {'list':{'args':['int', 'str'],
                                        'call': self.list,
                                        'version': 2,
                                        'validation': [self.val_0,
                                                       self.val_1,
                                                       self.val_2, 
                                                       self.val_3,
                                                       self.val_4 
                                                       ]},
                                'list1':{'args':[],
                                         'call': self.list1,
                                         'version': 2,
                                         'validation': []},
                                         
                                'list2':{'args':['num0', 'num1', 'num2'],
                                         'call': self.list2,
                                         'version': 2,
                                         'validation': []},
                                         
                                'list3':{'args':['num0', 'num1', 'num2'],
                                         'call': self.list2,
                                         'version': 2,
                                         'validation': []}}
        self.daofactory = DummyDAOFac()
        self.addDAO('GET', 'data1', 'DummyDAO1', [])
        self.addDAO('GET', 'data2', 'DummyDAO2', ['num'])
        self.addDAO('GET', 'data3', 'DummyDAO3', ['num', 'thing'])
        
    def list(self, *args, **kwargs):
        """ if sanitise needed to be called method signater of callee should be 
            (*args, **kwargs) """
        input = self.sanitise_input(args, kwargs, method = 'list')
        return input

    def list1(self):
        """ test no argument case, return 'No argument' string """
        return 'No argument'
    
    def list2(self, num0, num1, num2):
        """ test multiple argment string return the dictionary of key: value pair of 
            the arguments """     
        return {'num0': num0, 'num1': num1, 'num2': num2}
    
    def list3(self, *args, **kwargs):
        """ test sanitise without any validation specified """
        input = self.sanitise_input(args, kwargs, method = 'list3')
        return input
    
    def val_0(self, input):
        # checks whether input is right number
        if len(input) != 2:
            raise HTTPError(404, 'val_5 failed: input length is not 2 -- (%s)' % len(input))
        return input
    
    def val_1(self, input):
        # Convert the input data to an int (will be a string), ignore if it 
        # fails as the next validation will kill that, and it makes the unit test
        # trickier...
        try:
            input['int'] = int(input['int'])
        except:
            pass
        # Checks its first input contains a int
        try:
            assert type(input['int']) == type(123) 
        except AssertionError:
            raise HTTPError(400, 'val_1 failed: %s not int' % type(input['int']))
        return input
    
    def val_2(self, input):
        # Checks its second input is a string
        try:
            assert type(input['str']) == type('abc') 
        except AssertionError:
            raise HTTPError(400, 'val_2 failed: %s not str' % type(input['str']))
        return input
    
    def val_3(self, input):
        # Checks the int is 123
        try:
            assert input['int'] == 123
        except AssertionError:
            raise HTTPError(400, 'val_3 failed: %s != 123' % input['int'])
        
        return input
    
    def val_4(self, input):
        # Checks the str is 'abc'
        try:
            assert input['str'] == 'abc'
        except AssertionError:
            raise HTTPError(400, 'val_4 failed: %s != "abc"' % input['str'])
        
        return input
    
        
        