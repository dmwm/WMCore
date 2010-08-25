from WMCore.WebTools.RESTModel import RESTModel

class DummyDAO1:
    def execute(self):
        return 123

class DummyDAO2:
    def execute(self, num):
        return {'num': num}

class DummyDAOFac:
    def __call__(self, classname='DummyDAO1'):
        dao = None
        if classname == 'DummyDAO1':
            dao = DummyDAO1()
        elif classname == 'DummyDAO2':
            dao = DummyDAO2()
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
                                        'validation': [self.val_1, 
                                                       self.val_2, 
                                                       self.val_3, 
                                                       self.val_4]},
                                'list1':{'args':[],
                                         'call': self.list1,
                                         'version': 2,
                                         'validation': []},
                                         
                                'list2':{'args':['num0', 'num1', 'num2'],
                                         'call': self.list2,
                                         'version': 2,
                                         'validation': []}}
        self.daofactory = DummyDAOFac()
        self.addDAO('GET', 'data1', 'DummyDAO1', [])
        self.addDAO('GET', 'data2', 'DummyDAO2', ['num'])
        
    def list(self, int, str):
        input = self.sanitise_input('list', int, str)
        return input

    def list1(self):
        return 'No argument'
    
    def list2(self, num0, num1, num2):
        input = self.sanitise_input(num0, num1, num3, 'list')
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
        assert type(input['int']) == type(123)
        return input
    
    def val_2(self, input):
        # Checks its second input is a string
        assert type(input['str']) == type('abc')
        return input
    
    def val_3(self, input):
        # Checks the int is 123
        assert input['int'] == 123
        return input
    
    def val_4(self, input):
        # Checks the str is 'abc'
        assert input['str'] == 'abc'
        return input