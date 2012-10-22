import unittest
from WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter \
    import combineListOfDict, add, addToList

class DataFormatterTest(unittest.TestCase):
    """
    TestCase for RestServer and RestClient module
    """
    def setUp(self):

        self.baseList = [{'name': 'test_a', 'size': 1, 'list': 1,
                          'error_key': "error_123"},
                         {'name': 'test_b', 'size': 1, 'list': [1],
                          'error_key': ["error_123", "error_456"]},
                         {'name': 'test_c', 'size': 2, 'list': [1, 2]}]

        self.applyList1 = [{'name': 'test_a', 'size': 2, 'list': [3, 4]},
                          {'name': 'test_d', 'size': 2, 'list': [1, 2]}]

        self.applyList2 = [{'name': 'test_a', 'size': 3, 'list': [5, 6, 7]}]

        self.errorList = [{'error_url': "error_123", 'error': "123 error"},
                          {'error_url': "error_456", 'error': "456 error"}]


    def testcombineListOfDict(self):

        # normal combine test

        result =  [{'name': 'test_a', 'error_key': 'error_123',
                    'list': [5, 6, 7], 'size': 3},
                   {'list': [1, 2], 'name': 'test_d', 'size': 2},
                   {'list': [1, 2], 'name': 'test_c', 'size': 2},
                   {'error_key': ['error_123', 'error_456'],
                    'list': [1], 'name': 'test_b', 'size': 1}]
        interList = combineListOfDict('name', self.baseList, self.applyList1)

        self.assertEqual(combineListOfDict('name', interList, self.applyList2),
                         result)

        # with combine function
        result = [{'name': 'test_a', 'error_key': 'error_123',
                   'list': [1, 3, 4, 5, 6, 7], 'size': 6},
                  {'list': [1, 2], 'name': 'test_d', 'size': 2},
                  {'list': [1, 2], 'name': 'test_c', 'size': 2},
                  {'size': 1, 'list': [1], 'name': 'test_b',
                   'error_key': ['error_123', 'error_456']}]

        interList = combineListOfDict('name', self.baseList, self.applyList1,
                                      size = add, list = addToList)

        self.assertEqual(combineListOfDict('name', interList, self.applyList2,
                                size = add, list = addToList), result)

        # test error case
        result =   [{'list': [1, 2], 'name': 'test_c', 'size': 2},
                    {'error_key': ['error_123', 'error_456'],
                     'size': 1, 'list': [1], 'name': 'test_b',
                     'error': '123 error, 456 error'},
                     {'error': '123 error', 'size': 1, 'list': 1,
                     'name': 'test_a', 'error_key': 'error_123'}]

        self.assertEqual(combineListOfDict('name', self.baseList,
                            self.errorList, errorKey = 'error_key'), result)

        # all combined
        self.applyList1.extend(self.errorList)
        result = [{'name': 'test_a', 'error': '123 error',
                   'error_key': 'error_123', 'list': [1, 3, 4, 5, 6, 7],
                   'size': 6},
                   {'list': [1, 2], 'name': 'test_d', 'size': 2},
                  {'list': [1, 2], 'name': 'test_c', 'size': 2},
                  {'error': '123 error, 456 error', 'size': 1, 'list': [1],
                   'name': 'test_b', 'error_key': ['error_123', 'error_456']}]

        interList = combineListOfDict('name', self.baseList, self.applyList1,
                                      errorKey = 'error_key',
                                      size = add, list = addToList)

        self.assertEqual(combineListOfDict('name', interList, self.applyList2,
                                errorKey = 'error_key',
                                size = add, list = addToList),result)

if __name__ == '__main__':
    unittest.main()
