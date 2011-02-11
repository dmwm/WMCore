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

        self.applyList = [{'name': 'test_a', 'size': 2, 'list': [3, 4]},
                          {'name': 'test_a', 'size': 3, 'list': [5, 6, 7]},
                          {'name': 'test_d', 'size': 2, 'list': [1, 2]}]

        self.errorList = [{'error_url': "error_123", 'error': "123 error"},
                          {'error_url': "error_456", 'error': "456 error"}]


    def testcombineListOfDict(self):

        # normal combine test
        result = [{'name': 'test_a', 'error_key': 'error_123',
                   'list': [5, 6, 7], 'size': 3},
                  {'size': 1, 'list': [1], 'name': 'test_b',
                   'error_key': ['error_123', 'error_456']},
                  {'list': [1, 2], 'name': 'test_c', 'size': 2}]

        self.assertEqual(combineListOfDict('name', self.baseList, self.applyList),
                         result)

        # with combine function
        result = [{'name': 'test_a', 'error_key': 'error_123',
                   'list': [1, 3, 4, 5, 6, 7], 'size': 6},
                  {'size': 1, 'list': [1], 'name': 'test_b',
                   'error_key': ['error_123', 'error_456']},
                  {'list': [1, 2], 'name': 'test_c', 'size': 2}]

        self.assertEqual(combineListOfDict('name', self.baseList, self.applyList,
                                size = add, list = addToList), result)

        # test error case
        result =   [{'error': '123 error', 'size': 1, 'list': 1,
                     'name': 'test_a', 'error_key': 'error_123'},
                    {'error': '123 error, 456 error', 'size': 1, 'list': [1],
                     'name': 'test_b', 'error_key': ['error_123', 'error_456']},
                    {'list': [1, 2], 'name': 'test_c', 'size': 2}]

        self.assertEqual(combineListOfDict('name', self.baseList,
                            self.errorList, errorKey = 'error_key'), result)

        self.applyList.extend(self.errorList)

        # all combined
        result = [{'name': 'test_a', 'error': '123 error',
                   'error_key': 'error_123', 'list': [1, 3, 4, 5, 6, 7],
                   'size': 6},
                  {'error': '123 error, 456 error', 'size': 1, 'list': [1],
                   'name': 'test_b', 'error_key': ['error_123', 'error_456']},
                  {'list': [1, 2], 'name': 'test_c', 'size': 2}]

        self.assertEqual(combineListOfDict('name', self.baseList, self.applyList,
                                errorKey = 'error_key',
                                size = add, list = addToList),result)

if __name__ == '__main__':
    unittest.main()