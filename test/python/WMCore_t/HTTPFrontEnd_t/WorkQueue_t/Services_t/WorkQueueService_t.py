"""
Unittest file for WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueService.py

"""
import unittest

from WMCore.HTTPFrontEnd.WorkQueue.Services.WorkQueueService \
     import WorkQueueService

class WorkQueueServiceTest(unittest.TestCase):
    """
    Test only added server side  function not the client server
    communication.

    TODO: add more test if necessary
    (If there is no client api supported service,
     We need to test REST call from remote client.
     using i.e. Request, urllib.open(), etc )
    """
    def setUp(self):
        self.wqService = WorkQueueService("DummyModel")

    def testStatusValidation(self):
        input = {'before': '1', 'after': '2',
                 'elementIDs': ['1', '2']}
        output = self.wqService.statusValidation(input)
        convertedInput = {'before': 1, 'after': 2,
                          'elementIDs': [1, 2]}
        self.assertEqual(output, convertedInput)
        self.assertEqual(input, convertedInput)

if __name__ == '__main__':
    unittest.main()