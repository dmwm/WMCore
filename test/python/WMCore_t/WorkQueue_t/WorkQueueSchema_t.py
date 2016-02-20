from __future__ import absolute_import
from __future__ import print_function
import unittest

from .WorkQueueTestCase import WorkQueueTestCase

class WorkQueueSchemnaTest(WorkQueueTestCase):
    """
    _WorkQueueSchemnaTest_

    test schema creation and deletion
    """

    def testSchemaGeneration(self):
        print("test schema creation")

if __name__ == "__main__":
    unittest.main()
