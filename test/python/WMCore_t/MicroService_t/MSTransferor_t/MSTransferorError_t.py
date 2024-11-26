"""
Unit tests for MSTransferorError.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

# system modules
import unittest

# WMCore modules
from WMCore.MicroService.MSTransferor.MSTransferorError import MSTransferorStorageError, MSPILEUP_STORAGE_ERROR


class TransferorErrorTest(unittest.TestCase):
    "Unit test for TransferorError module"

    def testError(self):
        """Test MSTransferorError"""
        rec = {'workflow': 'testWorkflow'}

        # test custom emessage
        msg = 'test error'
        err = MSTransferorStorageError(msg, **rec)
        edict = err.error()
        self.assertEqual(edict['message'], msg)
        self.assertEqual(edict['code'], MSPILEUP_STORAGE_ERROR)
        self.assertEqual(edict['data'], rec)

        # test default assigned message
        err = MSTransferorStorageError('', **rec)
        edict = err.error()
        self.assertEqual(edict['message'], 'storage error')
        self.assertEqual(edict['code'], MSPILEUP_STORAGE_ERROR)
        self.assertEqual(edict['data'], rec)


if __name__ == '__main__':
    unittest.main()
