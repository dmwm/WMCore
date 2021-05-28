from __future__ import print_function
import logging
logging.basicConfig(level = logging.DEBUG)
import unittest
from mock import mock
from WMCore.Storage.Plugins.LCGImpl import LCGImpl as ourPlugin
import WMCore.Storage.Plugins.LCGImpl
moduleWeAreTesting = WMCore.Storage.Plugins.LCGImpl

from WMCore.Storage.StageOutError import StageOutError, StageOutFailure

from nose.plugins.attrib import attr

def getPluginObject(mock_os, mock_Popen, localSize, remoteSize):

    # Mock file size of local file
    mock_os.path.getsize.return_value = localSize

    process_mock = mock.Mock()
    # Mock remote file size
    attrs = {'communicate.return_value': ('0 0 0 0 %s' % remoteSize, 'error')}
    process_mock.configure_mock(**attrs)
    mock_Popen.return_value = process_mock

    # Actually run the test
    ourPlugin.runCommandFailOnNonZero = mock.Mock()
    ourPlugin.runCommandFailOnNonZero.return_value = (0,  None)
    testObject = ourPlugin()
    return testObject

class SRMV2ImplTest(unittest.TestCase):

    @attr("integration")
    @mock.patch('WMCore.Storage.Plugins.LCGImpl.subprocess.Popen')
    @mock.patch('WMCore.Storage.Plugins.LCGImpl.os')
    def testFailSrmCopy(self, mock_os, mock_Popen):
        
        mock_os.path.getsize.return_value = 9001

        # copy a file and have it fail
        testObject = ourPlugin()

        # copy normally and have it work
        self.assertRaises(StageOutError, testObject.doTransfer,'file:///store/NONEXISTANTSOURCE',
                              'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET',
                              True,
                              None,
                              None,
                              None,
                              None,
                              None)


    @attr("integration")
    @mock.patch('WMCore.Storage.Plugins.LCGImpl.subprocess.Popen')
    @mock.patch('WMCore.Storage.Plugins.LCGImpl.os')
    def testFailOnFileSize(self, mock_os, mock_Popen, localSize=9001, remoteSize=9002):

        # copy a file and have it fail file size check
        testObject = getPluginObject(mock_os, mock_Popen, localSize, remoteSize)

        # Do transfer and fail local<->remote file size check
        self.assertRaises(StageOutFailure, testObject.doTransfer,'file:///store/NONEXISTANTSOURCE',
                              'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET',
                              True,
                              None,
                              None,
                              None,
                              None,
                              None)

    @attr("integration")
    @mock.patch('WMCore.Storage.Plugins.LCGImpl.subprocess.Popen')
    @mock.patch('WMCore.Storage.Plugins.LCGImpl.os')
    def testWin(self, mock_os, mock_Popen, localSize=9001, remoteSize=9001):

        # copy a file and have it succeed
        testObject = getPluginObject(mock_os, mock_Popen, localSize, remoteSize)

        # Do transfer, match local<->remote file sizes and have it work
        newPfn = testObject.doTransfer('file:///store/NONEXISTANTSOURCE',
                              'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET',
                              True,
                              None,
                              None,
                              None,
                              None,
                              None)
        self.assertEqual(newPfn, 'srm://nonexistant.com/blah/?SFN=/store/NONEXISTANTTARGET')

if __name__ == "__main__":
    unittest.main()
