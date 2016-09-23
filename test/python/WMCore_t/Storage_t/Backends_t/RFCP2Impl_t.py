from __future__ import (print_function, division)

import unittest

from mock import mock, call, Mock

from WMCore.Storage.Backends.RFCP2Impl import RFCP2Impl


class RFCP2ImplTest(unittest.TestCase):
    def setUp(self):
        self.RFCP2Impl = RFCP2Impl()

    def testCreateSourceName(self):
        self.assertEqual("name", self.RFCP2Impl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.RFCP2Impl.createSourceName("", "file:////name"))

    @mock.patch('WMCore.Storage.Backends.RFCP2Impl.RFCP2Impl.run')
    def testCreateOutputDirectory_error0(self, mock_runCommand):
        mock_runCommand.return_value = 0
        self.RFCP2Impl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("rfstat dir/file > /dev/null ")

    @mock.patch('WMCore.Storage.Backends.RFCP2Impl.RFCP2Impl.run')
    def testCreateOutputDirectory_error0Exception(self, mock_runCommand):
        mock_runCommand.side_effect = Exception("Im in test, yay!")
        self.RFCP2Impl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("rfstat dir/file > /dev/null ")

    def testCreateOutputDirectory_error1Exception(self):
        self.RFCP2Impl.run = Mock()
        self.RFCP2Impl.run.side_effect = [1, Exception()]
        self.RFCP2Impl.createOutputDirectory("dir/file/test")
        self.RFCP2Impl.run.assert_has_calls([call("rfstat dir/file > /dev/null "),
                                             call("rfmkdir -m 775 -p dir/file")])

    @mock.patch('WMCore.Storage.Backends.RFCP2Impl.RFCP2Impl.run')
    def testCreateOutputDirectory_error1(self, mock_runCommand):
        mock_runCommand.return_value = 1
        self.RFCP2Impl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_has_calls([call("rfstat dir/file > /dev/null "),
                                          call("rfmkdir -m 775 -p dir/file")])

    def testCreateStageOutCommand_stageInTrue(self):
        self.RFCP2Impl.stageIn = True
        sourcePFN = "file://sourcePFNfile://"
        targetPFN = "targetPFNfile://"
        remotePFN = "file://sourcePFNfile://"
        localPFN = "targetPFNfile://"
        result = self.RFCP2Impl.createStageOutCommand(sourcePFN, targetPFN, options="test")
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN,
                                                                 options="test")
        self.assertEqual(expectedResults, result)

    def testCreateStageOutCommand_stageInFalse(self):
        self.RFCP2Impl.stageIn = False
        sourcePFN = "file://sourcePFNfile"
        targetPFN = "targetPFN"
        remotePFN = "targetPFN"
        localPFN = "file://sourcePFNfile"
        result = self.RFCP2Impl.createStageOutCommand(sourcePFN, targetPFN)
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN)
        self.assertEqual(expectedResults, result)

    def getResultForCreateStageOutCommand(self, sourcePFN, targetPFN, localPFN, remotePFN, options=None):
        result = "rfcp "
        if options != None:
            result += " %s " % options
        result += " %s " % sourcePFN
        result += " %s " % targetPFN
        result += "\nFILE_SIZE=`stat -c %s"
        result += " %s `;\n" % localPFN
        result += " echo \"Local File Size is: $FILE_SIZE\"; DEST_SIZE=`rfstat %s |" \
                  " grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && " \
                  "[ $FILE_SIZE == $DEST_SIZE ]; then exit 0; else echo " \
                  "\"Error: Size Mismatch between local and SE\"; exit 60311 ; fi " % (
            remotePFN)
        return result

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.RFCP2Impl.removeFile("file")
        mock_executeCommand.assert_called_with("stager_rm -M file ; nsrm file")
