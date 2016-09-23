from __future__ import (print_function, division)

import unittest

from mock import mock, call, Mock

from WMCore.Storage.Backends.RFCPRALImpl import RFCPRALImpl


class RFCPRALImplTest(unittest.TestCase):
    def setUp(self):
        self.RFCPRALImpl = RFCPRALImpl()

    def testCreateSourceName(self):
        self.assertEqual("name", self.RFCPRALImpl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.RFCPRALImpl.createSourceName("", "file:////name"))


    @mock.patch('WMCore.Storage.Backends.RFCPRALImpl.RFCPRALImpl.run')
    def testCreateOutputDirectory_error0(self, mock_runCommand):
        mock_runCommand.return_value = 0
        self.RFCPRALImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("rfstat dir/file > /dev/null ")

    @mock.patch('WMCore.Storage.Backends.RFCPRALImpl.RFCPRALImpl.run')
    def testCreateOutputDirectory_error0Exception(self, mock_runCommand):
        mock_runCommand.side_effect = Exception("Im in test, yay!")
        self.RFCPRALImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("rfstat dir/file > /dev/null ")

    def testCreateOutputDirectory_error1Exception(self):
        self.RFCPRALImpl.run = Mock()
        self.RFCPRALImpl.run.side_effect = [1, Exception()]
        self.RFCPRALImpl.createOutputDirectory("dir/file/test")
        self.RFCPRALImpl.run.assert_has_calls([call("rfstat dir/file > /dev/null "),
                                             call("rfmkdir -m 775 -p dir/file")])

    @mock.patch('WMCore.Storage.Backends.RFCPRALImpl.RFCPRALImpl.run')
    def testCreateOutputDirectory_error1(self, mock_runCommand):
        mock_runCommand.return_value = 1
        self.RFCPRALImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_has_calls([call("rfstat dir/file > /dev/null "),
                                          call("rfmkdir -m 775 -p dir/file")])

    def testCreateStageOutCommand_stageInTrue(self):
        self.RFCPRALImpl.stageIn = True
        sourcePFN = "file://sourcePFNfile://"
        targetPFN = "targetPFNfile://"
        remotePFN = "file://sourcePFNfile://"
        localPFN = "targetPFNfile://"
        result = self.RFCPRALImpl.createStageOutCommand(sourcePFN, targetPFN, options="test")
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN,
                                                                 options="test")
        self.assertEqual(expectedResults, result)

    def testCreateStageOutCommand_stageInFalse(self):
        self.RFCPRALImpl.stageIn = False
        sourcePFN = "file://sourcePFNfile"
        targetPFN = "targetPFN"
        remotePFN = "targetPFN"
        localPFN = "file://sourcePFNfile"
        result = self.RFCPRALImpl.createStageOutCommand(sourcePFN, targetPFN)
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

    def testParseCastorPath_castorRegex(self):
        parsed=self.RFCPRALImpl.parseCastorPath("//castor/ads.rl.ac.uk/test//test//test")
        expected="/castor/ads.rl.ac.uk/test/test/test"
        self.assertEqual(expected,parsed)

    def testParseCastorPath_rfioRegex(self):
        parsed=self.RFCPRALImpl.parseCastorPath("rfio:asadad//castor/ads.rl.ac.uk/test//test//test")
        expected="/castor/ads.rl.ac.uk/test/test/test"
        self.assertEqual(expected,parsed)

    def testParseCastorPath_rfioQuestionMark(self):
        parsed=self.RFCPRALImpl.parseCastorPath("rfio:asadad//castor/ads.rl.ac.uk/test//test?//test")
        expected="/castor/ads.rl.ac.uk/test/test"
        self.assertEqual(expected,parsed)

    def testParseCastorPath_noRegex(self):
        parsed=self.RFCPRALImpl.parseCastorPath("test//a/")
        expected="test/a/"
        self.assertEqual(expected,parsed)
        parsed=self.RFCPRALImpl.parseCastorPath("test/")
        expected="test/"
        self.assertEqual(expected,parsed)

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.RFCPRALImpl.removeFile("fileName")
        mock_executeCommand.assert_called_with("stager_rm -M fileName ; nsrm fileName")

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile_regex(self, mock_executeCommand):
        self.RFCPRALImpl.removeFile("//castor/ads.rl.ac.uk/prod/cms/store/unmerged//testas")
        expected="/castor/ads.rl.ac.uk/prod/cms/store/unmerged/testas"
        mock_executeCommand.assert_called_with("stager_rm -S cmsTemp -M %s ; nsrm %s" %(expected, expected))



