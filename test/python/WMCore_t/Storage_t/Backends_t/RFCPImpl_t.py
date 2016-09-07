from __future__ import (print_function, division)

import unittest

from mock import mock, call, Mock

from WMCore.Storage.Backends.RFCPImpl import RFCPImpl


class RFCPImpllTest(unittest.TestCase):
    def setUp(self):
        self.RFCPImpl = RFCPImpl()

    def testCreateSourceName(self):
        self.assertEqual("name", self.RFCPImpl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.RFCPImpl.createSourceName("", "file:////name"))


    @mock.patch('WMCore.Storage.Backends.RFCPImpl.RFCPImpl.run')
    def testCreateOutputDirectory_error0(self, mock_runCommand):
        mock_runCommand.return_value = 0
        self.RFCPImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("rfstat \"dir/file\" > /dev/null ")

    @mock.patch('WMCore.Storage.Backends.RFCPImpl.RFCPImpl.run')
    def testCreateOutputDirectory_error0Exception(self, mock_runCommand):
        mock_runCommand.side_effect = Exception("Im in test, yay!")
        self.RFCPImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("rfstat \"dir/file\" > /dev/null ")

    def testCreateOutputDirectory_error1Exception(self):
        self.RFCPImpl.run = Mock()
        self.RFCPImpl.run.side_effect = [1, Exception()]
        self.RFCPImpl.createOutputDirectory("dir/file/test")
        self.RFCPImpl.run.assert_has_calls([call("rfstat \"dir/file\" > /dev/null "),
                                             call("rfmkdir -m 775 -p dir/file")])

    @mock.patch('WMCore.Storage.Backends.RFCPImpl.RFCPImpl.run')
    def testCreateOutputDirectory_error1(self, mock_runCommand):
        mock_runCommand.return_value = 1
        self.RFCPImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_has_calls([call("rfstat \"dir/file\" > /dev/null "),
                                          call("rfmkdir -m 775 -p dir/file")])


    def testCreateStageOutCommand_stageInTrue(self):
        self.RFCPImpl.stageIn = True
        sourcePFN = "file://sourcePFNfile://"
        targetPFN = "targetPFNfile://"
        remotePFN = "file://sourcePFNfile://"
        localPFN = "targetPFNfile://"
        result = self.RFCPImpl.createStageOutCommand(sourcePFN, targetPFN, options="test")
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN,
                                                                 options="test")
        self.assertEqual(expectedResults, result)

    def testCreateStageOutCommand_stageInFalse(self):
        self.RFCPImpl.stageIn = False
        sourcePFN = "file://sourcePFNfile"
        targetPFN = "targetPFN"
        remotePFN = "targetPFN"
        localPFN = "file://sourcePFNfile"
        result = self.RFCPImpl.createStageOutCommand(sourcePFN, targetPFN)
        expectedResults = self.getResultForCreateStageOutCommand(sourcePFN, targetPFN, localPFN, remotePFN)
        self.assertEqual(expectedResults, result)

    def getResultForCreateStageOutCommand(self, sourcePFN, targetPFN, localPFN, remotePFN, options=None):
        result = "rfcp "
        if options != None:
            result += " %s " % options
        result += " \"%s\" " % sourcePFN
        result += " \"%s\" " % targetPFN
        result += "\nFILE_SIZE=`rfstat \"{0}\" | grep Size | cut -f2 -d:`\n" \
                  " echo \"Local File Size is: $FILE_SIZE\"; DEST_SIZE=`rfstat \"{1}\" |" \
                  " grep Size | cut -f2 -d:` ; if [ $DEST_SIZE ] && " \
                  "[ $FILE_SIZE == $DEST_SIZE ]; then exit 0; else echo " \
                  "\"Error: Size Mismatch between local and SE\"; exit 60311 ; fi ".format(localPFN,remotePFN)
        return result

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.RFCPImpl.removeFile("file")
        mock_executeCommand.assert_called_with("rfrm \"file\"")

    def testGetDirname_same(self):
        results=self.RFCPImpl.getDirname("folder/file")
        expeced="folder"
        self.assertEqual(expeced,results)

    def testGetDirname_rfio(self):
        results=self.RFCPImpl.getDirname("rfio:a/folder/file")
        expeced="/folder"
        self.assertEqual(expeced,results)

    def testGetDirname_rfioSlashes1(self):
        results = self.RFCPImpl.getDirname("rfio:/a/folder/file")
        expeced = "/a/folder"
        self.assertEqual(expeced, results)

    def testGetDirname_rfioSlashes2(self):
        results = self.RFCPImpl.getDirname("rfio://a/folder/file")
        expeced = "/folder"
        self.assertEqual(expeced, results)

    def testGetDirname_rfioSlashes3(self):
        results = self.RFCPImpl.getDirname("rfio:///a/folder/file")
        expeced = "/a/folder"
        self.assertEqual(expeced, results)

    def testGetDirname_rfioSlashes4(self):
        results=self.RFCPImpl.getDirname("rfio:////a/folder/file")
        expeced="/a/folder"
        self.assertEqual(expeced,results)

    def testGetDirName_rfioPath(self):
        url="rfio://castorlhcb.cern.ch:9002/?svcClass=lhcbdata&castorVersion=2&path=/castor/cern.ch0/" \
            "grid/lhcb/production/DC06/phys-v2-lumi2/00001650/DST/00$"
        results=self.RFCPImpl.getDirname(url)
        expeced="/castor/cern.ch0/grid/lhcb/production/DC06/phys-v2-lumi2/00001650/DST"
        self.assertEqual(expeced,results)

    def testGetDirName_rfioQuestion(self):
        url="rfio://castorlhcb.cern.ch:9002/svcClass=lhcbdata&castorVersion=2&/castor/?cern.ch0/" \
            "grid/lhcb/production/DC06/phys-v2-lumi2/00001650/DST/00$"
        results=self.RFCPImpl.getDirname(url)
        expeced="/svcClass=lhcbdata&castorVersion=2&/castor"
        self.assertEqual(expeced,results)



