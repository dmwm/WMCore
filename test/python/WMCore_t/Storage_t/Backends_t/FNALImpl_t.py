from __future__ import (print_function, division)

import unittest
from mock import mock

from WMCore.Storage.Backends.FNALImpl import FNALImpl


class FNALImplTest(unittest.TestCase):
    def setUp(self):
        self.FNALImpl = FNALImpl()

    def testStorageMethod_local(self):
        self.assertEqual("local", self.FNALImpl.storageMethod("test"))

    def testStorageMethod_xrdcp(self):
        self.assertEqual("xrdcp", self.FNALImpl.storageMethod("root://test"))

    def testStorageMethod_srm(self):
        self.assertEqual("srm", self.FNALImpl.storageMethod("srm://test"))

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testCreateOutputDirectory_local(self, mock_executeCommand):
        targetdir = "test"
        command = "#!/bin/sh\n"
        command += "if [ ! -e \"%s\" ]; then\n" % targetdir
        command += " mkdir -p %s\n" % targetdir
        command += "fi\n"
        self.FNALImpl.createOutputDirectory("test/file")
        mock_executeCommand.assert_called_with(command)

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    @mock.patch('WMCore.Storage.Backends.FNALImpl.LCGImpl.createOutputDirectory')
    def testCreateOutputDirectory_xrdcp(self, mock_executeCommand, mock_createOutputDirectory):
        self.FNALImpl.createOutputDirectory("root://test1/test2")
        mock_executeCommand.assert_not_called()
        mock_createOutputDirectory.assert_not_called()

    @mock.patch('WMCore.Storage.Backends.FNALImpl.LCGImpl.createOutputDirectory')
    def testCreateOutputDirectory_srm(self, mock_createOutputDirectory):
        self.FNALImpl.createOutputDirectory("srm://test1/test2")
        mock_createOutputDirectory.assert_called_with("srm://test1/test2")

    @mock.patch('WMCore.Storage.Backends.FNALImpl.LCGImpl.createSourceName')
    def testCreateSourceName_srm(self, mock_createSourceName):
        self.FNALImpl.createSourceName("proto", "srm://test1/test2")
        mock_createSourceName.assert_called_with("proto", "srm://test1/test2")

    @mock.patch('WMCore.Storage.Backends.FNALImpl.LCGImpl.createSourceName')
    def testCreateSourceName_local(self, mock_createSourceName):
        self.assertEqual("local", self.FNALImpl.createSourceName("proto", "local"))
        mock_createSourceName.assert_not_called()

    def testCreateStageOutCommand_stageInTrue(self):
        self.FNALImpl.stageIn = True
        result = self.FNALImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expedctedResult = "/usr/bin/xrdcp -d 0  sourcePFN  targetPFN "
        expedctedResult += """
        EXIT_STATUS=$?
        if [[ $EXIT_STATUS != 0 ]]; then
            echo "ERROR: xrdcp exited with $EXIT_STATUS"
        fi
        exit $EXIT_STATUS
        """
        self.assertEqual(expedctedResult, result)

    def testCreateStageOutCommand_stageInTrueOptions(self):
        self.FNALImpl.stageIn = True
        result = self.FNALImpl.createStageOutCommand("sourcePFN", "targetPFN", options="--test")
        expedctedResult = "/usr/bin/xrdcp -d 0  --test  sourcePFN  targetPFN "
        expedctedResult += """
        EXIT_STATUS=$?
        if [[ $EXIT_STATUS != 0 ]]; then
            echo "ERROR: xrdcp exited with $EXIT_STATUS"
        fi
        exit $EXIT_STATUS
        """
        self.assertEqual(expedctedResult, result)

    @mock.patch('WMCore.Storage.Backends.LCGImpl.LCGImpl.createStageOutCommand')
    def testCreateStageOutCommand_srm(self, mock_createStageOutCommand):
        mock_createStageOutCommand.return_value = "test"
        result = self.FNALImpl.createStageOutCommand("srm://test", "targetPFN")
        mock_createStageOutCommand.assert_called_with("srm://test", "targetPFN", None)
        self.assertEqual("test", result)
        result = self.FNALImpl.createStageOutCommand("sourcePFN", "srm://test")
        mock_createStageOutCommand.assert_called_with("sourcePFN", "srm://test", None)
        self.assertEqual("test", result)

    @mock.patch('WMCore.Storage.Backends.LCGImpl.os.stat')
    def testCreateStageOutCommand_xrdcpOptions(self, mock_stat):
        mock_stat.return_value = [0, 1, 2, 3, 4, 5, 6]
        result = self.FNALImpl.createStageOutCommand("root://test", "targetPFN", options="test")
        expectedResult = "xrdcp-old -d 0 -f "
        expectedResult += " test "
        expectedResult += " root://test "
        expectedResult += " targetPFN "
        expectedResult += """
            EXIT_STATUS=$?
            if [[ $EXIT_STATUS != 0 ]]; then
                echo "ERROR: xrdcp exited with $EXIT_STATUS"
            fi
            exit $EXIT_STATUS
            """
        self.assertEqual(expectedResult, result)

    @mock.patch('WMCore.Storage.Backends.LCGImpl.os.stat')
    def testCreateStageOutCommand_xrdcpChecksum(self, mock_stat):
        mock_stat.return_value = [0, 1, 2, 3, 4, 5, 6]
        result = self.FNALImpl.createStageOutCommand("root://test", "targetPFN", checksums={"adler32": "32"})
        expectedResult = "xrdcp-old -d 0 -f "
        expectedResult += " root://test "
        expectedResult += " targetPFN\?eos.targetsize=6\&eos.checksum=00000032 "
        expectedResult += """
            EXIT_STATUS=$?
            if [[ $EXIT_STATUS != 0 ]]; then
                echo "ERROR: xrdcp exited with $EXIT_STATUS"
            fi
            exit $EXIT_STATUS
            """
        print(expectedResult)
        print(result)
        self.assertEqual(expectedResult, result)

    def testCreateStageOutCommand_srmAndXrdcp(self):
        result = self.FNALImpl.createStageOutCommand("srm://test", "root://test")
        self.assertEqual(1, result)
        result = self.FNALImpl.createStageOutCommand("root://test", "srm://test")
        self.assertEqual(1, result)

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile_xrdcp(self, mock_executeCommand):
        self.FNALImpl.removeFile("root://test")
        mock_executeCommand.assert_called_with("xrd test rm root://test")

    @mock.patch('WMCore.Storage.Backends.FNALImpl.LCGImpl.removeFile')
    def testRemoveFile_srm(self, mock_removeFile):
        self.FNALImpl.removeFile("srm://test")
        mock_removeFile.assert_called_with("srm://test")

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile_local(self, mock_executeCommand):
        self.FNALImpl.removeFile("file")
        mock_executeCommand.assert_called_with("/bin/rm file")

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile_localStriped(self, mock_executeCommand):
        self.FNALImpl.removeFile("cookies.fnal.gov/test")
        mock_executeCommand.assert_called_with("/bin/rm test")
