from __future__ import (print_function, division)

import unittest

from mock import mock

from WMCore.Storage.Backends.CPImpl import CPImpl


class CPImplTest(unittest.TestCase):
    def setUp(self):
        self.CPImpl = CPImpl()

    def testCreateSourceName_simple(self):
        self.assertEqual("name", self.CPImpl.createSourceName("", "name"))

    @mock.patch('WMCore.Storage.Backends.CPImpl.os')
    def testCreateOutputDirectory_noDir(self, mock_os):
        mock_os.path.dirname.return_value = "/dir1/dir2"
        mock_os.path.isdir.return_value = False
        self.CPImpl.createOutputDirectory("/dir1/dir2/file")
        mock_os.path.dirname.assert_called_once_with("/dir1/dir2/file")
        mock_os.path.isdir.assert_called_once_with("/dir1/dir2")
        mock_os.makedirs.assert_called_once_with("/dir1/dir2")

    @mock.patch('WMCore.Storage.Backends.CPImpl.os')
    def testCreateOutputDirectory_withDir(self, mock_os):
        mock_os.path.dirname.return_value = "/dir1/dir2"
        mock_os.path.isdir.return_value = True
        self.CPImpl.createOutputDirectory("/dir1/dir2/file")
        mock_os.path.dirname.assert_called_once_with("/dir1/dir2/file")
        mock_os.path.isdir.assert_called_once_with("/dir1/dir2")
        mock_os.makedirs.assert_not_called()

    def testCreateStageOutCommand_noStageInNoOptionsNoChecksum(self):
        self.CPImpl.stageIn = False
        results = self.CPImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResults = self.createStageOutCommandResults(False, "sourcePFN", "targetPFN")
        self.assertEqual(expectedResults, results)

    def testCreateStageOutCommand_stageInNoOptionsNoChecksum(self):
        self.CPImpl.stageIn = True
        results = self.CPImpl.createStageOutCommand("sourcePFN", "targetPFN")
        expectedResults = self.createStageOutCommandResults(True, "sourcePFN", "targetPFN")
        self.assertEqual(expectedResults, results)

    def createStageOutCommandResults(self, stageIn, sourcePFN, targetPFN):
        copyCommand = ""
        if stageIn:
            remotePFN, localPFN = sourcePFN, targetPFN
        else:
            remotePFN, localPFN = targetPFN, sourcePFN
            copyCommand += "LOCAL_SIZE=`stat -c%%s %s`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
        copyCommand += "cp %s %s\n" % (sourcePFN, targetPFN)
        if stageIn:
            copyCommand += "LOCAL_SIZE=`stat -c%%s %s`\n" % localPFN
            copyCommand += "echo \"Local File Size is: $LOCAL_SIZE\"\n"
            removeCommand = ""
        else:
            removeCommand = "rm %s;" % remotePFN
        copyCommand += "REMOTE_SIZE=`stat -c%%s %s`\n" % remotePFN
        copyCommand += "echo \"Remote File Size is: $REMOTE_SIZE\"\n"
        copyCommand += "if [ $REMOTE_SIZE ] && [ $LOCAL_SIZE == $REMOTE_SIZE ]; then exit 0; "
        copyCommand += "else echo \"ERROR: Size Mismatch between local and SE\"; %s exit 60311 ; fi" % removeCommand
        return copyCommand

    @mock.patch('WMCore.Storage.Backends.CPImpl.os')
    def testRemoveFile(self, mock_os):
        self.CPImpl.removeFile("file")
        mock_os.remove.assert_called_once_with("file")
