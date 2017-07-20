from __future__ import (print_function, division)

import os
import unittest

from mock import mock, call, Mock

from WMCore.Storage.Backends.CPImpl import CPImpl
from WMCore.WMBase import getTestBase


class CPImplTest(unittest.TestCase):
    def setUp(self):
        self.CPImpl = CPImpl()

    def testCreateSourceName(self):
        self.assertEqual("name", self.CPImpl.createSourceName("", "name"))
        self.assertEqual("file:////name", self.CPImpl.createSourceName("", "file:////name"))

    @mock.patch('WMCore.Storage.Backends.CPImpl.CPImpl.run')
    def testCreateOutputDirectory_error0(self, mock_runCommand):
        mock_runCommand.return_value = 0
        self.CPImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("/bin/ls dir/file > /dev/null ")

    @mock.patch('WMCore.Storage.Backends.CPImpl.CPImpl.run')
    def testCreateOutputDirectory_error0Exception(self, mock_runCommand):
        mock_runCommand.side_effect = Exception("Im in test, yay!")
        self.CPImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_called_once_with("/bin/ls dir/file > /dev/null ")

    def testCreateOutputDirectory_error1Exception(self):
        self.CPImpl.run = Mock()
        self.CPImpl.run.return_value = [1, Exception("Failed to create directory")]
        self.CPImpl.createOutputDirectory("dir/file/test")
        self.CPImpl.run.assert_has_calls([call("/bin/ls dir/file > /dev/null "),
                                          call("umask 002 ; /bin/mkdir -p dir/file")])

    @mock.patch('WMCore.Storage.Backends.CPImpl.CPImpl.run')
    def testCreateOutputDirectory_error1(self, mock_runCommand):
        mock_runCommand.return_value = [1, 2]
        self.CPImpl.createOutputDirectory("dir/file/test")
        mock_runCommand.assert_has_calls([call("/bin/ls dir/file > /dev/null "),
                                          call("umask 002 ; /bin/mkdir -p dir/file")])

    def testCreateStageOutCommand_realFile(self):
        base = os.path.join(getTestBase(), "WMCore_t/Storage_t/ExecutableCommands.py")
        result = self.CPImpl.createStageOutCommand(base, "test")
        expectedResult = [" '642' -eq $DEST_SIZE", "DEST_SIZE=`/bin/ls -l test", base]
        for text in expectedResult:
            self.assertIn(text, result)

    @mock.patch('WMCore.Storage.Backends.CPImpl.os.stat')
    def testCreateStageOutCommand_noFile(self, mocked_stat):
        mocked_stat.return_value = [0, 1, 2, 3, 4, 5, 6]
        result = self.CPImpl.createStageOutCommand("sourcePFN", "targetPFN", options="optionsTest")
        expectedResult = [" '6' -eq $DEST_SIZE", "DEST_SIZE=`/bin/ls -l targetPFN", "sourcePFN", "optionsTest"]
        for text in expectedResult:
            self.assertIn(text, result)

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testRemoveFile(self, mock_executeCommand):
        self.CPImpl.removeFile("file")
        mock_executeCommand.assert_called_with("/bin/rm file")
