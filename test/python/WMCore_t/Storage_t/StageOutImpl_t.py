from __future__ import (print_function, division)

import unittest

from mock import mock, call

from WMCore.Storage.StageOutError import StageOutError
from WMCore.Storage.StageOutImpl import StageOutImpl


class StageOutImplTest(unittest.TestCase):
    def setUp(self):
        self.StageOutImpl = StageOutImpl()

    def testSplitPFN_noOpaque(self):
        self.assertEqual(("splitable/test/test1", "test1", "splitable/test/test1", ""),
                         StageOutImpl.splitPFN("splitable/test/test1"))
        self.assertEqual(('//eoscms//eos/cms/store/', 'eoscms', '//eoscms//eos/cms/store/', ""),
                         StageOutImpl.splitPFN("//eoscms//eos/cms/store/"))

    def testSplitPFN_doubleSlashRoot(self):
        self.assertEqual(('root', 'eoscms', '/eos/cms/store/', ""),
                         StageOutImpl.splitPFN("root://eoscms//eos/cms/store/"))

    def testSplitPFN_path(self):
        self.assertEqual(('root', 'eoscms', 'default', "?"),
                         StageOutImpl.splitPFN("root://eoscms//eos/cms/store?path=default"))

    def testSplitPFN_pathConnector(self):
        self.assertEqual(('root', 'eoscms', 'default', "?default2"),
                         StageOutImpl.splitPFN("root://eoscms//eos/cms/store?path=default&default2"))

    def testSplitPFN_path2(self):
        self.assertEqual(('root', 'eoscms', 'cms', "?cms2ss&path=default"),
                         StageOutImpl.splitPFN("root://eoscms//eos/cms/store?path=cms&cms2ss&path=default"))

    def testSplitPFN_pathConnector2(self):
        self.assertEqual(('root', 'eoscms', 'cms', "?path=default&default2"),
                         StageOutImpl.splitPFN("root://eoscms//eos/cms/store?path=cms&path=default&default2"))

    @mock.patch('WMCore.Storage.StageOutImpl.runCommandWithOutput')
    def testExecuteCommand_stageOutError(self, mock_runCommand):
        mock_runCommand.side_effect = Exception('BOOM!')
        with self.assertRaises(Exception) as context:
            self.StageOutImpl.executeCommand("command")
            self.assertTrue('ErrorCode : 60311' in context.exception)
        mock_runCommand.assert_called_with("command")

    @mock.patch('WMCore.Storage.StageOutImpl.runCommandWithOutput')
    def testExecuteCommand_exitCode(self, mock_runCommand):
        mock_runCommand.return_value = 1, "Test Fake Error"
        with self.assertRaises(Exception) as context:
            self.StageOutImpl.executeCommand("command")
            self.assertTrue('ErrorCode : 1' in context.exception)
        mock_runCommand.assert_called_with("command")

    @mock.patch('WMCore.Storage.StageOutImpl.runCommandWithOutput')
    def testExecuteCommand_valid(self, mock_runCommand):
        mock_runCommand.return_value = 0, "Test Success"
        self.StageOutImpl.executeCommand("command")
        mock_runCommand.assert_called_with("command")

    @mock.patch('WMCore.Storage.StageOutImpl.os.path')
    def testCreateRemoveFileCommand_isFile(self, mock_path):
        mock_path.isfile.return_value = True
        mock_path.abspath.return_value = "path"
        self.assertEqual("/bin/rm -f path", self.StageOutImpl.createRemoveFileCommand("test/path"))
        mock_path.isfile.assert_called_with("test/path")
        mock_path.abspath.assert_called_with("test/path")

    @mock.patch('WMCore.Storage.StageOutImpl.os.path')
    def testCreateRemoveFileCommand_notPath(self, mock_path):
        mock_path.isfile.return_value = False
        self.assertEqual("", self.StageOutImpl.createRemoveFileCommand("test/path"))
        mock_path.isfile.assert_called_with("test/path")

    def testCreateRemoveFileCommand_startsWithSlash(self):
        self.assertEqual("/bin/rm -f /test/path", self.StageOutImpl.createRemoveFileCommand("/test/path"))

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createSourceName')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createTargetName')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createOutputDirectory')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createStageOutCommand')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createDebuggingCommand')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    def testCallable(self, 
                    mock_executeCommand,          # from the last decorator
                    mock_createDebuggingCommand,  # from the second-to-last decorator
                    mock_createStageOutCommand,   # then this one
                    mock_createOutputDirectory, 
                    mock_createTargetName, 
                    mock_createSourceName):
        mock_createSourceName.return_value = "sourcePFN"
        mock_createTargetName.return_value = "targetPFN"
        mock_createStageOutCommand.return_value = "command"
        # Even if set, createDebuggingCommand should not be called on success.
        mock_createDebuggingCommand.return_value = "command"
        
        self.StageOutImpl("protocol", "inputPFN", "targetPFN")
        
        mock_createSourceName.assert_called_with("protocol", "inputPFN")
        mock_createTargetName.assert_called_with("protocol", "targetPFN")
        mock_createOutputDirectory.assert_called_with("targetPFN")
        mock_createStageOutCommand.assert_called_with("sourcePFN", "targetPFN", None, None, authMethod='TOKEN')
        # Verify that createDebuggingCommand was not called during a successful stage-out.
        mock_createDebuggingCommand.assert_not_called()
        mock_executeCommand.assert_called_with("command")

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createSourceName')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createTargetName')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createOutputDirectory')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createStageOutCommand')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createDebuggingCommand')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.executeCommand')
    @mock.patch('WMCore.Storage.StageOutImpl.time')
    def testCallable_StageOutError(self, mock_time, mock_executeCommand, mock_createStageOutCommand, 
                                mock_createDebuggingCommand, mock_createOutputDirectory, 
                                mock_createTargetName, mock_createSourceName):
        mock_createSourceName.return_value = "sourcePFN"
        mock_createTargetName.return_value = "targetPFN"
        mock_createStageOutCommand.return_value = "command"
        mock_createDebuggingCommand.return_value = "command"
        
        # Force createOutputDirectory to fail twice, then succeed.
        mock_createOutputDirectory.side_effect = [StageOutError("error"), StageOutError("error"), None]
        
        # Force executeCommand to fail on all 4 attempts, then succeed for the debugging command.
        mock_executeCommand.side_effect = [
            StageOutError("error"),
            StageOutError("error"),
            StageOutError("error"),
            StageOutError("error"),
            None  # This call is for executing the debugging command.
        ]
    
        with self.assertRaises(StageOutError):
            self.StageOutImpl("protocol", "inputPFN", "targetPFN")
        
        mock_createSourceName.assert_called_with("protocol", "inputPFN")
        mock_createTargetName.assert_called_with("protocol", "targetPFN")
        mock_createOutputDirectory.assert_called_with("targetPFN")
        # The stage-out command is generated with authMethod='TOKEN'
        mock_createStageOutCommand.assert_called_with("sourcePFN", "targetPFN", None, None, authMethod='TOKEN')
        # After retries are exhausted, the debugging command should be generated.
        mock_createDebuggingCommand.assert_called_with("sourcePFN", "targetPFN", None, None, authMethod='TOKEN')
        # Assert that time.sleep was called 4 times (once for each failed stage-out attempt).
        calls = [call(600), call(600), call(600), call(600)]
        mock_time.sleep.assert_has_calls(calls)

    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createSourceName')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createTargetName')
    @mock.patch('WMCore.Storage.StageOutImpl.StageOutImpl.createOutputDirectory')
    @mock.patch('WMCore.Storage.StageOutImpl.time')
    def testCallable_StageOutErrorFail(self, mock_time, mock_createOutputDirectory, mock_createTargetName,
                                       mock_createSourceName):
        mock_createSourceName.return_value = "sourcePFN"
        mock_createTargetName.return_value = "targetPFN"
        mock_createOutputDirectory.side_effect = [StageOutError("error"), StageOutError("error"),
                                                  StageOutError("Last error")]
        with self.assertRaises(Exception) as context:
            self.StageOutImpl("protocol", "inputPFN", "targetPFN")
            self.assertTrue('Last error' in context.exception)
        mock_createSourceName.assert_called_with("protocol", "inputPFN")
        mock_createTargetName.assert_called_with("protocol", "targetPFN")
        mock_createOutputDirectory.assert_called_with("targetPFN")
        calls = [call(600), call(600)]
        mock_time.sleep.assert_has_calls(calls)
