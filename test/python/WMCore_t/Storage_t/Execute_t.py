from __future__ import (print_function, division)

import os
import unittest

from mock import  mock

from WMCore.Storage.Execute import execute, runCommand, runCommandWithOutput
from WMCore.Storage.StageOutError import StageOutError
from WMCore.WMBase import getTestBase


class ExecuteTest(unittest.TestCase):
    base = os.path.join(getTestBase(), "WMCore_t/Storage_t/ExecutableCommands.py")

    @mock.patch('WMCore.Storage.Execute.runCommandWithOutput')
    def testExecute_exception(self, execute_runCommand):
        execute_runCommand.return_value = Exception("Im in test, yay!")
        self.assertRaises(StageOutError, execute, "test")

    @mock.patch('WMCore.Storage.Execute.runCommandWithOutput')
    def testExecute_exitCode(self, execute_runCommand):
        execute_runCommand.return_value = "Im exitCode", "test output"
        self.assertRaises(StageOutError, execute, "test")

    @mock.patch('WMCore.Storage.Execute.runCommandWithOutput')
    def testExecute_executed(self, execute_runCommand):
        execute_runCommand.return_value = 0, "test output"
        self.assertIsNone(execute("test"))
        execute_runCommand.assert_called_with("test")

    def testRunCommand_results(self):
        self.assertEqual(1, runCommand("python %s -exit 0" % self.base))
        self.assertEqual(1, runCommand("python %s -exit a" % self.base))
        self.assertEqual(2, runCommand("python %s -text" % self.base))
        self.assertEqual(0, runCommand("python %s -text a" % self.base))

    def testRunCommandWithOutput_results(self):
        err, text = runCommandWithOutput("python %s -text" % self.base)
        self.assertEqual(2, err)
        self.assertTrue("ExecutableCommands.py: error" in text)
        self.assertEqual((1, "0\n"), runCommandWithOutput("python %s -exit 0" % self.base))
        self.assertEqual((1, "a\n"), runCommandWithOutput("python %s -exit a" % self.base))
        self.assertEqual((0, "a\n"), runCommandWithOutput("python %s -text a" % self.base))

    @mock.patch('WMCore.Storage.Execute.sys')
    def testRunCommand_error(self, mock_sys):
        runCommand("python %s -exit 0" % self.base)
        mock_sys.stdout.write.assert_called_with("")
        mock_sys.stderr.write.assert_any_call("")
        mock_sys.stderr.write.assert_any_call("0\n")

    @mock.patch('WMCore.Storage.Execute.sys')
    def testRunCommand_noError(self, mock_sys):
        runCommand("python %s -text a" % self.base)
        mock_sys.stdout.write.assert_any_call("a\n")
        mock_sys.stderr.write.assert_any_call("")

    @mock.patch('WMCore.Storage.Execute.sys')
    def testRunCommand_errorAndText(self, mock_sys):
        runCommand("python %s -exception test" % self.base)
        assert "Exception: test" in mock_sys.stderr.write.call_args_list[0][0][0] or \
               "Exception: test" in mock_sys.stderr.write.call_args_list[1][0][0]
        mock_sys.stdout.write.assert_any_call("test\n")

    @mock.patch('WMCore.Storage.Execute.sys')
    def testRunCommandWithOutput_error(self, mock_sys):
        runCommand("python %s -exit 0" % self.base)
        mock_sys.stdout.write.assert_called_with("")
        mock_sys.stderr.write.assert_any_call("")
        mock_sys.stderr.write.assert_any_call("0\n")

    @mock.patch('WMCore.Storage.Execute.sys')
    def testRunCommandWithOutput_noError(self, mock_sys):
        runCommand("python %s -text a" % self.base)
        mock_sys.stdout.write.assert_any_call("a\n")
        mock_sys.stderr.write.assert_any_call("")

    @mock.patch('WMCore.Storage.Execute.sys')
    def testRunCommandWithOutput_errorAndText(self, mock_sys):
        runCommand("python %s -exception test" % self.base)
        assert "Exception: test\n" in mock_sys.stderr.write.call_args_list[0][0][0] or \
               "Exception: test" in mock_sys.stderr.write.call_args_list[1][0][0]
        mock_sys.stdout.write.assert_any_call("test\n")
