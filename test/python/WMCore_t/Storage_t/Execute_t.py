from __future__ import (print_function, division)

import os
import unittest

from mock import  mock

from Utils.PythonVersion import PY3

from WMCore.Storage.Execute import execute, runCommand, runCommandWithOutput
from WMCore.Storage.StageOutError import StageOutError
from WMCore.WMBase import getTestBase


class ExecuteTest(unittest.TestCase):
    base = os.path.join(getTestBase(), "WMCore_t/Storage_t/ExecutableCommands.py")

    def setUp(self):
        self.python_runtime = "python3" if PY3 else "python"

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
        self.assertEqual(1, runCommand("%s %s -exit 0" % (self.python_runtime, self.base)))
        self.assertEqual(1, runCommand("%s %s -exit a" % (self.python_runtime, self.base)))
        self.assertEqual(2, runCommand("%s %s -text" % (self.python_runtime, self.base)))
        self.assertEqual(0, runCommand("%s %s -text a" % (self.python_runtime, self.base)))

    def testRunCommandWithOutput_results(self):
        err, text = runCommandWithOutput("%s %s -text" % (self.python_runtime, self.base))
        self.assertEqual(2, err)
        self.assertTrue("ExecutableCommands.py: error" in text)
        self.assertEqual((1, 'stdout: \nstderr: 0\n'), runCommandWithOutput("%s %s -exit 0" % (self.python_runtime, self.base)))
        self.assertEqual((1, 'stdout: \nstderr: a\n'), runCommandWithOutput("%s %s -exit a" % (self.python_runtime, self.base)))
        self.assertEqual((0, 'stdout: a\n\nstderr: '), runCommandWithOutput("%s %s -text a" % (self.python_runtime, self.base)))

