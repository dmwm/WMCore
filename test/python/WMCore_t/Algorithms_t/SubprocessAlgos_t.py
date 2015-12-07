#!/usr/bin/env python
"""
_SubprocessAlgos_t_

Test class for basic subprocess Algorithms
"""


import unittest

from WMCore.Algorithms import SubprocessAlgos

class SubprocessAlgoTest(unittest.TestCase):
    """
    Main test body

    """

    def setUp(self):
        """
        Do nothing
        """
        pass

    def tearDown(self):
        """
        Do nothing
        """
        pass

    def testA_runCommand(self):
        """
        _runCommand_

        Check and see we can run a basic shell command.
        Also check to make sure we get the exit code back correctly
        """

        # First ls a directory you know will have stuff in it
        stdout, stderr, retcode = SubprocessAlgos.runCommand(cmd = 'ls /tmp/')
        self.assertTrue(len(stdout) > 0)
        self.assertEqual(retcode, 0)

        # Now try this same with the shell off
        stdout, stderr, retcode = SubprocessAlgos.runCommand(cmd = ['ls', '/tmp/'])
        self.assertTrue(len(stdout) > 0)
        self.assertEqual(retcode, 0)

        # Now test and see if we can catch a non-zero return code and output
        stdout, stderr, retcode = SubprocessAlgos.runCommand(cmd = 'echo HELP; exit 5')
        self.assertEqual(stdout, 'HELP\n')
        self.assertEqual(stderr, '')
        self.assertEqual(retcode, 5)

        # Now see if we can do a timeout if the process takes too long
        self.assertRaises(SubprocessAlgos.SubprocessAlgoException,
                          SubprocessAlgos.runCommand, cmd = 'sleep 10', timeout = 1)

        # And this one will go on for too long, but not raise because waitTime is not an int
        SubprocessAlgos.runCommand(cmd = 'sleep 1', timeout = 0.1)
        return


if __name__ == "__main__":
    unittest.main()
