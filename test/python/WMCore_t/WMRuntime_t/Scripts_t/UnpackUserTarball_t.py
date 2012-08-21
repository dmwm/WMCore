#!/usr/bin/env python
"""
_UnpackUserTarball_t.py

Tests for the user tarball unpacker and additional file mover

"""

import logging
import os
import subprocess
import sys
import tempfile
import unittest

from WMCore.WMRuntime.Scripts.UnpackUserTarball import UnpackUserTarball

class UnpackUserTarballTest(unittest.TestCase):
    """
    unittest for UnpackUserTarball script

    """


    # Set up a dummy logger
    logger = logging.getLogger('UNITTEST')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    logger.addHandler(ch)


    def setUp(self):
        """
        Set up for unit tests
        """

        os.environ['WMAGENTJOBDIR'] = '/tmp/'
        self.arch    = 'slc5_ia32_gcc434'
        self.version = 'CMSSW_3_8_7'
        self.base    = '/tmp/%s/' % self.version
        self.localFile = '/%s/unittestTarball.tgz' % os.environ['WMAGENTJOBDIR']
        self.tempDir = tempfile.mkdtemp()
        self.logger.debug("Using temp directory %s" % self.tempDir)

        self.origDir = os.getcwd()

        # Make a dummy CMSSW environment

        commands = [
            'rm -rf %s' % self.base,
            'mkdir -p %s/lib/%s/' % (self.base, self.arch),
            'touch %s/lib/%s/libSomething.so' % (self.base, self.arch),
            'mkdir -p %s/src/Module/Submodule/data/' % (self.base),
            'touch %s/src/Module/Submodule/data/datafile.txt' % (self.base),
            'touch %s/extra_file.txt' % (self.base),
            'touch %s/extra_file2.txt' % (self.base),
            'touch %s/additional_file.txt' % (self.base),
            'tar -C %s -czf %s .' % (self.base, self.localFile),
        ]

        for command in commands:
            self.logger.debug("Executing command %s" % command)
            subprocess.check_call(command.split(' '))
        os.mkdir(os.path.join(self.tempDir, self.version))
        os.chdir(os.path.join(self.tempDir, self.version))
        return


    def tearDown(self):
        """
        Clean up the files we've spewed all over
        """
        os.chdir(self.origDir)

        subprocess.check_call(['rm', '-rf', self.tempDir])
        subprocess.check_call(['rm', '-rf', self.base])
        subprocess.check_call(['rm', '-f', self.localFile])

        return


    def testFileSandbox(self):
        """
        _testFileSandbox_

        Test a single sandbox that is a file

        """
        sys.argv = ['scriptName','unittestTarball.tgz','']
        UnpackUserTarball()
        self.assert_(os.path.isfile('lib/slc5_ia32_gcc434/libSomething.so'))


    def testBadFile(self):
        """
        _testBadFile_

        Test we get an exception from a non-existent file

        """
        sys.argv = ['scriptName','doesNotExist.tgz','']
        self.assertRaises(IOError, UnpackUserTarball)


    def testUrlSandbox(self):
        """
        _testUrlSandbox_

        Test a single sandbox that is a URL

        """

        sys.argv = ['scriptName','http://home.fnal.gov/~ewv/unittestTarball.tgz','']
        UnpackUserTarball()
        self.assert_(os.path.isfile('lib/slc5_ia32_gcc434/libSomething.so'))


    def testUrlNotTar(self):
        """
        _testUrlSandbox_

        Test a single sandbox that is a URL

        """

        sys.argv = ['scriptName','http://home.fnal.gov/~ewv/index.html','']
        self.assertRaises(RuntimeError, UnpackUserTarball)


    def testBadUrl(self):
        """
        _testUrlSandbox_

        Test a single sandbox that is a URL

        """

        sys.argv = ['scriptName','http://home.fnal.gov/~ewv/not-there.txt','']
        self.assertRaises(RuntimeError, UnpackUserTarball)


    def testFileAndURLSandbox(self):
        """
        _testFileAndURLSandbox_

        Test two sandboxes. One a file, one a URL

        """

        sys.argv = ['scriptName','unittestTarball.tgz,http://home.fnal.gov/~ewv/unittestTarball.tgz','']
        UnpackUserTarball()
        # First is only in web tarball, second only in local
        self.assert_(os.path.isfile('lib/slc5_ia32_gcc434/libSomething.so'))
        self.assert_(os.path.isfile('lib/slc5_ia32_gcc434/libSomefile.so'))


    def testFileRelocation(self):
        """
        _testFileRelocation_

        Test our ability to relocate files within the sandbox

        """

        sys.argv = ['scriptName','unittestTarball.tgz','extra_file.txt,additional_file.txt']
        UnpackUserTarball()
        self.assert_(os.path.isfile('../extra_file.txt'))
        self.assert_(os.path.isfile('../additional_file.txt'))



if __name__ == "__main__":
    unittest.main()
