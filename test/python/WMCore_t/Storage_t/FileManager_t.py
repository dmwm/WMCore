#!/usr/bin/env python
"""
testing file manager

"""
from __future__ import print_function

import logging
import os.path
import shutil
import tempfile
import unittest

import WMCore.Storage.StageOutError
from WMCore.Storage.FileManager import StageInMgr, StageOutMgr, DeleteMgr


class FileManagerTest(unittest.TestCase):
    def setUp(self):
        self.testDir = None

    def tearDown(self):
        if (self.testDir != None):
            try:
                shutil.rmtree(self.testDir)
            except Exception:
                # meh, if it fails, I guess something weird happened
                pass

    def testStageFile(self):
        pass

    # def stageFile(self, fileToStage, stageOut = True):
    def testDelete(self):
        pass

    # def deleteLFN(self, lfn):
    def testInitialiseSiteConf(self):
        pass

    # def initialiseSiteConf(self):
    def testInitialiseOverride(self):
        # def initialiseOverride(self):
        pass

    def testGetTransferDetails(self):
        pass

    # def getTransferDetails(self, lfn, currentMethod):
    def testStageIn(self):
        pass

    def testStageOut(self):
        pass

    # def stageIn(self,fileToStage):
    # def stageOut(self,fileToStage):

    def test_doTransfer(self):
        pass

    # def _doTransfer(self, currentMethod, methodCounter, lfn, pfn, stageOut):



    def testCleanSuccessfulStageOuts(self):
        pass

    # def cleanSuccessfulStageOuts(self):
    def testSearchTFC(self):
        pass

    # def searchTFC(self, lfn):

    def testStageOutMgrWrapperWin(self):
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': 'file:///etc/hosts', \
                           'PNN': None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(**{
            'command': 'test-win',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': ''})
        wrapper(fileForTransfer)

    def testStageOutMgrWrapperFail(self):
        fileForTransfer = {'LFN': 'failtest', \
                           'PFN': 'failtest', \
                           'PNN': None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(numberOfRetries=1,
                              retryPauseTime=0, **{
                'command': 'test-fail',
                'option': '',
                'phedex-node': 'test-win',
                'lfn-prefix': ''})
        self.assertRaises(WMCore.Storage.StageOutError.StageOutError, wrapper.__call__, fileForTransfer)

    def testStageOutMgrWrapperRealCopy(self):
        self.testDir = tempfile.mkdtemp()
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': '/etc/hosts', \
                           'PNN': None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(**{
            'command': 'cp',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': self.testDir})
        wrapper(fileForTransfer)
        self.assertTrue(os.path.exists(os.path.join(self.testDir, '/etc/hosts')))

    def testStageOutMgrWrapperRealCopyFallback(self):
        self.testDir = tempfile.mkdtemp()
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': '/etc/hosts', \
                           'PNN': None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(**{
            'command': 'testFallbackToOldBackend',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': self.testDir})
        wrapper(fileForTransfer)
        self.assertTrue(os.path.exists(os.path.join(self.testDir, '/etc/hosts')))

    def testStageInMgrWrapperWin(self):
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': '/etc/hosts', \
                           'PNN': None, \
                           'StageOutCommand': None}
        wrapper = StageInMgr(**{
            'command': 'test-win',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': ''})
        wrapper(fileForTransfer)

    def testStageInMgrWrapperFail(self):
        fileForTransfer = {'LFN': 'failtest', \
                           'PFN': 'failtest', \
                           'PNN': None, \
                           'StageOutCommand': None}
        wrapper = StageInMgr(numberOfRetries=1,
                             retryPauseTime=0, **{
                'command': 'test-fail',
                'option': '',
                'phedex-node': 'test-win',
                'lfn-prefix': ''})
        self.assertRaises(WMCore.Storage.StageOutError.StageOutError, wrapper.__call__, fileForTransfer)

    def testStageInMgrWrapperRealCopy(self):

        self.testDir = tempfile.mkdtemp()
        shutil.copy('/etc/hosts', self.testDir + '/INPUT')
        fileForTransfer = {'LFN': '/INPUT', \
                           'PFN': '%s/etc/hosts' % self.testDir, \
                           'PNN': None, \
                           'StageOutCommand': None}

        wrapper = StageInMgr(**{
            'command': 'cp',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': self.testDir})
        wrapper(fileForTransfer)

    def testStageInMgrWrapperRealCopyFallback(self):
        self.testDir = tempfile.mkdtemp()
        shutil.copy('/etc/hosts', self.testDir + '/INPUT')
        fileForTransfer = {'LFN': '/INPUT', \
                           'PFN': '%s/etc/hosts' % self.testDir, \
                           'PNN': None, \
                           'StageOutCommand': None}

        wrapper = StageInMgr(**{
            'command': 'testFallbackToOldBackend',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': self.testDir})
        wrapper(fileForTransfer)

    def testDeleteMgrWrapper(self):
        self.testDir = tempfile.mkdtemp()
        shutil.copy('/etc/hosts', self.testDir + '/INPUT')
        fileForTransfer = {'LFN': '/INPUT', \
                           'PFN': '%s/etc/hosts' % self.testDir, \
                           'PNN': None, \
                           'StageOutCommand': None}

        wrapper = StageInMgr(**{
            'command': 'cp',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': self.testDir})
        retval = wrapper(fileForTransfer)
        print("got the retval %s" % retval)
        wrapper = DeleteMgr(**{
            'command': 'cp',
            'option': '',
            'phedex-node': 'test-win',
            'lfn-prefix': self.testDir})
        wrapper(retval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
