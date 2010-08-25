#!/usr/bin/env python
"""
testing file manager

"""

import unittest
import logging
import shutil
import tempfile
import os.path
from WMCore.Storage.FileManager import StageInMgr,StageOutMgr,DeleteMgr,FileManager
import WMCore.Storage.StageOutError
class FileManagerTest(unittest.TestCase):
    
    def setUp(self):
        self.testDir = None
        
    def tearDown(self):
        if (self.testDir != None):
            try:
                shutil.rmtree( self.testDir )
            except:
                # meh, if it fails, I guess something weird happened
                pass
    
    def testStageFile(self):
        pass       
    #def stageFile(self, fileToStage, stageOut = True):
    def testDelete(self):
        pass
    #def deleteLFN(self, lfn):
    def testInitialiseSiteConf(self):
        pass
    #def initialiseSiteConf(self):
    def testInitialiseOverride(self):
    # def initialiseOverride(self):
        pass
   

    def testGetTransferDetails(self):
        pass
    #def getTransferDetails(self, lfn, currentMethod):
    def testStageIn(self):
        pass
    def testStageOut(self):
        pass
    
    #def stageIn(self,fileToStage):    
    #def stageOut(self,fileToStage):

    def test_doTransfer(self):
        pass   
    #def _doTransfer(self, currentMethod, methodCounter, lfn, pfn, stageOut):
    
    

    def testCleanSuccessfulStageOuts(self):
        pass
    #def cleanSuccessfulStageOuts(self):
    def testSearchTFC(self):
        pass
    #def searchTFC(self, lfn):

    def testStageOutMgrWrapperWin(self):
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': 'file:///etc/hosts', \
                           'SEName' : None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(  **{
                                'command'    : 'test-win',
                                'option'    : '', 
                                'se-name'  : 'test-win', 
                                'lfn-prefix':''})
        wrapper(fileForTransfer)
        
        pass
    def testStageOutMgrWrapperFail(self):
        fileForTransfer = {'LFN': 'failtest', \
                           'PFN': 'failtest', \
                           'SEName' : None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr( numberOfRetries= 1,
                               retryPauseTime=0, **{
                                'command'    : 'test-fail',
                                'option'    : '', 
                                'se-name'  : 'test-win', 
                                'lfn-prefix':''})
        self.assertRaises(WMCore.Storage.StageOutError.StageOutError, wrapper.__call__, fileForTransfer)

    def testStageOutMgrWrapperRealCopy(self):
        self.testDir = tempfile.mkdtemp()
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': '/etc/hosts', \
                           'SEName' : None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(  **{
                                'command'    : 'cp',
                                'option'    : '', 
                                'se-name'  : 'test-win', 
                                'lfn-prefix': self.testDir})
        wrapper(fileForTransfer)
        self.assertTrue( os.path.exists(os.path.join(self.testDir, '/etc/hosts')))

    def testStageInMgrWrapperWin(self):
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': '/etc/hosts', \
                           'SEName' : None, \
                           'StageOutCommand': None}
        wrapper = StageInMgr(  **{
                                'command'    : 'test-win',
                                'option'    : '', 
                                'se-name'  : 'test-win', 
                                'lfn-prefix':''})
        wrapper(fileForTransfer)
        
        pass
    def testStageInMgrWrapperFail(self):
        fileForTransfer = {'LFN': 'failtest', \
                           'PFN': 'failtest', \
                           'SEName' : None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr( numberOfRetries= 1,
                               retryPauseTime=0, **{
                                'command'    : 'test-fail',
                                'option'    : '', 
                                'se-name'  : 'test-win', 
                                'lfn-prefix':''})
        self.assertRaises(WMCore.Storage.StageOutError.StageOutError, wrapper.__call__, fileForTransfer)

    def testStageInMgrWrapperRealCopy(self):
        self.testDir = tempfile.mkdtemp()
        fileForTransfer = {'LFN': '/etc/hosts', \
                           'PFN': '/etc/hosts', \
                           'SEName' : None, \
                           'StageOutCommand': None}
        wrapper = StageOutMgr(  **{
                                'command'    : 'cp',
                                'option'    : '', 
                                'se-name'  : 'test-win', 
                                'lfn-prefix': self.testDir})
        wrapper(fileForTransfer)

    def testDeleteMgrWrapper(self):
        pass
if __name__ == "__main__":
    import nose
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
#    nose.main()