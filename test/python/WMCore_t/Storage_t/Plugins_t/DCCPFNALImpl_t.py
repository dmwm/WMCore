import os
import unittest
import mox
import WMCore_t.Storage_t.Plugins_t.PluginTestBase_t
from WMCore.Storage.Plugins.DCCPFNALImpl import DCCPFNALImpl as ourPlugin

from WMCore.Storage.Plugins.CPImpl import CPImpl as ourFallbackPlugin
import WMCore.Storage.Plugins.DCCPFNALImpl
import subprocess
from WMCore.WMBase import getWMBASE
from WMCore.Storage.StageOutError import StageOutError, StageOutFailure

class RunCommandThing:
    def __init__(self, target):
        self.target = target
    def runCommand(self,things):
        return ("dummy1", "dummy2")
    

class DCCPFNALImplTest(unittest.TestCase):
    
    def setUp(self):
        self.commandPrepend = os.path.join(getWMBASE(),'src','python','WMCore','Storage','Plugins','DCCPFNAL','wrapenv.sh')
        self.runMocker  = mox.MockObject(RunCommandThing)
        self.copyMocker = mox.MockObject(ourFallbackPlugin)
        def runCommandStub(command):
            (num1, num2) =  self.runMocker.runCommand(command)
            return (num1, num2)
        def getImplStub(command, useNewVersion = None):
            return self.copyMocker
        
        self.runCommandBackup   = WMCore.Storage.Plugins.DCCPFNALImpl.runCommand
        WMCore.Storage.Plugins.DCCPFNALImpl.runCommand = runCommandStub
        self.stageOutImplBackup = WMCore.Storage.Plugins.DCCPFNALImpl.retrieveStageOutImpl
        WMCore.Storage.Plugins.DCCPFNALImpl.retrieveStageOutImpl = getImplStub
        pass
    
    def tearDown(self):
        WMCore.Storage.Plugins.DCCPFNALImpl.runCommand = self.runCommandBackup
        WMCore.Storage.Plugins.DCCPFNALImpl.retrieveStageOutImpl = self.stageOutImplBackup
        
    
    def testFail(self):

        #first try to make a non existant file (regular)
        self.runMocker.runCommand( 
            [self.commandPrepend,'dccp', '-o', '86400', '-d', '0', '-X', '-role=cmsprod', '/store/NONEXISTANTSOURCE', '/store/NONEXISTANTTARGET' ]\
             ).AndReturn(("1", "This was a test of the fail system"))
             
        #then try to make a non existant file on lustre
        # -- fake making a directory
        self.runMocker.runCommand( 
            [self.commandPrepend, 'mkdir', '-m', '755', '-p', '/store/unmerged']\
             ).AndReturn(("0", "we made a directory, yay"))        
        # -- fake the actual copy
        self.copyMocker.doTransfer( \
            '/store/unmerged/lustre/NONEXISTANTSOURCE', '/store/unmerged/lustre/NONEXISTANTTARGET', True, None, None, None, None\
             ).AndRaise(StageOutFailure("testFailure"))
        
        # do one with a real pfn
        self.runMocker.runCommand(\
            [self.commandPrepend, 'mkdir', '-m', '755', '-p',\
            '/pnfs/cms/WAX/11/store/temp/WMAgent/unmerged/RECO/WMAgentCommissioning10-v7newstageout']).AndReturn(("0",""))
        self.runMocker.runCommand([self.commandPrepend, 'dccp', '-o', '86400', '-d', '0', '-X', '-role=cmsprod', 'file:///etc/hosts', 'dcap://cmsdca.fnal.gov:24037/pnfs/fnal.gov/usr/cms/WAX/11/store/temp/WMAgent/unmerged/RECO/WMAgentCommissioning10-v7newstageout/0000/0661D749-DD95-DF11-8A0F-00261894387C.root ']).AndReturn(("0",""))
        # now try to delete it (pnfs)
        self.runMocker.runCommand( 
            ['rm', '-fv', '/pnfs/cms/WAX/11/store/tmp/testfile' ]\
             ).AndReturn(("1", "This was a test of the fail system"))
        # try to delete it (lustre)
        self.runMocker.runCommand( 
            ['/bin/rm', '/lustre/unmerged/NOTAFILE']\
             ).AndReturn(("1", "This was a test of the fail system"))

        mox.Replay(self.runMocker)       
        mox.Replay(self.copyMocker)
        #ourPlugin.runCommand = runMocker.runCommand()
        testObject = ourPlugin()
        
        self.assertRaises(StageOutFailure,
                           testObject.doTransfer,'/store/NONEXISTANTSOURCE',
                              '/store/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.assertRaises(StageOutFailure,
                           testObject.doTransfer,'/store/unmerged/lustre/NONEXISTANTSOURCE',
                              '/store/unmerged/lustre/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.assertRaises(StageOutFailure,
                           testObject.doTransfer,'file:///etc/hosts',
                              'dcap://cmsdca.fnal.gov:24037/pnfs/fnal.gov/usr/cms/WAX/11/store/temp/WMAgent/unmerged/RECO/WMAgentCommissioning10-v7newstageout/0000/0661D749-DD95-DF11-8A0F-00261894387C.root ',
                              True, 
                              None,
                              None,
                              None,
                              None)
        testObject.doDelete('/store/tmp/testfile', None, None, None, None  )
        testObject.doDelete('/store/unmerged/lustre/NOTAFILE',None, None, None, None )
        mox.Verify(self.runMocker)
        mox.Verify(self.copyMocker)
    def testWin(self):


        
        #first try to make a file (regular). this one works
        self.runMocker.runCommand( 
            [self.commandPrepend,'dccp', '-o', '86400', '-d', '0', '-X', '-role=cmsprod', '/store/NONEXISTANTSOURCE', '/store/NONEXISTANTTARGET' ]\
             ).AndReturn((0, "This transfer works"))
        self.runMocker.runCommand( 
            [self.commandPrepend,'/opt/d-cache/dcap/bin/check_dCachefilecksum.sh', '/store/NONEXISTANTTARGET', '/store/NONEXISTANTSOURCE']\
             ).AndReturn((0, "Oh, the checksum was checked"))
        
        # now make a file and have the checksum fail
        self.runMocker.runCommand( 
            [self.commandPrepend,'dccp', '-o', '86400', '-d', '0', '-X', '-role=cmsprod', '/store/NONEXISTANTSOURCE', '/store/NONEXISTANTTARGET' ]\
             ).AndReturn((0, "This transfer works"))
        self.runMocker.runCommand( 
            [self.commandPrepend,'/opt/d-cache/dcap/bin/check_dCachefilecksum.sh', '/store/NONEXISTANTTARGET', '/store/NONEXISTANTSOURCE']\
             ).AndReturn((1, "Oh, the checksum was checked. Things look bad"))
        self.runMocker.runCommand( 
            [self.commandPrepend, 'mkdir', '-m', '755', '-p', '/store/unmerged']\
             ).AndReturn((0, ""))
                          
        #then try to make a non existant file on lustre
        # -- fake making a directory
        self.runMocker.runCommand( 
            [self.commandPrepend, 'mkdir', '-m', '755', '-p', '/store/unmerged']\
             ).AndReturn((0, "we made a directory, yay"))        
        # -- fake the actual copy
        self.copyMocker.doTransfer( \
            '/store/unmerged/lustre/NONEXISTANTSOURCE', '/store/unmerged/lustre/NONEXISTANTTARGET', True, None, None, None, None\
             ).AndReturn("balls")
        
        mox.Replay(self.runMocker)       
        mox.Replay(self.copyMocker)
        #ourPlugin.runCommand = runMocker.runCommand()
        testObject = ourPlugin()
        
        # copy normally and have it work
        newPfn = testObject.doTransfer('/store/NONEXISTANTSOURCE',
                              '/store/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.assertEqual(newPfn, '/store/NONEXISTANTTARGET')
        # second time fails the checksum
        self.assertRaises(StageOutFailure,
                           testObject.doTransfer,'/store/NONEXISTANTSOURCE',
                              '/store/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        
        # copy to lustre normally and have it work
        newPfn = testObject.doTransfer('/store/unmerged/lustre/NONEXISTANTSOURCE',
                              '/store/unmerged/lustre/NONEXISTANTTARGET',
                              True, 
                              None,
                              None,
                              None,
                              None)
        self.assertEqual(newPfn, "balls")
        mox.Verify(self.runMocker)
        mox.Verify(self.copyMocker)
        

if __name__ == "__main__":
    unittest.main()

