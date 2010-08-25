#!/usr/bin/env python
"""
_TestImpl_

Couple of test implementations for unittests

"""



from WMCore.Storage.Registry import registerStageOutImpl
from WMCore.Storage.StageOutImpl import StageOutImpl
from WMCore.Storage.StageOutError import StageOutFailure

class WinImpl(StageOutImpl):
    """
    _WinImpl_

    Test plugin that always returns success

    """
    def createSourceName(self, protocol, lfn):
        return "WIN!!!"


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        print "WinImpl.createStageOutCommand(%s, %s, %s)" % (sourcePFN, targetPFN, options)
        return "WIN!!!"


    def removeFile(self, pfnToRemove):
        print "WinImpl.removeFile(%s)" % pfnToRemove
        return "WIN!!!"


    def executeCommand(self, command):
        return 0



class FailImpl(StageOutImpl):
    """
    _FailImpl_

    Test plugin that always results in a StageOutFailure

    """

    def createSourceName(self, protocol, lfn):
        return "FAIL!!!"


    def createStageOutCommand(self, sourcePFN, targetPFN, options = None):
        print "FailImpl.createStageOutCommand(%s, %s, %s)" % (sourcePFN, targetPFN, options)
        return "FAIL!!!"


    def removeFile(self, pfnToRemove):
        print "FailImpl.removeFile(%s)" % pfnToRemove
        return "FAIL!!!"


    def executeCommand(self, command):
        msg = "FailImpl returns FAIL!!!"
        raise StageOutFailure( msg)


registerStageOutImpl("test-win", WinImpl)
registerStageOutImpl("test-fail", FailImpl)
