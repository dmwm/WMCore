"""
Use this for only unit test
"""
import os
import tempfile
import sys
import logging

from WMCore.Configuration import Configuration, saveConfigurationFile

class EmulatorHelper(object):
    """
    Works as global value for the emulator switch.
    WARNNING: This is not multi thread safe.
    """
    #DO not change default values
    PhEDEx = None
    DBSReader = None
    SiteDBJSON = None
    RequestManager = None

    @staticmethod
    def getEmulatorClass(clsName, *args):

        if clsName == 'PhEDEx':
            from WMQuality.Emulators.PhEDExClient.PhEDEx \
                import PhEDEx as PhEDExEmulator
            return PhEDExEmulator

        if clsName == 'DBSReader':
            if 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/' in args:
                from WMQuality.Emulators.DBSClient.DBS3Reader \
                    import DBS3Reader as DBSEmulator
            else:
                from WMQuality.Emulators.DBSClient.DBSReader \
                    import DBSReader as DBSEmulator
            print "Returnign DBSEmulator", DBSEmulator
            return DBSEmulator

        if clsName == 'SiteDBJSON':
            from WMQuality.Emulators.SiteDBClient.SiteDB \
                import SiteDBJSON as SiteDBEmulator
            return SiteDBEmulator

        if clsName == 'RequestManager':
            from WMQuality.Emulators.RequestManagerClient.RequestManager \
                import RequestManager as RequestManagerEmulator
            return RequestManagerEmulator

    @staticmethod
    def setEmulators(phedex=False, dbs=False, siteDB=False, requestMgr=False):
        EmulatorHelper.PhEDEx = phedex
        EmulatorHelper.DBSReader = dbs
        print "Setting DBSReader to", dbs
        EmulatorHelper.SiteDBJSON = siteDB
        EmulatorHelper.RequestManager = requestMgr

    @staticmethod
    def resetEmulators():
        EmulatorHelper.PhEDEx = None
        EmulatorHelper.DBSReader = None
        EmulatorHelper.SiteDBJSON = None
        EmulatorHelper.RequestManager = None

    @staticmethod
    def getClass(cls, *args):
        """
        if emulator flag is set return emulator class
        otherwise return original class.
        if emulator flag is not initialized
            and EMULATOR_CONFIG environment variable is set,
        r
        """
        emFlag = getattr(EmulatorHelper, cls.__name__)
        if emFlag:
            return EmulatorHelper.getEmulatorClass(cls.__name__, *args)
        elif emFlag == None:
            try:
                from WMQuality.Emulators import emulatorSwitch
            except:
                # if emulatorSwitch class is not imported don't use
                # emulator
                setattr(EmulatorHelper, cls.__name__, False)
            else:
                envFlag = emulatorSwitch(cls.__name__)
                setattr(EmulatorHelper, cls.__name__, envFlag)
                if envFlag:
                    return EmulatorHelper.getEmulatorClass(cls.__name__, *args)
        # if emulator flag is False, return original class
        return cls

def emulatorHook(cls):
    """
    This is used as decorator to switch between Emulator and real Class
    on instance creation.
    """
    class EmulatorWrapper:
        def __init__(self, *args, **kwargs):
            aClass = EmulatorHelper.getClass(cls, *args)
            self.wrapped = aClass(*args, **kwargs)
            self.__class__.__name__ = self.wrapped.__class__.__name__

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    return EmulatorWrapper
