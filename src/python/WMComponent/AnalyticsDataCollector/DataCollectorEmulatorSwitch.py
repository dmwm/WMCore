"""
Use this for only unit test
"""
from builtins import object

class EmulatorHelper(object):
    """
    Works as global value for the emulator switch.
    WARNNING: This is not multi thread safe.
    """
    #DO not change default values
    LocalCouchDBData = None
    ReqMonDBData = None
    WMAgentDBData= None

    @staticmethod
    def getEmulatorClass(clsName):

        if clsName == 'LocalCouchDBData':
            from WMQuality.Emulators.AnalyticsDataCollector.DataCollectorAPI \
                import LocalCouchDBData as LocalCouchDBDataEmulator
            return LocalCouchDBDataEmulator

        if clsName == 'ReqMonDBData':
            from WMQuality.Emulators.AnalyticsDataCollector.DataCollectorAPI \
                import ReqMonDBData as ReqMonDBDataEmulator
            return ReqMonDBDataEmulator

        if clsName == 'WMAgentDBData':
            from WMQuality.Emulators.AnalyticsDataCollector.DataCollectorAPI \
                import WMAgentDBData as WMAgentDBDataEmulator
            return WMAgentDBDataEmulator

    @staticmethod
    def setEmulators(localCouch=False,reqMon=False, wmagentDB=False):
        EmulatorHelper.LocalCouchDBData = localCouch
        EmulatorHelper.ReqMonDBData = reqMon
        EmulatorHelper.WMAgentDBData = wmagentDB

    @staticmethod
    def resetEmulators():
        EmulatorHelper.LocalCouchDBData = None
        EmulatorHelper.ReqMonDBData = None
        EmulatorHelper.WMAgentDBData = None

    @staticmethod
    def getClass(cls):
        """
        if emulator flag is set return emulator class
        otherwise return original class.
        if emulator flag is not initialized
            and EMULATOR_CONFIG environment variable is set,
        """
        emFlag = getattr(EmulatorHelper, cls.__name__)
        if emFlag:
            return EmulatorHelper.getEmulatorClass(cls.__name__)
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
                    return EmulatorHelper.getEmulatorClass(cls.__name__)
        # if emulator flag is False, return original class
        return cls

def emulatorHook(cls):
    """
    This is used as decorator to switch between Emulator and real Class
    on instance creation.
    """
    class EmulatorWrapper(object):
        def __init__(self, *args, **kwargs):
            aClass = EmulatorHelper.getClass(cls)
            self.wrapped = aClass(*args, **kwargs)
            self.__class__.__name__ = self.wrapped.__class__.__name__

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    return EmulatorWrapper
