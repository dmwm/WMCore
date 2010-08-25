#!/usr/bin/env python
"""
_InstallPSetTweak_

Runtime Script Implementation to add a PSetTweak to a PSet file within
a scram environment

"""


from WMCore.WMRuntime.ScriptInterface import ScriptInterface
from PSetTweaks.WMTweak import applyTweak
from PSetTweaks.PSetTweak import PSetTweak

class InstallPSetTweak(ScriptInterface):
    """
    _InstallPSetTweak_

    Use the step information to find the PSet Tweak file and
    PSet file, import them both and add the tweak to the
    PSet

    """


    def __call__(self):
        """
        _operator()_

        Implement loading & combining of tweak and PSet file

        TODO: Logging verbosity etc

        """
        step = self.stepSpace.getWMStep()
        psetFile = getattr(step.application.command, "configuration", None)
        psetTweak = getattr(step.application.command, "psetTweak", None)

        if psetFile == None:
            print "step.application.command.configuration not set"
            return 1
        if psetTweak == None:
            print "step.application.command.psetTweak not set"
            return 1


        tweak = PSetTweak()
        try:
            tweak.unpersist(psetTweak)
        except Exception, ex:
            print "Failed to load tweak: %s" % str(ex)
            return 1


        try:
            from FWCore.ParameterSet.Config import process
        except ImportError, ex:
            print "Failed to load FWCore.ParameterSet library"
            return 1


        psetModule = "WMTaskSpace.%s.PSet" % self.stepSpace.name()
        try:
            process = __import__(psetModule,
                                 globals(),
                                 locals(), ['process'], -1)

        except ImportError, ex:
            msg = "Unable to import process from %s:\n" % psetModule
            msg += str(ex)
            print msg
            return 1

        try:
            applyTweak(process, tweak)
        except Exception, ex:
            msg = "Unable to apply Tweak:\n %s" % str(ex)
            print msg
            return 1

        #
        # save it as PSet.py


        return 0




