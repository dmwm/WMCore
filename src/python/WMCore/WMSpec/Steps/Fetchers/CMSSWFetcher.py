#!/usr/bin/env python
"""
_CMSSWFetcher_

Fetch configfiles and PSet TWeaks for CMSSW Steps in a WMTask

"""
import os
import urllib
from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface
from WMCore.Cache.ConfigCache import WMConfigCache
import WMCore.WMSpec.WMStep as WMStep


class CMSSWFetcher(FetcherInterface):
    """
    _CMSSWFetcher_

    Pull configs from local config cache and add to sandbox

    """


    def __call__(self, wmTask):
        """
        Trip through steps, find CMSSW steps, pull in config files,
        PSet Tweaks etc

        """
        for t in wmTask.steps().nodeIterator():
            t = WMStep.WMStepHelper(t)
            stepPath = "%s/%s" % (self.workingDirectory(), t.name())

            # the CMSSW has a special case with its ConfigCache argument
            if not t.stepType() == "CMSSW": continue
            if (hasattr(t.data.application.configuration,'retrieveConfigUrl')):
                # main config file
                fileTarget = "%s/%s" % (
                    stepPath,
                    t.data.application.command.configuration)
                urllib.urlretrieve(
                    t.data.application.configuration.retrieveConfigUrl,
                    fileTarget)
                # PSet Tweak
                cacheUrl = t.data.application.configuration.configCacheUrl
                cacheDb  = t.data.application.configuration.cacheName
                configId = t.data.application.configuration.configId
                cacheUrl = cacheUrl.replace("http://", "")
                tweakTarget = t.data.application.command.psetTweak

                configCache = WMConfigCache(cacheDb,cacheUrl)
                tweak = configCache.getTweak(configId)
                if tweak:
                    tweakFile = "%s/%s" % (stepPath, tweakTarget)
                    tweak.persist(tweakFile, "json")




