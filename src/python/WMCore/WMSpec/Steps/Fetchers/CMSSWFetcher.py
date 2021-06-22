#!/usr/bin/env python
"""
_CMSSWFetcher_

Fetch configfiles and PSet TWeaks for CMSSW Steps in a WMTask

"""

from future import standard_library
standard_library.install_aliases()

from WMCore.WMSpec.Steps.Fetchers.FetcherInterface import FetcherInterface
from WMCore.Cache.WMConfigCache import ConfigCache

import WMCore.WMSpec.WMStep as WMStep

class CMSSWFetcher(FetcherInterface):
    """
    _CMSSWFetcher_

    Pull configs from local config cache and add to sandbox.
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
            if not t.stepType() in ("CMSSW"):
                continue
            if getattr(t.data.application.configuration, 'configCacheUrl', None) != None:
                # main config file
                fileTarget = "%s/%s" % (stepPath, t.data.application.command.configuration)
                #urllib.request.urlretrieve(
                #    t.data.application.configuration.retrieveConfigUrl,
                #    fileTarget)
                # PSet Tweak
                cacheUrl = t.data.application.configuration.configCacheUrl
                cacheDb  = t.data.application.configuration.cacheName
                configId = t.data.application.configuration.configId

                configCache = ConfigCache(cacheUrl, cacheDb)
                configCache.loadByID(configId)
                configCache.saveConfigToDisk(targetFile = fileTarget)
