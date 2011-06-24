#!/usr/bin/env python
"""
    _SandboxCreator_

    Given a path, workflow and task, create a sandbox within the path
"""


import os
import re
import tarfile
import tempfile
import WMCore.WMSpec.WMTask as WMTask
import WMCore.WMSpec.WMStep as WMStep
import urllib
import urlparse

import WMCore
import PSetTweaks
from WMCore.WMSpec.Steps.StepFactory import getFetcher


def tarballExclusion(path):
    """
    _tarballExclusion_

    Eliminates all unnecessary packages
    """
    patternList = ['.svn', '.git', '.pyc']

    for pattern in patternList:
        if re.search(pattern, path):
            return True
    return False

class SandboxCreator:

    def __init__(self):
        self.packageWMCore = True

    def disableWMCorePackaging(self):
        """
            __disableWMCorePackaging__

            use to keep the sandboxer from adding WMCore/* to the sandbox.
            testing would take forever otherwise
        """
        self.packageWMCore = False

    def extractSandbox(self, archivePath, targetPath):
        """
            __extractSandbox__

            extracts a sandbox at the given archivePath to the given targetPath
        """
        os.makedirs(targetPath)
        archive = tarfile.TarFile(archivePath)
        archive.extractall(targetPath)
        archive.close()


    def _makePathonPackage(self, path):
        os.makedirs( path )
        initHandle = open(path + "/__init__.py", 'w')
        initHandle.write("# dummy file for now")
        initHandle.close()
        
    def makeSandbox(self, buildItHere, workload):
        """
            __makeSandbox__

            MakeSandbox creates and archives a sandbox in buildItHere,
            returning the path to the archive and putting it in the
            task
        """
        workloadName = workload.name()
        # Create path to sandbox
        path = "%s/%s/WMSandbox" % (buildItHere, workloadName)
        workloadFile = os.path.join(path, "WMWorkload.pkl")
        archivePath = os.path.join(buildItHere, "%s/%s-Sandbox.tar.bz2" % (workloadName, workloadName))
        # check if already built
        if os.path.exists(archivePath) and os.path.exists(workloadFile):
            workload.setSpecUrl(workloadFile) # point to sandbox spec
            return archivePath

        #  //
        # // Set up Fetcher plugins, use default list for maintaining
        #//  compatibility
        commonFetchers = [ "CMSSWFetcher", "URLFetcher", "PileupFetcher" ]

        # generate the real path and make it
        self._makePathonPackage(path)
        
        # Add sandbox path to workload
        workload.setSandbox(archivePath)
        userSandboxes = []
        for topLevelTask in workload.taskIterator():
            for taskNode in topLevelTask.nodeIterator():
                task = WMTask.WMTaskHelper(taskNode)
                
                fetcherNames = commonFetchers[:]
                taskFetchers = getattr(task.data, "fetchers", [])
                fetcherNames.extend(taskFetchers)
                fetcherInstances = map(getFetcher, fetcherNames)
                
                taskPath = "%s/%s" % (path, task.name())
                self._makePathonPackage(taskPath)
                
                
                #TODO sandbox is property of workload now instead of task
                #but someother places uses as task propery (i.e. TaskQueue)
                # so backward compatability save as task attribute as well.
                setattr(task.data.input, 'sandbox', archivePath)
        
                for s in task.steps().nodeIterator():
                    s = WMStep.WMStepHelper(s)
                    stepPath = "%s/%s" % (taskPath, s.name())
                    self._makePathonPackage(stepPath)
                    userSandboxes.extend(s.getUserSandboxes())

                #  //
                # // Execute the fetcher plugins
                #//
                for fetcher in fetcherInstances:
                    fetcher.setWorkingDirectory(taskPath)
                    fetcher(task)

        
        
        # pickle up the workload for storage in the sandbox
        workload.setSpecUrl(workloadFile)
        workload.save(workloadFile)

        # now, tar everything up and put it somewhere special
        #(archiveHandle,archivePath) = tempfile.mkstemp('.tar.bz2','sbox',
        #                                              buildItHere)

        pythonHandle = open(archivePath, 'w+b')
        archive = tarfile.open(None,'w:bz2', pythonHandle)
        archive.add("%s/%s/" % (buildItHere, workloadName), '/',
                    exclude = tarballExclusion)
        if (self.packageWMCore):
            # package up the WMCore distribution
            # hopefully messing with this magic isn't a recipie for disaster
            wmcorePath = WMCore.__path__[0]
            #archive.add(wmcorePath, '/WMCore/', exclude = removePycFiles)
            archive.add(wmcorePath, '/WMCore/', exclude = tarballExclusion)
            psetTweaksPath = PSetTweaks.__path__[0]
            archive.add(psetTweaksPath, '/PSetTweaks',
                        exclude = tarballExclusion)

        for sb in userSandboxes:
            splitResult = urlparse.urlsplit(sb)
            if not splitResult[0]:
                archive.add(sb, os.path.basename(sb))

        archive.close()
        pythonHandle.close()



        return archivePath



