#!/usr/bin/env python
"""
    _SandboxCreator_

    Given a path, workflow and task, create a sandbox within the path
"""
__revision__ = "$Id: SandboxCreator.py,v 1.11 2009/10/19 18:44:59 evansde Exp $"
__version__ = "$Revision: 1.11 $"
import os
import re
import tarfile
import tempfile
import WMCore.WMSpec.WMStep as WMStep
import urllib
import WMCore
import PSetTweaks
from WMCore.WMSpec.Steps.StepFactory import getFetcher

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



    def makeSandbox(self, buildItHere, workload, task):
        """
            __makeSandbox__

            MakeSandbox creates and archives a sandbox in buildItHere,
            returning the path to the archive and putting it in the
            task
        """
        #  //
        # // Set up Fetcher plugins, use default list for maintaining
        #//  compatibility
        fetcherNames = [ "CMSSWFetcher", "URLFetcher" ]
        taskFetchers = getattr(task.data, "fetchers", [])
        fetcherNames.extend(taskFetchers)
        fetcherInstances = map(getFetcher, fetcherNames)



        # generate the real path and make it
        workloadName = workload.name()
        taskName     = task.name()
        path = "%s/%s/%s/WMSandbox" % (buildItHere, workloadName, taskName)
        os.makedirs( path )

        # for each step in the task, make a directory
        for t in task.steps().nodeIterator():
            t = WMStep.WMStepHelper(t)
            stepPath = "%s/%s" % (path, t.name())
            os.makedirs( stepPath )
            initHandle = open(stepPath + "/__init__.py", 'w')
            initHandle.write("# dummy file for now")
            initHandle.close()



        #  //
        # // Execute the fetcher plugins
        #//
        for fetcher in fetcherInstances:
            fetcher.setWorkingDirectory(path)
            fetcher(task)

        # and generate the __init__.py
        # TODO: find out what else should go in this init. I don't understand
        #   python's module system all the way yet
        initHandle = open(path + "/__init__.py", 'w')
        initHandle.write("# dummy file for now")
        initHandle.close()

        # pickle up the workload for storage in the sandbox
        workload.save(path + "/WMWorkload.pcl")

        # now, tar everything up and put it somewhere special
        (archiveHandle,archivePath) = tempfile.mkstemp('.tar.bz2','sbox',
                                                      buildItHere)


        task.data.sandboxArchivePath = archivePath
        pythonHandle = os.fdopen(archiveHandle,'w+b')

        #TODO: Tweak so the name of the sandbox is a bit more sensible
        archive = tarfile.open(None,'w:bz2', pythonHandle)
        archive.add("%s/%s/%s/" % (buildItHere, workloadName, taskName),'/')
        if (self.packageWMCore):
            # package up the WMCore distribution
            # hopefully messing with this magic isn't a recipie for disaster
            wmcorePath = WMCore.__path__[0]
            archive.add(wmcorePath, '/WMCore/')
            psetTweaksPath = PSetTweaks.__path__[0]
            archive.add(psetTweaksPath, '/PSetTweaks')
        archive.close()
        pythonHandle.close()



        return archivePath



