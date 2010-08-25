#!/usr/bin/env python
"""
    _SandboxCreator_

    Given a path, workflow and task, create a sandbox within the path
"""
__revision__ = "$Id: SandboxCreator.py,v 1.9 2009/09/04 18:39:27 evansde Exp $"
__version__ = "$Revision: 1.9 $"
import os
import re
import tarfile
import tempfile
import WMCore.WMSpec.WMStep as WMStep
import urllib
import WMCore

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

            # the CMSSW has a special case with its ConfigCache argument
            if (hasattr(t.data.application.configuration,'retrieveConfigUrl')):
                fileTarget = "%s/%s" % (
                    stepPath,
                    t.data.application.command.configuration)
                urllib.urlretrieve(
                    t.data.application.configuration.retrieveConfigUrl,
                    fileTarget)

            # within the step, the sandbox section has files to be imported
            # TODO: do we need a function to wrap this data call?
            for fileInfo in t.data.sandbox:
                # fileInfo.src is the source file
                # fileInfo.injob is where we stuck it
                match = re.search("^.*/(.*?)$", fileInfo.src)
                if (match):
                    fileSuffix = match.group(1)
                else:
                    fileSuffix = "sandboxFile.dat"

                fileTarget = "%s/%s" % (stepPath, fileSuffix)
                urllib.urlretrieve(fileInfo.src, fileTarget)
                fileInfo.injob = fileTarget

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

        archive.close()
        pythonHandle.close()



        return archivePath



