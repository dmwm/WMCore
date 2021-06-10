#!/usr/bin/env python
"""
    _SandboxCreator_

    Given a path, workflow and task, create a sandbox within the path
"""

from builtins import map, object

from future import standard_library

from Utils.PythonVersion import PY2
from Utils.Utilities import encodeUnicodeToBytes

standard_library.install_aliases()

import logging
import os
import shutil
import tarfile
import tempfile
import zipfile

from urllib.parse import urlsplit

import PSetTweaks
import Utils
import WMCore.WMSpec.WMStep as WMStep
import WMCore.WMSpec.WMTask as WMTask
from WMCore.WMSpec.Steps.StepFactory import getFetcher


def tarFilter(tarinfo):
    """
    _tarFilter_

    Filters what goes into a tarball
    """
    if tarinfo.name.endswith(".svn") or tarinfo.name.endswith(".git"):
        return None
    else:
        return tarinfo


class SandboxCreator(object):
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
        os.makedirs(path)
        with open(path + "/__init__.py", 'w') as initHandle:
            initHandle.write("# dummy file for now")

    def makeSandbox(self, buildItHere, workload):
        """
            __makeSandbox__

            MakeSandbox creates and archives a sandbox in buildItHere,
            returning the path to the archive and putting it in the
            task
        """
        workloadName = workload.name()
        # Create path to sandbox
        pileupCachePath = "%s/pileupCache" % buildItHere
        path = "%s/%s/WMSandbox" % (buildItHere, workloadName)
        workloadFile = os.path.join(path, "WMWorkload.pkl")
        archivePath = os.path.join(buildItHere, "%s/%s-Sandbox.tar.bz2" % (workloadName, workloadName))
        # check if already built
        if os.path.exists(archivePath) and os.path.exists(workloadFile):
            workload.setSpecUrl(workloadFile)  # point to sandbox spec
            return archivePath
        if os.path.exists(path):
            shutil.rmtree(path)
        # //
        # // Set up Fetcher plugins, use default list for maintaining
        # //  compatibility
        commonFetchers = ["CMSSWFetcher", "URLFetcher", "PileupFetcher"]

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
                fetcherInstances = list(map(getFetcher, fetcherNames))

                taskPath = "%s/%s" % (path, task.name())
                self._makePathonPackage(taskPath)

                # TODO sandbox is property of workload now instead of task
                # but someother places uses as task propery (i.e. TaskQueue)
                # so backward compatability save as task attribute as well.
                setattr(task.data.input, 'sandbox', archivePath)

                for s in task.steps().nodeIterator():
                    s = WMStep.WMStepHelper(s)
                    stepPath = "%s/%s" % (taskPath, s.name())
                    self._makePathonPackage(stepPath)
                    userSandboxes.extend(s.getUserSandboxes())

                # //
                # // Execute the fetcher plugins
                # //
                for fetcher in fetcherInstances:
                    # TODO: when cache directory is set as path, cache is maintained by workflow.
                    # In that case, cache will be deleted when workflow is done,
                    # but if different workflow can share the same cache.
                    # You can set the cache direcoty somewhere else, but need to have cache refresh (delete) policy
                    fetcher.setCacheDirectory(pileupCachePath)
                    fetcher.setWorkingDirectory(taskPath)
                    fetcher(task)

        # pickle up the workload for storage in the sandbox
        workload.setSpecUrl(workloadFile)
        workload.save(workloadFile)

        # now, tar everything up and put it somewhere special

        tarContent = []
        deleteFiles = []
        tarContent.append(("%s/%s/" % (buildItHere, workloadName), '/'))

        if self.packageWMCore:

            wmcorePath = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

            (zipHandle, zipPath) = tempfile.mkstemp()
            os.close(zipHandle)
            zipFile = zipfile.ZipFile(zipPath,
                                      mode='w',
                                      compression=zipfile.ZIP_DEFLATED)

            for (root, dirnames, filenames) in os.walk(wmcorePath):
                for filename in filenames:
                    if not filename.endswith(".svn") and not filename.endswith(".git"):
                        zipFile.write(filename=os.path.join(root, filename),
                                      # the name in the archive is the path relative to WMCore/
                                      arcname=os.path.join(root, filename)[len(wmcorePath) - len('WMCore/') + 1:])

            # Add a dummy module for zipimport testing
            (handle, dummyModulePath) = tempfile.mkstemp()
            # Python3 mkstemp file descriptors expects bytes-object
            os.write(handle, encodeUnicodeToBytes("#!/usr/bin/env python\n"))
            os.write(handle, encodeUnicodeToBytes("print('ZIPIMPORTTESTOK')\n"))
            os.close(handle)
            zipFile.write(filename=dummyModulePath, arcname='WMCore/ZipImportTestModule.py')

            # Add the wmcore zipball to the sandbox
            zipFile.close()
            tarContent.append((zipPath, '/WMCore.zip'))
            deleteFiles.append(dummyModulePath)
            deleteFiles.append(zipPath)

            psetTweaksPath = PSetTweaks.__path__[0]
            tarContent.append((psetTweaksPath, '/PSetTweaks'))

            utilsPath = Utils.__path__[0]
            tarContent.append((utilsPath, '/Utils'))

        for sb in userSandboxes:
            splitResult = urlsplit(sb)
            if not splitResult[0]:
                tarContent.append((sb, os.path.basename(sb)))

        with tarfile.open(archivePath, 'w:bz2') as tar:
            for (name, arcname) in tarContent:
                tar.add(name, arcname, filter=tarFilter)

        for deleteFile in deleteFiles:
            os.unlink(deleteFile)

        logging.info("Created sandbox %s with size %d",
                     os.path.basename(archivePath),
                     os.path.getsize(archivePath))

        return archivePath
