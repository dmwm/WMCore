#!/usr/bin/env python
"""
_Template_

Template class for all Step Template implementations to inherit and implement
the API

"""
import os

from WMCore.WMSpec.WMStep import WMStepHelper
from WMCore.WMSpec.ConfigSectionTree import nodeName

class CoreHelper(WMStepHelper):
    """
    _CoreHelper_

    Helper API for core settings

    """
    def stepName(self):
        """
        _stepName_

        Get the name of the step

        """
        return nodeName(self.data)


    def addEnvironmentVariable(self, varname, setting):
        """
        _addEnvironmentVariable_

        add a key = value style setting to the environment for this
        step

        """
        setattr(self.data.environment.variables, varname, setting)
        return

    def addEnvironmentPath(self, pathname, setting):
        """
        _addEnvironmentPath_

        add a key = value1:value2:value3 environment setting to this step

        """
        if getattr(self.data.environment.paths, pathname, None) == None:
            setattr(self.data.environment.paths, pathname, [])
        pathentry = getattr(self.data.environment.paths, pathname)
        pathentry.append(setting)
        return

    def environment(self):
        """
        _environment_

        Get the environment settings for this step

        """
        return self.data.environment

    def addDirectory(self, dirName):
        """
        _addDirectory_

        Add a subdirectory structure to the template that will be built by
        the builder

        """
        split = dirName.split("/")
        split = [ x for x in split if x.strip() != "" ]

        dirs = getattr(self.data.build.directories, self.stepName())
        for subdir in split:
            exists = getattr(dirs, subdir, None)
            if exists == None:
                dirs.section_(subdir)
            dirs = getattr(dirs, subdir)
        return dirs



    def addFile(self, fileName, newLocation = None):
        """
        _addFile_

        Add a file to the job at build time. This file must be
        a local filesystem file available at fileName.

        An optional location within the step can be specified which
        may include a path structure that gets translated into calls
        to addDirectory

        """
        dirs = getattr(self.data.build.directories, self.stepName())
        if newLocation != None:
            filename = os.path.basename(newLocation)
            dirname = os.path.dirname(newLocation)
            dirs = self.addDirectory(dirname)
            setattr(dirs, filename, { "Source" : fileName, "Target" : filename})
        else:
            filename = os.path.basename(fileName)
            setattr(dirs, filename, {"Target" : filename, "Source" : fileName })
        return


    def directoryStructure(self):
        """
        _directoryStructure_

        Util to retrieve the directory structure

        """
        return self.data.build.directories

class Template:
    """
    _Template_

    Base interface definition for any WMStep Template

    """

    def __init__(self):
        pass

    def __call__(self, step):
        """
        _operator(step)_

        Install the template on the step instance provided

        """
        self.coreInstall(step)
        self.install(step)


    def coreInstall(self, step):
        """
        _coreInstall_

        Install attributes common to all steps

        """
        # Environment settings to pass to the step
        step.section_("environment")
        step.environment.section_("variables")
        step.environment.section_("paths")

        # Directory structure and files to be included in the job
        # beyond those that would be added by a Step Specific builder
        # Step Specific subclasses can simply append to these to get files
        # and dirs into the job
        step.section_("build")
        step.build.section_("directories")
        step.build.directories.section_(nodeName(step))






    def install(self, step):
        """
        _install_

        Override this method to install the required attributes
        in the step Instance provided

        """
        msg = "WMSpec.Steps.Template.install method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError(msg)


    def helper(self, step):
        """
        _helper_

        Wrap the step instance in a helper class tailored to this particular
        step type

        """
        msg = "WMSpec.Steps.Template.helper method not overridden in "
        msg += "implementation: %s\n" % self.__class__.__name__
        raise NotImplementedError(msg)
