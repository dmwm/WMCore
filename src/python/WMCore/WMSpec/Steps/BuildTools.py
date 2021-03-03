#!/usr/bin/env python
"""
_BuildTools_

Utils to assist in the build process


"""

from builtins import object
from future.utils import viewvalues

import os
import logging
from WMCore.WMSpec.ConfigSectionTree import nodeName


class File(object):
    """
    _File_

    Class representing a file added to a directory.

    Contains a source for the file and a name within
    the directory

    """
    def __init__(self, directory, name, source):
        self.directory = directory
        self.name = name
        self.source = source




    def path(self):
        """
        _path_

        Get name of this file within directory structure

        """
        return "%s/%s" % (self.directory.path(), self.name)

    def fetch(self, targetDir):
        """
        _fetch_

        Get the source and put it in the target dir.

        Note: for now this uses cp, could use other
        things based on source type, eg http:// etc etc

        """
        command = "/bin/cp -rf %s %s/%s" % (self.source,
                                        targetDir,
                                        self.name)
        logging.info("fetch:%s" % command)
        os.system(command)
        return




class Directory(object):
    """
    _Directory_

    structure representing a dir, to which files can be attached


    """
    def __init__(self, name):
        self.name = name
        self.parent = None
        self.children = {}
        self.files = {}
        self.physicalPath = None


    def addDirectory(self, name):
        """
        _addDirectory_

        Add a new child Directory to this.
        Return reference to new Directory instance

        """
        if name in self.children:
            return self.children[name]

        self.children[name] = Directory(name)
        self.children[name].parent = self
        return self.children[name]


    def addFile(self, source, targetName = None):
        """
        _addFile_

        Add a file to this directory.
        The file will be pulled in from the source specified.
        targetName is the optional name of the file in this
        directory. If not specified, the basename of the file
        will be used

        """
        target = targetName
        if target == None:
            target = os.path.basename(source)

        if target in self.files:
            msg = "File %s already exists in directory %s" % (
                self.name, target)
            raise RuntimeError(msg)

        newFile = File(self, target, source)
        self.files[target] = newFile
        return


    def path(self):
        """
        _path_

        Get name of this dir within directory structure

        """
        if self.parent == None:
            return self.name
        return "%s/%s" % (self.parent.path(), self.name)


    def create(self, targetDir):
        """
        _create_

        Make this directory in the targetDirectory provided,
        pull in all files and then recursively create any
        children

        """
        newDir = "%s/%s" % (targetDir, self.name)
        logging.info("create(%s)" % newDir)
        if not os.path.exists(newDir):
            os.makedirs(newDir)
        for f in viewvalues(self.files):
            f.fetch(newDir)

        for child in viewvalues(self.children):
            child.create(newDir)
        return

    def __str__(self):
        result = "%s\n" % self.path()
        for f in viewvalues(self.files):
            result += "%s ==> %s\n" % (f.path(), f.source)
        for d in viewvalues(self.children):
            result += str(d)

        return result








def processDir( cfgSect, parent):
    """
    _processDir_

    Process a ConfigSection based directory

    """
    for setting in cfgSect._internal_settings:
        value = getattr(cfgSect, setting)
        if not isinstance(value, dict): continue
        parent.addFile(value['Source'], value['Target'])

    for subdir in cfgSect._internal_children:
        newsubdir = parent.addDirectory(subdir)
        processDir(getattr(cfgSect, subdir), newsubdir)
    return


def makeDirectory(step):
    """
    _makeDirectory_

    Create a Directory & file structure from the step provided

    """

    dirs = getattr(step.build.directories, nodeName(step))
    topDir = Directory(nodeName(step))

    processDir(dirs, topDir)

    return topDir
