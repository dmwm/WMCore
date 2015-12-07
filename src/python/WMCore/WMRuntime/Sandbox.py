#!/usr/bin/env python
"""
_Sandbox_

Sandbox access API to lookup and retrieve files from the sandbox area
at runtime.

Sandbox is found by looking for WMSandbox module and working down
the module tree imposed by it


"""

import os
import inspect

class Sandbox:
    """
    _Sandbox_


    """
    def __init__(self, taskName, stepName):
        self.moduleName = "WMSandbox.%s.%s" % (taskName, stepName)
        self.module = __import__(self.moduleName,
                                 globals(), locals(), [stepName])
                                 #globals(), locals(), [stepName], -1)
        self.directory = os.path.dirname(inspect.getsourcefile(self.module))


    def listFiles(self):
        """
        _listFiles_

        List files in sandbox for this instance

        """
        allFiles = [ x for x in os.listdir(self.directory)
                     if not x.startswith("__init__") ]
        return allFiles



    def getFile(self, filename):
        """
        _getFile_

        Get the absolute location of the file in the sandbox

        """
        if filename not in self.listFiles():
            msg = "Cannot find file %s in %s" % (filename, self.directory)
            #raise or return None here??
            raise RuntimeError(msg)
        return os.path.join(self.directory, filename)
