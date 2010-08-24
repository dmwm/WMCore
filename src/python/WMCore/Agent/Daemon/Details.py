#!/usr/bin/env python
"""
_DaemonDetails_

Util object to parse the Daemon parameters for a component and
convert it into a dictionary

Also, provides utils to shutdown the daemon process

"""

__revision__ = "$Id: Details.py,v 1.1 2008/10/07 13:54:04 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import os
import time
#FIXME: needs to be replaced with persistent backend.
from xml.dom.minidom import parse



class Details(dict):
    """
    _DaemonDetails_

    Dictionary of information about a Component Daemon
    along with some utility methods to manage it

    Components, upon startup, produce a Daemon.xml file in their component
    directory containing the details of the daemon process.
    This util takes that file, reads its fields into a dictionary.
    
    """
    def __init__(self, daemonXmlFile):
        dict.__init__(self)
        self.load(daemonXmlFile)


    def load(self, xmlFile):
        """
        _load_

        Load the XML values into this instance

        """
        dom = parse(xmlFile)
        daemons = dom.getElementsByTagName("Daemon")
        for daemon in daemons:
            childNodes = daemon.childNodes
            for childNode in childNodes:
                name = childNode.nodeName
                if name != "#text":
                    value = childNode.getAttribute("Value")
                    self[name] = int(value)

    def isAlive(self):
        """
        _isAlive_

        Is the process still running?

        Dumb check on /proc/pid existing. Anyone know a better way?

        """
        return os.path.exists("/proc/%s" % self['ProcessID'])

    def kill(self, signal = 15):
        """
        _kill_

        Kill the process with the signal provided

        """
        os.kill(self['ProcessID'], signal)
        time.sleep(1)
        return

    def killWithPrejudice(self, signal = 15):
        """
        _killWithPredjudice_

        Issue the kill, then watch to make sure it shuts down.
        If it takes more than a couple of seconds, kill -9 it.

        """
        os.kill(self['ProcessID'], signal)
        for count in range(0, 3):
            time.sleep(1)
            if not self.isAlive():
                return
            continue
        os.kill(self['ProcessID'], 9)
        return
