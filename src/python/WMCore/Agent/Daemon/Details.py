#!/usr/bin/env python
"""
_DaemonDetails_

Util object to parse the Daemon parameters for a component and
convert it into a dictionary

Also, provides utils to shutdown the daemon process

"""
from __future__ import print_function

from builtins import str, range

import os
import subprocess
import shutil
import time
# FIXME: needs to be replaced with persistent backend.
from xml.dom.minidom import parse

from Utils.PythonVersion import PY3
from Utils.Utilities import encodeUnicodeToBytesConditional


def run(command):
    proc = subprocess.Popen(
        ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        )

    proc.stdin.write(encodeUnicodeToBytesConditional(command, condition=PY3))
    stdout, stderr = proc.communicate()
    rc = proc.returncode

    return stdout, stderr, rc


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
        self.daemonXmlFile = daemonXmlFile

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
        """
        # Reference: ps -T -p 1946167 -o euser,pid,ppid,lwp,nlwp,stat,start
        # it prints the user, process and its threads, number of threads, etc
        dummyse, dummyso, rc = run('ps -p %s' % self['ProcessID'])
        if rc != 0:
            return False
        return True

    def kill(self, signal=15):
        """
        _kill_

        Kill the process with the signal provided

        """
        os.kill(self['ProcessID'], signal)
        time.sleep(1)
        self.removeAndBackupDaemonFile()
        return

    def killWithPrejudice(self, signal=15):
        """
        _killWithPredjudice_

        Issue the kill, then watch to make sure it shuts down.
        If it takes more than a couple of seconds, kill -9 it.

        """
        os.killpg(self['ProcessGroupID'], signal)
        for dummycount in range(0, 3):
            time.sleep(1)
            if not self.isAlive():
                self.removeAndBackupDaemonFile()
                return
            continue
        os.killpg(self['ProcessGroupID'], 9)
        self.removeAndBackupDaemonFile()
        return

    def removeAndBackupDaemonFile(self):
        """
        Removes the daemon file (after a kill) and backs it up
        for post mortem.
        """
        path, dFile = os.path.split(self.daemonXmlFile)
        timeStamp = time.strftime("%d-%M-%Y")
        newFile = "%s.BAK.%s" % (dFile, str(timeStamp))
        newLocation = os.path.join(path, newFile)
        try:
            shutil.move(self.daemonXmlFile, newLocation)
        except Exception as ex:
            print('Move failed. Remove manual: ' + str(ex))
