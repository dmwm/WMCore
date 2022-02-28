"""
    This module is used to fork the current process into a daemon.
    Almost none of this is necessary (or advisable) if your daemon
    is being started by inetd. In that case, stdin, stdout and stderr are
    all set up for you to refer to the network connection, and the fork()s
    and session manipulation should not be done (to avoid confusing inetd).
    Only the chdir() and umask() steps remain as useful.
    References:
        UNIX Programming FAQ
            1.7 How do I get my program to act like a daemon?
                http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        Advanced Programming in the Unix Environment
            W. Richard Stevens, 1992, Addison-Wesley, ISBN 0-201-56317-7.

    History:
      2001/07/10 by Jorgen Hermann
      2002/08/28 by Noah Spurrier
      2003/02/24 by Clark Evans

      http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/66012
"""
from __future__ import print_function

from builtins import str
import logging
import os
import sys
import time
from logging.handlers import RotatingFileHandler
from xml.dom.minidom import Document

# File mode creation mask of the daemon.
UMASK = 0o022


def daemonize(stdout='/dev/null', stderr=None, stdin='/dev/null',
              workdir=None, startmsg='started with pid %s',
              keepParent=False):
    """
        This forks the current process into a daemon.
        The stdin, stdout, and stderr arguments are file names that
        will be opened and be used to replace the standard file descriptors
        in sys.stdin, sys.stdout, and sys.stderr.
        These arguments are optional and default to /dev/null.
        Note that stderr is opened unbuffered, so
        if it shares a file with stdout then interleaved output
        may not appear in the order that you expect.
    """
    # Do first fork.
    try:
        pid = os.fork()
        if pid > 0:
            if not keepParent:
                os._exit(0)  # Exit first parent.
            return pid
    except OSError as e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        print("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)
    # Decouple from parent environment.
    os.chdir("/")
    os.umask(UMASK)
    os.setsid()

    # Do second fork.
    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)  # Exit second parent.
    except OSError as e:
        sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        print("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Open file descriptors and print start message
    if not stderr:
        stderr = stdout
    si = open(stdin, 'r')
    so = open(stdout, 'a+')
    se = open(stderr, 'a+')
    pid = str(os.getpid())
    sys.stderr.write("\n%s\n" % startmsg % pid)
    sys.stderr.flush()
    if workdir:
        # open(pidfile,'w+').write("%s\n" % pid)
        # Since the current working directory may be a mounted filesystem, we
        # avoid the issue of not being able to unmount the filesystem at
        # shutdown time by changing it to the root directory.
        os.chdir(workdir)
        # We probably don't want the file mode creation mask inherited from
        # the parent, so we give the child complete control over permissions.
        os.umask(UMASK)

        xmlDoc = Document()
        daemon = xmlDoc.createElement("Daemon")
        processId = xmlDoc.createElement("ProcessID")
        processId.setAttribute("Value", str(os.getpid()))
        daemon.appendChild(processId)

        parentProcessId = xmlDoc.createElement("ParentProcessID")
        parentProcessId.setAttribute("Value", str(os.getppid()))
        daemon.appendChild(parentProcessId)

        processGroupId = xmlDoc.createElement("ProcessGroupID")
        processGroupId.setAttribute("Value", str(os.getpgrp()))
        daemon.appendChild(processGroupId)

        userId = xmlDoc.createElement("UserID")
        userId.setAttribute("Value", str(os.getuid()))
        daemon.appendChild(userId)

        effectiveUserId = xmlDoc.createElement("EffectiveUserID")
        effectiveUserId.setAttribute("Value", str(os.geteuid()))
        daemon.appendChild(effectiveUserId)

        groupId = xmlDoc.createElement("GroupID")
        groupId.setAttribute("Value", str(os.getgid()))
        daemon.appendChild(groupId)

        effectiveGroupId = xmlDoc.createElement("EffectiveGroupID")
        effectiveGroupId.setAttribute("Value", str(os.getegid()))
        daemon.appendChild(effectiveGroupId)

        with open("Daemon.xml", "w") as props:
            props.write(daemon.toprettyxml())

    # Redirect standard file descriptors.
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    return 0


def test():
    """
        This is an example main function run by the daemon.
        This prints a count and timestamp once per second.
    """
    logFile = os.path.join("/tmp/daemon.logging")
    logHandler = RotatingFileHandler(logFile, "a", 1000000000, 3)
    logFormatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    logHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(logHandler)
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Message to log file")

    sys.stdout.write('Message to stdout...')
    sys.stderr.write('Message to stderr...')
    c = 0
    while True:
        sys.stdout.write('%d: %s\n' % (c, time.ctime(time.time())))
        logging.info('%d: %s\n', c, time.ctime(time.time()))
        sys.stdout.flush()
        c = c + 1
        time.sleep(1)
    logging.info('>>>Starting:<<<')


def createDaemon(workdir, keepParent=False):
    """
    This is a wrapper over the new daemon methods.
    That follows the same interface.

    """
    pidfile = os.path.join(workdir, 'Daemon.xml')
    startmsg = 'started with pid %s'
    try:
        pf = open(pidfile, 'r')
        pid = (pf.read().strip())
        pf.close()
    except IOError:
        pid = None
    if pid:
        mess = """
Start aborted since pid file '%s' exists.
Please kill process and remove file first.
If process is still running this file contains
information on that.
"""
        print(mess % pidfile)
        sys.exit(1)

    return daemonize(workdir=workdir, startmsg=startmsg, keepParent=keepParent)


if __name__ == "__main__":
    parent_id = createDaemon('/tmp', keepParent=False)
    if parent_id == 0:
        test()
    print('Kept parent: ' + str(parent_id))
