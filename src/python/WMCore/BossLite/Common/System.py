#!/usr/bin/env python
"""
_System_

"""

from ProdCommon.BossLite.Common.Exceptions import TimeOut
from subprocess import Popen, PIPE, STDOUT
import time
import os
import logging
import select, signal, fcntl

__version__ = "$Id: System.py,v 1.1 2010/04/27 08:25:46 spigafi Exp $"
__revision__ = "$Revision: 1.1 $"


def setPgid():
    """
    preexec_fn for Popen to set subprocess pgid
    
    """

    os.setpgid( os.getpid(), 0 )


def executeCommand( command, timeout=None ):
    """
    _executeCommand_

    It executes the command provided in a popen object with a timeout
    """

    start = time.time()
    p = Popen( command, shell=True, \
               stdin=PIPE, stdout=PIPE, stderr=STDOUT, \
               close_fds=True, preexec_fn=setPgid )

    # playing with fd
    fd = p.stdout.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    # return values
    timedOut = False
    outc = []

    while 1:
        (r, w, e) = select.select([fd], [], [], timeout)

        if fd not in r :
            timedOut = True
            break

        read = p.stdout.read()
        if read != '' :
            outc.append( read )
        else :
            break

    if timedOut :
        stop = time.time()
        try:
            os.killpg( os.getpgid(p.pid), signal.SIGTERM)
            os.kill( p.pid, signal.SIGKILL)
            p.wait()
            p.stdout.close()
        except OSError, err :
            logging.warning(
                'Warning: an error occurred killing subprocess [%s]' \
                % str(err) )

        raise TimeOut( command, ''.join(outc), timeout, start, stop )


    try:
        p.wait()
        p.stdout.close()
    except OSError, err:
        logging.warning( 'Warning: an error occurred closing subprocess [%s] %s  %s' \
                         % (str(err), ''.join(outc), p.returncode ))

    returncode = p.returncode
    if returncode is None :
        returncode = -666666

    return ''.join(outc), returncode



def evalStdList( strList ) :
    """
    _evalStdList_

    eval of a string which is espected to be a list
    it works for strings created with str([...])
    """

    strList = strList[ 1 : -1 ]

    if strList == '':
        return []
    if strList[0] == "'" or strList[0] == '"':
        return [ str(val[ 1 : -1 ]) for val in strList.split(', ') ]
    else :
        return [ str(val) for val in strList.split(',') ]


def evalCustomList( strList ) :
    """
    _evalCustomList_

    eval of a string which is espected to be a list
    it works for any well formed string representing a list
    """
    
    # strList = strList[ strList.find('[')+1 : strList.rfind(']') ].strip()
    strList = strList.strip()
    strList = strList.strip('[]')

    if strList == '':
        return []
    if strList[0] == "'": 
        return [ str(val[ val.find("'")+1 : val.rfind("'") ])
                 for val in strList.split(',') ]
    elif strList[0] == '"':
        return [ str(val[ val.find('"')+1 : val.rfind('"') ])
                 for val in strList.split(',') ]
    else :
        return [ str(val) for val in strList.split(',') ]
