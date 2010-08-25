#!/usr/bin/env python
"""
_System_

"""

from WMCore.BossLite.Common.Exceptions import TimeOut

from subprocess import Popen, PIPE, STDOUT
import time
import os
import logging
import select, signal, fcntl





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
        logging.warning( 'Warning: an error occurred closing ' + \
                            'subprocess [%s] %s  %s' % 
                            (str(err), ''.join(outc), p.returncode ))

    returncode = p.returncode
    if returncode is None :
        returncode = -666666

    return ''.join(outc), returncode


def decodeTimestamp( tmp ) :
    """
    decodeTimestamp
    """
    
    if not tmp :
        return 0
    
    return tmp


def encodeTimestamp( tmp ) :
    """
    encodeTimestamp
    """
    
    return tmp


def strToList( tmp ) :
    """
    eval of a string which is espected to be a list
    it works for strings created with str([...])
    """
    
    if tmp == '':
        return []
    else: 
        return tmp[:-1].split(',') 


def listToStr( tmp ) :
    """
    eval of a string which is espected to be a list
    it works for strings created with str([...])
    """

    output = str("")
    for i in tmp :
        output += str(i) + ',' 
        
    # return '"' + str(tmp).replace('"','""') + '"'
    return output
