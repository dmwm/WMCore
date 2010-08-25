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

__version__ = "$Id: System.py,v 1.6 2010/05/11 22:29:43 spigafi Exp $"
__revision__ = "$Revision: 1.6 $"


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
    if type(tmp) == str :
        # SQLite case --> string
        extractedTuple = time.strptime(tmp,'%Y-%m-%d %H:%M:%S')
        
    else :
        # MySQL case --> datetime object
        extractedTuple =  tmp.timetuple()
    
    # add/subtract the timezone ...
    result = time.mktime(extractedTuple[0:6] + (0,0,0)) - time.timezone    
    
    # Rounding ...
    return int(result)

def encodeTimestamp( tmp ) :
    """
    encodeTimestamp
    """
    
    try:
        # if I can, I convert...
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(tmp)))
    except TypeError :
        pass
    except ValueError :
        pass
    
    # 0 / invalid value -> 1970-01-01 00:00:00
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(0)))

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

    
def evalStdList( strList ) :
    """
    _evalStdList_ -> DEPRECATED
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
    _evalCustomList_ -> DEPRECATED
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
