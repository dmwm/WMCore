#!/usr/bin/python


__revision__ = "$Id: Heartbeat.py,v 1.1 2009/09/11 01:29:16 khawar Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "Khawar.Ahmad@cern.ch"

import threading
import time

#heart beat interval
HBEAT_TIME=300

class HeartBeat(threading.Thread):
    """
    _HeartBeat_

    This thread will send heartbeat msgs to TQ
    on regular intervals 
    """ 
    def __init__(self, comm, pilotInstance):
        """ 
        _init_ 
        """
        threading.Thread.__init__(self)
        self.commPlugin = comm
        self.pilotInstance = pilotInstance
        self.stopIt = False 

    def run( self ):
        """
        _run_
        
        Thread entry point method 
        """
        sleepTime = 30
        hbeatTimer = 0
        while ( not self.stopIt ):

            # First verify that pilot has got the ID
            if ( self.pilotInstance.pilotId == None ):
                time.sleep(5)
                continue

            # Check heartbeat
            hbeatTimer += sleepTime
            if hbeatTimer >= HBEAT_TIME:
                self.commPlugin.sendHeartbeatMsg()
                hbeatTimer = 0

            # Go to sleep
            time.sleep(sleepTime)


    
