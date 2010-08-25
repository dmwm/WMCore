#!/usr/bin/env python
import xdrlib
import random
import types
import socket
import time

from WMCore.WMRuntime.Tools.Plugins.ApMonLite.ApMonException import ApMonException

#  //
# // Copy version string from apmon.py, dont know if it makes a difference
#//



#  //
# // Map python types to XDR Types for packaging
#//
_ValuePackers = {
    types.StringType:  (0, xdrlib.Packer.pack_string), # strings
    types.IntType   :  (2, xdrlib.Packer.pack_int),	# integer XDR_INT32
    types.FloatType :  (5, xdrlib.Packer.pack_double),	# float XDR_REAL64
    }


class ApMonDestination(dict):
    """
    _ApMonDestination_

    Object that represents an ApMon Destination and allows
    sending of key/value pairs of data to it in a transaction
    based manner

    """
    def __init__(self, destHost, destPort, destPasswd, cluster, node):
        self.setdefault("Host", destHost)
        self.setdefault("Port", destPort)
        self.setdefault("Passwd", destPasswd)
        self.setdefault("Cluster" , cluster)
        self.setdefault("Node", node)
        self.setdefault("Timestamp", False)
        self.setdefault("InstanceID", random.randint(0, 0x7FFFFFFE))
        self.setdefault("SequenceNumber", 0)
        self.udpSocket = None
                        
    def connect(self):
        """
        _connect_

        Bind to a UDP Socket to send data

        """
        if self.udpSocket == None:
            try:
                self.udpSocket = socket.socket(socket.AF_INET,
                                               socket.SOCK_DGRAM)

            except StandardError, ex:
                msg = "Error in ApMonDestination.connect:\n"
                msg += "Unable to create UDP Socket:\n"
                msg += str(ex)
                raise ApMonException(msg, Destination = str(self))
            
            
        return

    def disconnect(self):
        """
        _disconnect_

        Unbind and close UDP socket

        """
        if self.udpSocket == None:
            return
        self.udpSocket.close()
        self.udpSocket = None
        return


    def send(self, **params):
        """
        _send_

        Send set of key/value pairs as a UDP broadcast to the
        destination represented by this instance

        returns 0 if send successful, 1 otherwise

        Exceptions within this class will be propagated back as
        ApMonExceptions.
        
        """
        try:
            udpPacket = self.makeUDPPacket(**params)
        except StandardError, ex:
            msg = "StandardError creating UDP Packet:\n"
            msg += str(ex)
            raise ApMonException(msg, Destination = str(self))

        return self.udpBroadcast(udpPacket)
    
    

    def makeUDPPacket(self, **params):
        #  //
        # // UDP Packager
        #//
        packer = xdrlib.Packer()
        #  //
        # // first pack version of this client and the dest password
        #//
        packer.pack_string ("v:%sp:%s" % (__version__, self['Passwd']))
    
        #  //
        # // Now we pack these two random undocumented integers...
        #//  after incrementing them, with a wrap to 0 after 2000M
        self['SequenceNumber'] = (self['SequenceNumber'] + 1) % 2000000000
        
        packer.pack_int (self['InstanceID'])
        packer.pack_int (self['SequenceNumber'])

        #  //
        # // Pack the clustername and node name
        #//
        packer.pack_string (self['Cluster'])
        packer.pack_string (self['Node'])

        #  //
        # // pack the number of parameters
        #//
        packer.pack_int (len(params))

        #  //
        # // pack the parameters themselves
        #//
        for name, value in params.items():
            self.packParameter(packer, name, value)

        #  //
        # // pack the timestamp if required
        #//
        if self['Timestamp']:
            packer.pack_int(int(time.time()))

        return packer.get_buffer()

    def packParameter(self, packerRef, name, value):
        """
        _packParameter_
        
        Pack a key:value pair into the packer provided based on type
        
        """
        #  //
        # // validate name
        #//
        if name in ("", None):
            return

        #  //
        # // validate value
        #//
        if value == None:
            return
        if type(value) not in _ValuePackers.keys():
            return

        #  //
        # //  pack value with type info
        #//
        typeValue = _ValuePackers[type(value)][0]
        packerRef.pack_string (name)
        packerRef.pack_int (typeValue)
        _ValuePackers[type(value)][1] (packerRef, value)
        return


    def udpBroadcast(self, bufferContent):
        """
        _udpBroadcast_
        
        send bufferContent to the host and port provided 
        
        If not already connected, open and close connection, else
        use current connection
        
        """
        openedSocket = False
        if self.udpSocket == None:
            self.connect()
            openedSocket = True
        returnValue = 0
        try:
            self.udpSocket.sendto(bufferContent, (self['Host'], self['Port']))
        except StandardError, ex:
            returnValue = 1
        except Exception, ex:
            returnValue = 2
        if openedSocket: 
            self.disconnect()
        return returnValue

    def __str__(self):
        """make string rep of this object"""
        result = "<ApMonDestination:"
        result += "%s:" % self["Host"]
        result += "%s>" % self["Port"]
        return result

if __name__ == '__main__':

    destHost = "cms-pamon.cern.ch"
    destPort =  8884 # ApMon destination port here
    destPasswd = ''
    clusterName = "evansdetest"
    nodeName = "twoflower.fnal.gov"
    params = {
        "MonitorID" : "evansde-dashboard-test",
        "MonitorJobID" : "1",
        #  "intParam" : 1234,
        #"floatParam" : 456.78,
        }
    
    
    apMonDest = ApMonDestination(destHost, destPort, destPasswd,
                                 clusterName, nodeName)
    apMonDest['InstanceID'] = 222233333
    apMonDest.connect()
    for i in range(1, 100):
        print apMonDest.send(**params)
    apMonDest.disconnect()

    
