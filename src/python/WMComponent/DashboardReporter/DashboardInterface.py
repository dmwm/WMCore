#!/usr/bin/env python

import sys
import xdrlib
import random
import types
import socket
import logging

from WMCore.Services.UUID import makeUUID

from WMCore.WMException import WMException




#  //
# // Map python types to XDR Types for packaging
#//
_ValuePackers = {
    types.StringType:  (0, xdrlib.Packer.pack_string), # strings
    types.IntType   :  (2, xdrlib.Packer.pack_int),	# integer XDR_INT32
    types.FloatType :  (5, xdrlib.Packer.pack_double),	# float XDR_REAL64
    }

class DashboardInterfaceException(WMException):
    """
    _DashboardInterfaceException_

    Something's wrong in communication via UDP
    """

    pass


class DashboardInterface(dict):
    """
    Class for actually talking to the dashboard.

    """



    def __init__(self, destHost, destPort, destPasswd, cluster = makeUUID(), node = makeUUID()):
        self.setdefault("Host", destHost)
        self.setdefault("Port", destPort)
        self.setdefault("Passwd", destPasswd)
        self.setdefault("Cluster" , cluster)
        self.setdefault("Node", node)
        self.setdefault("Timestamp", False)
        self.setdefault("InstanceID", random.randint(0, 0x7FFFFFFE))
        self.setdefault("SequenceNumber", 0)
        self.udpSocket = None


        return


    def connect(self):
        """
        _connect_

        Open and bind a UDP socket
        """
        if self.udpSocket == None:
            try:
                self.udpSocket = socket.socket(socket.AF_INET,
                                               socket.SOCK_DGRAM)

            except StandardError, ex:
                msg = "Error in DashboardInterface.connect:\n"
                msg += "Unable to create UDP Socket:\n"
                msg += str(ex)
                logging.error(msg)
                raise DashboardInterfaceException(msg)
            
            
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


    def send(self, package):
        """
        _send_

        Send set of key/value pairs as a UDP broadcast to the
        destination represented by this instance

        returns 0 if send successful, 1 otherwise

        package should be a normal python dictionary
        """
        try:
            udpPacket = self.makeUDPPacket(package)
        except StandardError, ex:
            msg = "StandardError creating UDP Packet:\n"
            msg += str(ex)
            logging.error(msg)
            logging.debug("Attempted to build package %s\n" % package)
            raise DashboardInterfaceException(msg, Destination = str(self))

        return self.udpBroadcast(udpPacket)

    


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
            bytes = self.udpSocket.sendto(bufferContent, (self['Host'], self['Port']))
        except StandardError, ex:
            logging.error("Encountered StandardError in udpBroadcast\n")
            logging.error(str(ex))
            returnValue = 1
        except Exception, ex:
            logging.error("Encounter unhandled Exception in udpBroadcast\n")
            logging.error(str(ex))
            returnValue = 2
        if bytes == 0:
            # Then we didn't send anything!
            logging.error("Did not send dashboard anything for package %s" % str(bufferContent))
        if openedSocket: 
            self.disconnect()
        return returnValue


    def makeUDPPacket(self, package):
        #  //
        # // UDP Packager
        #//
        packer = xdrlib.Packer()
        #  //
        # // first pack version of this client and the dest password
        #//
        __version__ = "WMCore_Version_Disabled"
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
        packer.pack_int (len(package.keys()))

        #  //
        # // pack the parameters themselves
        #//
        for name, value in package.items():
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



    
