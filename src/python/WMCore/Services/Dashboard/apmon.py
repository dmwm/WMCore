
"""
 * ApMon - Application Monitoring Tool
 * Version: 2.2.4
 *
 * Copyright (C) 2006 California Institute of Technology
 *
 * Permission is hereby granted, free of charge, to use, copy and modify
 * this software and its documentation (the "Software") for any
 * purpose, provided that existing copyright notices are retained in
 * all copies and that this notice is included verbatim in any distributions
 * or substantial portions of the Software.
 * This software is a part of the MonALISA framework (http://monalisa.cacr.caltech.edu).
 * Users of the Software are asked to feed back problems, benefits,
 * and/or suggestions about the software to the MonALISA Development Team
 * (developers@monalisa.cern.ch). Support for this software - fixing of bugs,
 * incorporation of new features - is done on a best effort basis. All bug
 * fixes and enhancements will be made available under the same terms and
 * conditions as the original software,

 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY DERIVATIVES THEREOF,
 * EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. THIS SOFTWARE IS
 * PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE NO
 * OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 * MODIFICATIONS.
"""

"""
apmon.py

This is a python implementation for the ApMon API for sending
data to the MonALISA service.

For further details about ApMon please see the C/C++ or Java documentation
You can find a sample usage of this module in apmTest.py.

Note that the parameters must be either integers(32 bits) or doubles(64 bits).
Sending strings is supported, but they will not be stored in the
farm's store nor shown in the farm's window in the MonALISA client.
"""

import xdrlib
import socket
import time
import random
import copy

#__all__ = ["ApMon"]

#__debug = False # set this to True to be verbose


class ApMon:
    """
    Main class for sending monitoring data to a MonaLisa module.
    One or more destinations can be chosen for the data. See constructor.

    The data is packed in UDP datagrams, using XDR. The following fields are sent:
    - version & password (string)
    - cluster name (string)
    - node name (string)
    - number of parameters (int)
    - for each parameter:
        - name (string)
        - value type (int)
        - value
    - optionally a (int) with the given timestamp

    Attributes (public):
    - destinations - a list containing (ip, port, password) tuples
    - configAddresses - list with files and urls from where the config is read
    - configRecheckInterval - period, in seconds, to check for changes
      in the configAddresses list
    - configRecheck - boolean - whether to recheck periodically for changes
      in the configAddresses list
    """

    __defaultOptions = {};

    def __init__ (self, initValue, logr):
        """
        Class constructor:
        - if initValue is a string, put it in configAddresses and load destinations
          from the file named like that. if it starts with "http://", the configuration
          is loaded from that URL. For background monitoring, given parameters will overwrite defaults

        - if initValue is a list, put its contents in configAddresses and create
          the list of destinations from all those sources. For background monitoring,
          given parameters will overwrite defaults (see __defaultOptions)

        - if initValue is a tuple (of strings), initialize destinations with that values.
          Strings in this tuple have this form: "{hostname|ip}[:port][ passwd]", the
          default port being 8884 and the default password being "". Background monitoring will be
          enabled sending the parameters active from __defaultOptions (see end of file)

        - if initValue is a hash (key = string(hostname|ip[:port][ passwd]),
          val = hash{'param_name': True/False, ...}) the given options for each destination
          will overwrite the default parameters (see __defaultOptions)
        """

        self.destinations = {}              # empty, by default; key = tuple (host, port, pass) ; val = hash {"param_mame" : True/False, ...}
        self.destPrevData = {}              # empty, by defaul; key = tuple (host, port, pass) ; val = hash {"param_mame" : value, ...}
        self.senderRef = {}            # key = tuple (host, port, pass); val = hash {'INSTANCE_ID', 'SEQ_NR' }
        self.configAddresses = []           # empty, by default; list of files/urls from where we read config
        self.maxMsgRate = 100            # Maximum number of messages allowed to be sent per second
        self.__defaultSenderRef = {'INSTANCE_ID': random.randint(0,0x7FFFFFFE), 'SEQ_NR': 0};
        self.__defaultUserCluster = "ApMon_UserSend";
        self.__defaultUserNode = socket.getfqdn();

        # don't touch these:
        self.__freed = False
        self.__udpSocket = None
        # don't allow a user to send more than MAX_MSG messages per second, in average
        self.__crtTime = 0;
        self.__prvTime = 0;
        self.__prvSent = 0;
        self.__crtSent = 0;
        self.__crtDrop = 0;
        self.__hWeight = 0.95;              # in (0,1) increase to wait more time before maxMsgRate kicks-in
        self.logger = logr
        self.setDestinations(initValue)
        self.__udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def sendParameters (self, clusterName, nodeName, params):
        """
        Send multiple parameters specifying cluster and node name for them
        """
        self.sendTimedParameters (clusterName, nodeName, -1, params);

    def sendTimedParameters (self, clusterName, nodeName, timeStamp, params):
        """
        Send multiple monitored parameters to MonALISA.

        """
        if (clusterName == None) or (clusterName == ""):
            clusterName = self.__defaultUserCluster
        else:
            self.__defaultUserCluster = clusterName
        if nodeName == None:
            nodeName = self.__defaultUserNode
        else:
            self.__defaultUserNode = nodeName
        if len(self.destinations) == 0:
            self.logger.error( "Not sending parameters since no destination is defined.");
            return
        for dest in self.destinations.keys():
            self.__directSendParams(dest, clusterName, nodeName, timeStamp, params);


    def free(self):
        """
        Stop background threads, close opened sockets. You have to use this function if you want to
        free all the resources that ApMon takes, and allow it to be garbage-collected.
        """

        if self.__udpSocket != None:
            self.logger.debug("Closing UDP socket on ApMon object destroy.");
            self.__udpSocket.close();
            self.__udpSocket = None;
        self.__freed = True


    #########################################################################################
    # Internal functions - Config reloader thread
    #########################################################################################

    def __directSendParams (self, destination, clusterName, nodeName, timeStamp, params):

        if self.__shouldSend() == False:
            self.logger.debug("Pausing half second since rate is too fast!");
            time.sleep(0.5)

        if destination == None:
            self.logger.debug("Destination is None");
            return;

        host, port, passwd = destination
        crtSenderRef = self.senderRef[destination]
        crtSenderRef['SEQ_NR'] = (crtSenderRef['SEQ_NR'] + 1) % 2000000000; # wrap around 2 mld

        xdrPacker = xdrlib.Packer ()

        xdrPacker.pack_string ("v:"+self.__version+"p:"+passwd)

        xdrPacker.pack_int (crtSenderRef['INSTANCE_ID'])
        xdrPacker.pack_int (crtSenderRef['SEQ_NR'])

        xdrPacker.pack_string (clusterName)
        xdrPacker.pack_string (nodeName)

        sent_params_nr = 0
        paramsPacker = xdrlib.Packer ()

        if type(params) == type( {} ):
            for name, value in params.iteritems():
                if self.__packParameter(paramsPacker, name, value):
                    sent_params_nr += 1
        elif type(params) == type( [] ):
            for name, value in params:
                self.logger.debug("Adding parameter "+name+" = "+str(value));
                if self.__packParameter(paramsPacker, name, value):
                    sent_params_nr += 1
        else:
            self.logger.debug("Unsupported params type in sendParameters: " + str(type(params)));

        xdrPacker.pack_int (sent_params_nr)

        if (timeStamp != None) and (timeStamp > 0):
            paramsPacker.pack_int(timeStamp);

        buffer = xdrPacker.get_buffer() + paramsPacker.get_buffer()
        self.logger.debug("Building XDR packet ["+str(clusterName)+"/"+str(nodeName)+"] <"+str(crtSenderRef['SEQ_NR'])+"/"+str(crtSenderRef['INSTANCE_ID'])+"> "+str(sent_params_nr)+" params, "+str(len(buffer))+" bytes.");
        # send this buffer to the destination, using udp datagrams
        try:
            self.__udpSocket.sendto(buffer, (host, port))
            self.logger.debug("Packet sent to "+host+":"+str(port)+" "+passwd)
        except socket.error as msg:
            self.logger.error("Cannot send packet to "+host+":"+str(port)+" "+passwd+": "+str(msg[1]))
        xdrPacker.reset()
        paramsPacker.reset()
    
    # private method converting unitcode to str
    def __convertUnicodeToStr(self, blob):
        if isinstance(blob, unicode) :
            try:
                return str(blob)
            except UnicodeEncodeError:
                #This contains some unicode outside ascii range
                return 'unknown'
        else:
            return blob
         
    def __packParameter(self, xdrPacker, name, value):
        if (name is None) or (name is ""):
            self.logger.debug("Undefined parameter name. Ignoring value "+str(value))
            return False
        if (value is None):
            self.logger.debug("Ignore " + str(name)+ " parameter because of None value")
            return False
        name = self.__convertUnicodeToStr(name)
        value = self.__convertUnicodeToStr(value)
        try:
            typeValue = self.__valueTypes[type(value)]
            xdrPacker.pack_string (name)
            xdrPacker.pack_int (typeValue)
            self.__packFunctions[typeValue] (xdrPacker, value)
            self.logger.debug("Adding parameter "+str(name)+" = "+str(value))
            return True
        except Exception as ex:
            self.logger.debug("Error packing %s = %s; got %s" % (name, str(value), ex))
            return False

    # Destructor
    def __del__(self):
        if not self.__freed:
            self.free();

    # Decide if the current datagram should be sent.
    # This decision is based on the number of messages previously sent.
    def __shouldSend(self):
        now = long(time.time());
        if now != self.__crtTime :
            # new time
            # update previous counters;
            self.__prvSent = self.__hWeight * self.__prvSent + (1.0 - self.__hWeight) * self.__crtSent / (now - self.__crtTime);
            self.__prvTime = self.__crtTime;
            self.logger.debug("previously sent: " + str(self.__crtSent) + "; dropped: " + str(self.__crtDrop));
            # reset current counter
            self.__crtTime = now;
            self.__crtSent = 0;
            self.__crtDrop = 0;

        # compute the history
        valSent = self.__prvSent * self.__hWeight + self.__crtSent * (1 - self.__hWeight);

        doSend = True;

        # when we should start dropping messages
        level = self.maxMsgRate - self.maxMsgRate / 10;

        if valSent > (self.maxMsgRate - level) :
            if random.randint(0,self.maxMsgRate / 10) >= (self.maxMsgRate - valSent):
                doSend = False;

        # counting sent and dropped messages
        if doSend:
            self.__crtSent+=1;
        else:
            self.__crtDrop+=1;

        return doSend;


    def setDestinations(self, initValue):
        """
        Set the destinations of the ApMon instance. It accepts the same parameters as the constructor.
        """

        if type(initValue) == type([]):
            self.configAddresses = []
            for dest in initValue:
                self.__addDestination (dest, self.destinations)
        elif type(initValue) == type({}):
            self.configAddresses = []
            for dest, opts in initValue.items():
                self.__addDestination (dest, self.destinations, opts)

    def __addDestination (self, aDestination, tempDestinations, options = {}):
        """
        Add a destination to the list.

        aDestination is a string of the form "{hostname|ip}[:port] [passwd]" without quotes.
        If the port is not given, it will be used the default port (8884)
        If the password is missing, it will be considered an empty string
        """
        aDestination = aDestination.strip().replace('\t', ' ')
        while aDestination != aDestination.replace('  ', ' '):
            aDestination = aDestination.replace('  ', ' ')
        sepPort = aDestination.find (':')
        sepPasswd = aDestination.rfind (' ')
        if sepPort >= 0:
            host = aDestination[0:sepPort].strip()
            if sepPasswd > sepPort + 1:
                port = aDestination[sepPort+1:sepPasswd].strip()
                passwd = aDestination[sepPasswd:].strip()
            else:
                port = aDestination[sepPort+1:].strip()
                passwd = ""
        else:
            port = str(self.__defaultPort)
            if sepPasswd >= 0:
                host = aDestination[0:sepPasswd].strip()
                passwd = aDestination[sepPasswd:].strip()
            else:
                host = aDestination.strip()
                passwd = ""
        if (not port.isdigit()):
            self.logger.error("Bad value for port number "+`port`+" in "+aDestination+" destination");
            return
        alreadyAdded = False
        port = int(port)
        try:
            host = socket.gethostbyname(host) # convert hostnames to IP addresses to avoid suffocating DNSs
        except socket.error as msg:
            self.logger.error("Error resolving "+host+": "+str(msg))
            return
        for h, p, w in tempDestinations.keys():
            if (h == host) and (p == port):
                alreadyAdded = True
                break
        destination = (host, port, passwd)
        if not alreadyAdded:
            self.logger.debug("Adding destination "+host+':'+`port`+' '+passwd)
            if(self.destinations.has_key(destination)):
                tempDestinations[destination] = self.destinations[destination]  # reuse previous options
            else:
                tempDestinations[destination] = copy.deepcopy(self.__defaultOptions)  # have a different set of options for each dest
            if not self.destPrevData.has_key(destination):
                self.destPrevData[destination] = {}    # set it empty only if it's really new
            if not self.senderRef.has_key(destination):
                self.senderRef[destination] = copy.deepcopy(self.__defaultSenderRef) # otherwise, don't reset this nr.
            if options != self.__defaultOptions:
                # we have to overwrite defaults with given options
                for key, value in options.items():
                    self.logger.debug("Overwritting option: "+key+" = "+`value`)
                    tempDestinations[destination][key] = value
        else:
            self.logger.debug("Destination "+host+":"+str(port)+" "+passwd+" already added. Skipping it");


    ################################################################################################
    # Private variables. Don't touch
    ################################################################################################

    __valueTypes = {
        type("string"): 0,    # XDR_STRING (see ApMon.h from C/C++ ApMon version)
        type(1): 2,         # XDR_INT32
        type(1.0): 5};         # XDR_REAL64

    __packFunctions = {
        0: xdrlib.Packer.pack_string,
        2: xdrlib.Packer.pack_int,
        5: xdrlib.Packer.pack_double }

    __defaultPort = 8884
    __version = "2.2.4-py"            # apMon version number
