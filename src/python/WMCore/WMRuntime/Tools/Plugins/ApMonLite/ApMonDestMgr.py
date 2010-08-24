#!/usr/bin/env python
"""
_ApMonDestMgr_

Object that manages multicasts to a set of ApMonDestinations

"""
import random
import logging
from WMCore.WMRuntime.Tools.Plugins.ApMonLite.ApMonDestination import ApMonDestination


_Connect = lambda x: x.connect()
_Disconnect = lambda x: x.disconnect()


class ApMonDestMgr(list):
    """
    _ApMonDestMgr_

    List based container for managing a set of ApMonDestinations
    and broadcasting messages to them.

    Simple implementation that keeps a list of ApMonDestination instances
    and provides a simple API to add destinations, connect, disconnect and
    send messages

    """
    def __init__(self, clusterName, nodeName, instanceId = None):
        list.__init__(self)
        self.clusterName = clusterName
        self.nodeName = nodeName
        self.instanceId = instanceId
        if self.instanceId == None:
            self.instanceId = random.randint(0, 0x7FFFFFFE)



    def newDestination(self, host, port, password = ''):
        """
        _newDestination_

        Add a new destination to this list for the host, port, passwd
        triplet provided.

        The ApMonDestination instance is returned so that extra
        configuration can be performed if required

        """
        newDest = ApMonDestination( host, port, password, self.clusterName,
                                    self.nodeName)
        newDest['InstanceID'] = self.instanceId
        self.append(newDest)
        return newDest


    def connect(self):
        """
        _connect_

        Connect to UDP socket for all destinations

        """
        map(_Connect, self)
        return

    def disconnect(self):
        """
        _disconnect_

        Disconnect UDP socket for all destinations

        """
        map(_Disconnect, self)
        

    def send(self, **args):
        """
        _send_

        Broadcast key value data to all destinations

        """
        if len(self) == 0:
            logging.error("Attempting to use ApMonLite with no destinations")
        for dest in self:
            dest.send(**args)
        return
    


if __name__ == '__main__':
    mgr = ApMonDestMgr("evansdetest", "twoflower.fnal.gov")
    params = {
        "MonitorID" : "evansde-dashboard-test",
        "MonitorJobID" : "1",
        }


    mgr.newDestination("cms-pamon.cern.ch", 8884)
    mgr.newDestination("cithep90.ultralight.org", 58884)

     
    mgr.connect()
    for i in range(1, 100):
        mgr.send(**params)
    

    mgr.disconnect()
