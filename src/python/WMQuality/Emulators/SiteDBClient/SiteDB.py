#!/usr/bin/env python
"""
_SiteDBClient_

Emulating SiteDB
"""

class SiteDBJSON(object):

    """
    API for dealing with retrieving information from SiteDB
    """
    mapping = {'T2_XX_SiteA' : 'a.example.com', 'T2_XX_SiteB' : 'b.example.com'}


    def __init__(self, dict={}):
        pass

    def phEDExNodetocmsName(self, node):
        """
        Convert PhEDEx node name to cms site
        """
        # api doesn't work at the moment - so reverse engineer
        # first strip special endings and check with cmsNametoPhEDExNode
        # if this fails (to my knowledge no node does fail) do a full lookup
        name = node.replace('_MSS',
                            '').replace('_Buffer',
                                        '').replace('_Export', '')
        return name

    def cmsNametoSE(self, name):
        return self.mapping[name]

    def seToCMSName(self, name):
        if name == "cmssrm.fnal.gov":
            return "T1_US_FNAL"
        return name # for now se == site name

    def getAllCMSNames(self):
        """Return cms names"""
        return self.mapping.keys()
