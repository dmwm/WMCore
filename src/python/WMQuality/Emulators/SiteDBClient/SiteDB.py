#!/usr/bin/env python
"""
_SiteDBClient_

Emulating SiteDB
"""

import logging
import re

pnn_regex = re.compile(r'^T[0-3%]((_[A-Z]{2}(_[A-Za-z0-9]+)*)?)')

class SiteDBJSON(dict):
    """
    API for dealing with retrieving information from SiteDB
    """
    _people_data = [{'dn' : '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gutsche/CN=582680/CN=Oliver Gutsche',
                     'username' : 'gutsche'},
                    {'dn' : "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'",
                     'username' : 'liviof'}]

    _sitenames_data = [{u'site_name': u'T0_CH_CERN', u'type': u'cms', u'alias': u'T0_CH_CERN'}, # this is fake record not exists in siteDB
                       {u'site_name': u'T0_CH_CERN', u'type': u'phedex', u'alias': u'T0_CH_CERN_MSS'}, # this is fake record not exists in siteDB
                       {u'site_name': u'T0_CH_CERN', u'type': u'phedex', u'alias': u'T0_CH_CERN_Disk'}, # this is fake record not exists in siteDB
                       {u'site_name': u'T0_CH_CERN', u'type': u'phedex', u'alias': u'T0_CH_CERN_Export'}, # this is fake record not exists in siteDB{u'site_name': u'FNAL', u'type': u'cms', u'alias': u'T1_US_FNAL'},
                       {u'site_name': u'FNAL', u'type': u'cms', u'alias': u'T1_US_FNAL'},
                       {u'site_name': u'FNAL', u'type': u'phedex', u'alias': u'T1_US_FNAL_Disk'},
                       {u'site_name': u'FNAL', u'type': u'phedex', u'alias': u'T1_US_FNAL_Buffer'},
                       {u'site_name': u'FNAL', u'type': u'phedex', u'alias': u'T1_US_FNAL_MSS'},
                       {u'site_name': u'RAL', u'type': u'cms', u'alias': u'T1_UK_RAL'},
                       {u'site_name': u'RAL', u'type': u'phedex', u'alias': u'T1_UK_RAL_Disk'},
                       {u'site_name': u'RAL', u'type': u'phedex', u'alias': u'T1_UK_RAL_Buffer'},
                       {u'site_name': u'RAL', u'type': u'phedex', u'alias': u'T1_UK_RAL_MSS'},
                       {u'site_name': u'Nebraska', u'type': u'cms', u'alias': u'T2_US_Nebraska'},
                       {u'site_name': u'Nebraska', u'type': u'phedex', u'alias': u'T2_US_Nebraska'},
                       {u'site_name': u'T2_XX_SiteA', u'type': u'cms', u'alias': u'T2_XX_SiteA'},
                       {u'site_name': u'T2_XX_SiteA', u'type': u'phedex', u'alias': u'T2_XX_SiteA'},
                       {u'site_name': u'T2_XX_SiteB', u'type': u'cms', u'alias': u'T2_XX_SiteB'},
                       {u'site_name': u'T2_XX_SiteB', u'type': u'phedex', u'alias': u'T2_XX_SiteB'},
                       {u'site_name': u'T2_XX_SiteC', u'type': u'cms', u'alias': u'T2_XX_SiteC'},
                       {u'site_name': u'CERN Tier-2', u'type': u'cms', u'alias': u'T2_CH_CERN'},
                       {u'site_name': u'CERN AI', u'type': u'cms', u'alias': u'T2_CH_CERN_AI'},
                       {u'site_name': u'CERN Tier-0', u'type': u'cms', u'alias': u'T2_CH_CERN_T0'},
                       {u'site_name': u'CERN Tier-2 HLT', u'type': u'cms', u'alias': u'T2_CH_CERN_HLT'}]

    _siteresources_data = [# Site resources no longer returns CE data
                           #{u'type': u'CE', u'site_name': u'RAL', u'fqdn': u'lcgce10.gridpp.rl.ac.uk', u'is_primary': u'n'},
                           {u'type': u'SE', u'site_name': u'RAL', u'fqdn': u'srm-cms.gridpp.rl.ac.uk', u'is_primary': u'n'},
                           {u'type': u'SE', u'site_name': u'RAL', u'fqdn': u'srm-cms-disk.gridpp.rl.ac.uk', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'RAL', u'fqdn': u'lcgce02.gridpp.rl.ac.uk', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'FNAL', u'fqdn': u'cmsosgce2.fnal.gov', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'FNAL', u'fqdn': u'cmsosgce4.fnal.gov', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'FNAL', u'fqdn': u'cmsosgce.fnal.gov', u'is_primary': u'n'},
                           {u'type': u'SE', u'site_name': u'FNAL', u'fqdn': u'cmssrm.fnal.gov', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'Nebraska', u'fqdn': u'red.unl.edu', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'Nebraska', u'fqdn': u'red-gw1.unl.edu', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'Nebraska', u'fqdn': u'red-gw2.unl.edu', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'Nebraska', u'fqdn': u'ff-grid.unl.edu', u'is_primary': u'n'},
                           {u'type': u'SE', u'site_name': u'Nebraska', u'fqdn': u'red-srm1.unl.edu', u'is_primary': u'n'},
                           {u'type': u'SE', u'site_name': u'Nebraska', u'fqdn': u'srm.unl.edu', u'is_primary': u'n'},
                           {u'type': u'SE', u'site_name': u'Nebraska', u'fqdn': u'dcache07.unl.edu', u'is_primary': u'n'},
                           #{u'type': u'CE', u'site_name': u'T2_XX_SiteA', u'fqdn': u'T2_XX_SiteA', u'is_primary' : u'n'},
                           #{u'type': u'CE', u'site_name': u'T2_XX_SiteB', u'fqdn': u'T2_XX_SiteB', u'is_primary' : u'n'},
                           #{u'type': u'CE', u'site_name': u'T2_XX_SiteC', u'fqdn': u'T2_XX_SiteC', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'T2_XX_SiteA', u'fqdn': u'T2_XX_SiteA', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'T2_XX_SiteB', u'fqdn': u'T2_XX_SiteB', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'T2_XX_SiteC', u'fqdn': u'T2_XX_SiteC', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'CERN Tier-2', u'fqdn': u'srm-eoscms.cern.ch', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'CERN Tier-0', u'fqdn': u'srm-eoscms.cern.ch', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'CERN AI', u'fqdn': u'srm-eoscms.cern.ch', u'is_primary' : u'n'},
                           #{u'type': u'CE', u'site_name': u'CERN Tier-2', u'fqdn': u'ce207.cern.ch', u'is_primary' : u'n'},
                           {u'type': u'SE', u'site_name': u'CERN Tier-2 HLT', u'fqdn': u'srm-eoscms.cern.ch', u'is_primary' : u'n'}]

    _dataProcessing_data = [{u'phedex_name': u'T0_CH_CERN_MSS', u'psn_name': u'T0_CH_CERN', u'site_name': u'T0_CH_CERN'},
                            {u'phedex_name': u'T0_CH_CERN_Disk', u'psn_name': u'T0_CH_CERN', u'site_name': u'T0_CH_CERN'},
                            {u'phedex_name': u'T0_CH_CERN_Export', u'psn_name': u'T0_CH_CERN', u'site_name': u'T0_CH_CERN'},
                            {u'phedex_name': u'T2_CH_CERN', u'psn_name': u'T2_CH_CERN', u'site_name': u'CERN-PROD'},
                            {u'phedex_name': u'T1_DE_KIT_Disk', u'psn_name': u'T1_DE_KIT', u'site_name': u'FZK-LCG2'},
                            {u'phedex_name': u'T3_US_Omaha', u'psn_name': u'T3_US_Omaha', u'site_name': u'Firefly'},
                            {u'phedex_name': u'T2_US_Nebraska', u'psn_name': u'T2_US_Nebraska', u'site_name': u'Nebraska'},
                            {u'phedex_name': u'T1_UK_RAL', u'psn_name': u'T1_UK_RAL', u'site_name': u'RAL'},
                            {u'phedex_name': u'T1_US_FNAL', u'psn_name': u'T1_US_FNAL', u'site_name': u'FNAL'},
                            {u'phedex_name': u'T1_UK_RAL_Disk', u'psn_name': u'T1_UK_RAL', u'site_name': u'RAL-LCG2-DISK'},
                            {u'phedex_name': u'T1_US_FNAL_Disk', u'psn_name': u'T1_US_FNAL', u'site_name': u'FNAL'},
                            {u'phedex_name': u'T1_US_FNAL_Buffer', u'psn_name': u'T1_US_FNAL', u'site_name': u'FNAL'},
                            {u'phedex_name': u'T1_US_FNAL_MSS', u'psn_name': u'T1_US_FNAL', u'site_name': u'FNAL'},
                            {u'phedex_name': u'T2_UK_London_IC', u'psn_name': u'T2_UK_London_IC', u'site_name': u'UKI-LT2-IC-HEP'},
                            {u'phedex_name': u'T2_XX_SiteA_MSS', u'psn_name': u'T2_XX_SiteA', u'site_name': u'XX_T2_XX_SiteA'},
                            {u'phedex_name': u'T2_XX_SiteB_MSS', u'psn_name': u'T2_XX_SiteB', u'site_name': u'XX_T2_XX_SiteB'},
                            {u'phedex_name': u'T2_XX_SiteC_MSS', u'psn_name': u'T2_XX_SiteC', u'site_name': u'XX_T2_XX_SiteC'},
                            {u'phedex_name': u'T2_XX_SiteA', u'psn_name': u'T2_XX_SiteA', u'site_name': u'XX_T2_XX_SiteA'},
                            {u'phedex_name': u'T2_XX_SiteAA', u'psn_name': u'T2_XX_SiteAA', u'site_name': u'XX_T2_XX_SiteA'},
                            {u'phedex_name': u'T2_XX_SiteB', u'psn_name': u'T2_XX_SiteB', u'site_name': u'XX_T2_XX_SiteB'},
                            {u'phedex_name': u'T2_XX_SiteC', u'psn_name': u'T2_XX_SiteC', u'site_name': u'XX_T2_XX_SiteC'},
                           ]

    def __init__(self, config={}):
        self['logger'] = logging.getLogger(self.__class__.__name__)
        pass

    def _people(self, username=None, clearCache=False):
        raise NotImplementedError

    def _sitenames(self, sitename=None, clearCache=False):
        raise NotImplementedError

    def _siteresources(self, clearCache=False):
        raise NotImplementedError

    def _dataProcessing(self, pnn=None, psn=None, clearCache=False):
        """
        aReturns a mapping between PNNs and PSNs.
        In case a PSN is provided, then it returns only the PNN(s) it maps to.
        In case a PNN is provided, then it returns only the PSN(s) it maps to.
        """

        raise NotImplementedError

    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        raise NotImplementedError

    def cmsNametoCE(self, cmsName):
        """
        Convert CMS name (also pattern) to list of CEs
        """
        raise NotImplementedError
        #return self.cmsNametoList(cmsName, 'CE')

    def cmsNametoSE(self, cmsName):
        """
        Convert CMS name (also pattern) to list of SEs
        """
        raise NotImplementedError

    def getAllCENames(self):
        """
        _getAllCENames_

        Get all CE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        raise NotImplementedError

    def getAllSENames(self):
        """
        _getAllSENames_

        Get all SE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        raise NotImplementedError

    def getAllCMSNames(self):
        """
        _getAllCMSNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        raise NotImplementedError

    def cmsNametoList(self, cmsname_pattern, kind):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs.
        """
        raise NotImplementedError

    def ceToCMSName(self, ce):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        raise NotImplementedError

    def seToCMSName(self, se):
        """
        Convert SE name to the CMS Site they belong to
        """
        raise NotImplementedError

    def seToPNNs(self, se):
        """
        Convert SE name to the PNN they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of pnns
        """
        raise NotImplementedError

    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        raise NotImplementedError

    def PNNtoPSN(self, pnn):
        """
        Emulator to convert PhEDEx node name to Processing Site Name(s)
        """
        raise NotImplementedError

    def PSNtoPNN(self, psn):
        """
        Emulator to convert Processing Site Name to PhEDEx Node Name(s)
        """
        raise NotImplementedError

    def PNNstoPSNs(self, pnns):
        """
        Emulator to convert list of PhEDEx node names to Processing Site Name(s)
        """
        raise NotImplementedError

    def PSNstoPNNs(self, psns):
        """
        Emulator to convert list of Processing Site Names to PhEDEx Node Names
        """
        raise NotImplementedError

    def PSNtoPNNMap(self, psn_pattern=''):
        """
        PSN to PNN map
        """
        raise NotImplementedError

