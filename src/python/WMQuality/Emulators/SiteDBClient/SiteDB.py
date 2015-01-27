#!/usr/bin/env python
"""
_SiteDBClient_

Emulating SiteDB
"""
import re

class SiteDBJSON(object):
    """
    API for dealing with retrieving information from SiteDB
    """
    _people_data = [{'dn' : '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gutsche/CN=582680/CN=Oliver Gutsche',
                     'username' : 'gutsche'},
                    {'dn' : "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'",
                     'username' : 'liviof'}]

    _sitenames_data = [{u'site_name': u'FNAL', u'type': u'cms', u'alias': u'T1_US_FNAL'},
                       {u'site_name': u'FNAL', u'type': u'phedex', u'alias': u'T1_US_FNAL_Buffer'},
                       {u'site_name': u'FNAL', u'type': u'phedex', u'alias': u'T1_US_FNAL_MSS'},
                       {u'site_name': u'RAL', u'type': u'cms', u'alias': u'T1_UK_RAL'},
                       {u'site_name': u'Nebraska', u'type': u'cms', u'alias': u'T2_US_Nebraska'},
                       {u'site_name': u'T2_XX_SiteA', u'type': u'cms', u'alias': u'T2_XX_SiteA'},
                       {u'site_name': u'T2_XX_SiteB', u'type': u'cms', u'alias': u'T2_XX_SiteB'},
                       {u'site_name': u'T2_XX_SiteC', u'type': u'cms', u'alias': u'T2_XX_SiteC'},
                       {u'site_name': u'CERN Tier-2', u'type': u'cms', u'alias': u'T2_CH_CERN'},
                       {u'site_name': u'CERN AI', u'type': u'cms', u'alias': u'T2_CH_CERN_AI'},
                       {u'site_name': u'CERN Tier-0', u'type': u'cms', u'alias': u'T2_CH_CERN_T0'},
                       {u'site_name': u'CERN Tier-2 HLT', u'type': u'cms', u'alias': u'T2_CH_CERN_HLT'}]

    _siteresources_data = [
                           # Site resources no longer returns CE data
                           #{u'type': u'CE', u'site_name': u'RAL', u'fqdn': u'lcgce11.gridpp.rl.ac.uk', u'is_primary': u'n'},
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

    _dataProcessing_data = [{u'phedex_name': u'T2_CH_CERN',  u'psn_name': u'T2_CH_CERN',  u'site_name': u'CERN-PROD'},
                            {u'phedex_name': u'T1_DE_KIT_Disk',  u'psn_name': u'T1_DE_KIT',  u'site_name': u'FZK-LCG2'},
                            {u'phedex_name': u'T3_US_Omaha',  u'psn_name': u'T3_US_Omaha',  u'site_name': u'Firefly'},
                            {u'phedex_name': u'T2_US_Nebraska',  u'psn_name': u'T2_US_Nebraska',  u'site_name': u'Nebraska'},
                            {u'phedex_name': u'T1_UK_RAL_Disk',  u'psn_name': u'T1_UK_RAL',  u'site_name': u'RAL-LCG2-DISK'},
                            {u'phedex_name': u'T1_US_FNAL_Disk',  u'psn_name': u'T1_US_FNAL',  u'site_name': u'USCMS-FNAL-WC1-DISK'},
                            {u'phedex_name': u'T2_UK_London_IC',  u'psn_name': u'T2_UK_London_IC',  u'site_name': u'UKI-LT2-IC-HEP'},
                           ]

    def __init__(self, config={}):
        pass

    def _people(self, username=None, clearCache=False):
        if username:
            return filter(lambda x: x['username']==username, self._people_data)
        else:
            return self._people_data

    def _sitenames(self, sitename=None, clearCache=False):
        if sitename:
            return filter(lambda x: x['site_name']==sitename, self._sitenames_data)
        else:
            return self._sitenames_data

    def _siteresources(self, clearCache=False):
        return self._siteresources_data

    def _dataProcessing(self, clearCache=False):
        return self._dataProcessing_data

    def dnUserName(self, dn):
        """
        Convert DN to Hypernews name. Clear cache between trys
        in case user just registered or fixed an issue with SiteDB
        """
        try:
            userinfo = filter(lambda x: x['dn']==dn, self._people())[0]
            username = userinfo['username']
        except (KeyError, IndexError):
            userinfo = filter(lambda x: x['dn']==dn, self._people(clearCache=True))[0]
            username = userinfo['username']
        return username

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
        return self.cmsNametoList(cmsName, 'SE')

    def getAllCENames(self):
        """
        _getAllCENames_

        Get all CE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        ceList = filter(lambda x: x['type']=='CE', siteresources)
        ceList = map(lambda x: x['fqdn'], ceList)
        return ceList

    def getAllSENames(self):
        """
        _getAllSENames_

        Get all SE names from SiteDB
        This is so that we can easily add them to ResourceControl
        """
        siteresources = self._siteresources()
        seList = filter(lambda x: x['type']=='SE', siteresources)
        seList = map(lambda x: x['fqdn'], seList)
        return seList

    def getAllCMSNames(self):
        """
        _getAllCMSNames_

        Get all the CMSNames from siteDB
        This will allow us to add them in resourceControl at once
        """
        sitenames = self._sitenames()
        cmsnames = filter(lambda x: x['type']=='cms', sitenames)
        cmsnames = map(lambda x: x['alias'], cmsnames)
        return cmsnames

    def cmsNametoList(self, cmsname_pattern, kind, file=None):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs. The file is
        for backward compatibility with SiteDBv1
        """
        cmsname_pattern = cmsname_pattern.replace('*','.*')
        cmsname_pattern = cmsname_pattern.replace('%','.*')
        cmsname_pattern = re.compile(cmsname_pattern)

        sitenames = filter(lambda x: x['type']=='cms' and cmsname_pattern.match(x['alias']),
                           self._sitenames())
        sitenames = set(map(lambda x: x['site_name'], sitenames))
        siteresources = filter(lambda x: x['site_name'] in sitenames, self._siteresources())
        hostlist = filter(lambda x: x['type']==kind, siteresources)
        hostlist = map(lambda x: x['fqdn'], hostlist)

        return hostlist

    def ceToCMSName(self, ce):
        """
        Convert SE name to the CMS Site they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of cms site alias
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==ce, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = filter(lambda x: x['type']=='cms', siteNames)
        return [x['alias'] for x in cmsname]

    def seToCMSName(self, se):
        """
        Convert SE name to the CMS Site they belong to
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==se, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        cmsname = filter(lambda x: x['type']=='cms', siteNames)
        return [x['alias'] for x in cmsname]

    def cmsNametoPhEDExNode(self, cmsName):
        """
        Convert CMS name to list of Phedex Nodes
        """
        sitenames = self._sitenames()
        try:
            sitename = filter(lambda x: x['type']=='cms' and x['alias']==cmsName, sitenames)[0]['site_name']
        except IndexError:
            return None
        phedexnames = filter(lambda x: x['type']=='phedex' and x['site_name']==sitename, sitenames)
        phedexnames = map(lambda x: x['alias'], phedexnames)
        return phedexnames

    def PNNtoPSN(self, pnn):
        """
        Emulator to convert PhEDEx node name to Processing Site Name(s)
        """

        psnMap = self._dataProcessing()
        try:
            reducedMap = filter(lambda x: x[u'phedex_name']==pnn, psnMap)
            psns = [x[u'psn_name'] for x in reducedMap]
        except IndexError:
            return None
        return psns
