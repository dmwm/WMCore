#!/usr/bin/env python
"""
_SiteDBClient_

Emulating SiteDB
"""
import re
import WMCore.Services.SiteDB.SiteDB as RealSiteDB
#TODO remove this when all DBS origin_site_name is converted to PNN
pnn_regex = re.compile(r'^T[0-3%]((_[A-Z]{2}(_[A-Za-z0-9]+)*)?)')

class SiteDBJSON(object):
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

    _dataProcessing_data = [{u'phedex_name': u'T0_CH_CERN_MSS',  u'psn_name': u'T0_CH_CERN',  u'site_name': u'T0_CH_CERN'},
                            {u'phedex_name': u'T0_CH_CERN_Disk',  u'psn_name': u'T0_CH_CERN',  u'site_name': u'T0_CH_CERN'},
                            {u'phedex_name': u'T0_CH_CERN_Export',  u'psn_name': u'T0_CH_CERN',  u'site_name': u'T0_CH_CERN'},
                            {u'phedex_name': u'T2_CH_CERN',  u'psn_name': u'T2_CH_CERN',  u'site_name': u'CERN-PROD'},
                            {u'phedex_name': u'T1_DE_KIT_Disk',  u'psn_name': u'T1_DE_KIT',  u'site_name': u'FZK-LCG2'},
                            {u'phedex_name': u'T3_US_Omaha',  u'psn_name': u'T3_US_Omaha',  u'site_name': u'Firefly'},
                            {u'phedex_name': u'T2_US_Nebraska',  u'psn_name': u'T2_US_Nebraska',  u'site_name': u'Nebraska'},
                            {u'phedex_name': u'T1_UK_RAL_Disk',  u'psn_name': u'T1_UK_RAL',  u'site_name': u'RAL-LCG2-DISK'},
                            {u'phedex_name': u'T1_US_FNAL_Disk',  u'psn_name': u'T1_US_FNAL',  u'site_name': u'FNAL'},
                            {u'phedex_name': u'T1_US_FNAL_Buffer',  u'psn_name': u'T1_US_FNAL',  u'site_name': u'FNAL'},
                            {u'phedex_name': u'T1_US_FNAL_MSS',  u'psn_name': u'T1_US_FNAL',  u'site_name': u'FNAL'},
                            {u'phedex_name': u'T2_UK_London_IC',  u'psn_name': u'T2_UK_London_IC',  u'site_name': u'UKI-LT2-IC-HEP'},
                            {u'phedex_name': u'T2_XX_SiteA_MSS',  u'psn_name': u'T2_XX_SiteA',  u'site_name': u'XX_T2_XX_SiteA'},
                            {u'phedex_name': u'T2_XX_SiteB_MSS',  u'psn_name': u'T2_XX_SiteB',  u'site_name': u'XX_T2_XX_SiteB'},
                            {u'phedex_name': u'T2_XX_SiteC_MSS',  u'psn_name': u'T2_XX_SiteC',  u'site_name': u'XX_T2_XX_SiteC'},
                            {u'phedex_name': u'T2_XX_SiteA',  u'psn_name': u'T2_XX_SiteA',  u'site_name': u'XX_T2_XX_SiteA'},
                            {u'phedex_name': u'T2_XX_SiteAA',  u'psn_name': u'T2_XX_SiteAA',  u'site_name': u'XX_T2_XX_SiteA'},
                            {u'phedex_name': u'T2_XX_SiteB',  u'psn_name': u'T2_XX_SiteB',  u'site_name': u'XX_T2_XX_SiteB'},
                            {u'phedex_name': u'T2_XX_SiteC',  u'psn_name': u'T2_XX_SiteC',  u'site_name': u'XX_T2_XX_SiteC'},
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

    def _dataProcessing(self, pnn=None, psn=None, clearCache=False):
        """
        aReturns a mapping between PNNs and PSNs.
        In case a PSN is provided, then it returns only the PNN(s) it maps to.
        In case a PNN is provided, then it returns only the PSN(s) it maps to.
        """
        mapping = self._dataProcessing_data
        if pnn:
            mapping = [item['psn_name'] for item in mapping if item['phedex_name']==pnn]
        elif psn:
            mapping = [item['phedex_name'] for item in mapping if item['psn_name']==psn]
        return mapping

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

    def cmsNametoList(self, cmsname_pattern, kind):
        """
        Convert CMS name pattern T1*, T2* to a list of CEs or SEs.
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
    
    def seToPNNs(self, se):
        """
        Convert SE name to the PNN they belong to,
        this is not a 1-to-1 relation but 1-to-many, return a list of pnns
        """
        try:
            siteresources = filter(lambda x: x['fqdn']==se, self._siteresources())
        except IndexError:
            return None
        siteNames = []
        for resource in siteresources:
            siteNames.extend(self._sitenames(sitename=resource['site_name']))
        pnns = filter(lambda x: x['type']=='phedex', siteNames)
        return [x['alias'] for x in pnns]

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
        return self._dataProcessing(pnn=pnn)

    def PSNtoPNN(self, psn):
        """
        Emulator to convert Processing Site Name to PhEDEx Node Name(s)
        """
        return self._dataProcessing(psn=psn)

    def PNNstoPSNs(self, pnns):
        """
        Emulator to convert list of PhEDEx node names to Processing Site Name(s)
        """
        psns = set()
        for pnn in pnns:
            if pnn=="T0_CH_CERN_Export" or pnn.endswith("_MSS") or pnn.endswith("_Buffer"):
                continue
            psn_list = self.PNNtoPSN(pnn)
            psns.update(psn_list)
            if not psn_list:
                self["logger"].warning("No PSNs for PNN: %s" % pnn)
        return list(psns)

    def PSNstoPNNs(self, psns):
        """
        Emulator to convert list of Processing Site Names to PhEDEx Node Names
        """
        pnns = set()
        for psn in psns:
            pnn_list = self.PSNtoPNN(psn)
            if not pnn_list:
                self["logger"].warning("No PNNs for PSN: %s" % psn)
            pnns.update(pnn_list)
        return list(pnns)

    def PSNtoPNNMap(self, psn_pattern=''):
        if not isinstance(psn_pattern, str):
            raise TypeError('psn_pattern arg must be of type str')

        mapping = {}
        psn_pattern = re.compile(psn_pattern)  # .replace('*', '.*').replace('%', '.*'))
        for entry in self._dataProcessing():
            if not psn_pattern.match(entry['psn_name']):
                continue
            mapping.setdefault(entry['psn_name'], set()).add(entry['phedex_name'])
        return mapping
    
    #TODO remove this when all DBS origin_site_name is converted to PNN
    def checkAndConvertSENameToPNN(self, seNameOrPNN):
        """
        check whether argument is sename 
        if it is convert to PNN
        if not just return argument
        """
        if isinstance(seNameOrPNN, str):
            seNameOrPNN = [seNameOrPNN]
        
        newList = []
        for se in seNameOrPNN:
            if not pnn_regex.match(se):
                newList.extend(self.seToPNNs(se))
            else:
                newList.extend(se)
        return newList
