"""
UnifiedSiteInfo module holds helper function to obtain site information.

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
Original code: https://github.com/CMSCompOps/WmAgentScripts/Unified
"""

# futures
from __future__ import division

# syste modules
import json
import tempfile
from collections import defaultdict
try:
    from future.utils import with_metaclass
except Exception as _:
    # copy from Python 2.7.14 future/utils/__init__.py
    def with_metaclass(meta, *bases):
        "Helper function to provide metaclass for singleton definition"
        class metaclass(meta):
            "Metaclass for singleton definition"
            __call__ = type.__call__
            __init__ = type.__init__
            def __new__(cls, name, this_bases, d):
                "Return new class instance"
                if this_bases is None:
                    return type.__new__(cls, name, (), d)
                return meta(name, bases, d)
        return metaclass('temporary_class', None, {})

# WMCore modules
from Utils.Patterns import Singleton
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata, cern_sso_cookie
from WMCore.MicroService.Unified.Common import cert, ckey, getMSLogger

def getNodeQueues():
    "Helper function to fetch nodes usage from PhEDEx data service"
    headers = {'Accept': 'application/json'}
    params = {}
    mgr = RequestHandler()
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/nodeusagehistory'
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    data = json.loads(res)
    ret = defaultdict(int)
    for node in data['phedex']['node']:
        for usage in node['usage']:
            ret[node['name']] += int(usage['miss_bytes'] / 1023.**4) #in TB
    return ret


class SiteCache(with_metaclass(Singleton, object)):
    "Return site info from various CMS data-sources"
    def __init__(self, mode=None, logger=None):
        self.logger = getMSLogger(verbose=True, logger=logger)
        if mode == 'test':
            self.siteInfo = {}
        else:
            self.siteInfo = self.fetch()

    def fetch(self):
        "Fetch information about sites from various CMS data-services"
        tfile = tempfile.NamedTemporaryFile()
        dashboardUrl = "http://dashb-ssb.cern.ch/dashboard/request.py"
        urls = [
            '%s/getplotdata?columnid=106&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=107&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=108&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=109&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=136&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=158&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=159&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=160&batch=1&lastdata=1' % dashboardUrl,
            '%s/getplotdata?columnid=237&batch=1&lastdata=1' % dashboardUrl,
            ### FIXME: these calls to gwmsmon are failing pretty badly with
            ### "302 Found" and failing to decode, causing a huge error dump
            ### to the logs
            # 'https://cms-gwmsmon.cern.ch/totalview/json/site_summary',
            # 'https://cms-gwmsmon.cern.ch/prodview/json/site_summary',
            # 'https://cms-gwmsmon.cern.ch/poolview/json/totals',
            # 'https://cms-gwmsmon.cern.ch/prodview/json/maxusedcpus',
            'http://cmsgwms-frontend-global.cern.ch/vofrontend/stage/mcore_siteinfo.json',
            'http://t3serv001.mit.edu/~cmsprod/IntelROCCS/Detox/SitesInfo.txt',
            'http://cmsmonitoring.web.cern.ch/cmsmonitoring/storageoverview/latest/StorageOverview.json',
        ]
        cookie = {}
        ssbids = ['106', '107', '108', '109', '136', '158', '159', '160', '237']
        sids = ['1', '2', 'm1', 'm3', 'm4', 'm5', 'm6']
        for url in urls:
            if 'gwmsmon' in url:
                cern_sso_cookie(url, tfile.name, cert(), ckey())
                cookie.update({url: tfile.name})
        gen = multi_getdata(urls, ckey(), cert(), cookie=cookie)
        siteInfo = {}
        for row in gen:
            if 'Detox' in row['url']:
                data = row['data']
            else:
                try:
                    data = json.loads(row['data'])
                except Exception as exc:
                    self.logger.exception('error %s for row %s', str(exc), row)
                    data = {}
            if 'ssb' in row['url']:
                for ssbid in ssbids:
                    if ssbid in row['url']:
                        siteInfo['ssb_%s' % ssbid] = data
            elif 'prodview/json/site_summary' in row['url']:
                siteInfo['gwmsmon_prod_site_summary'] = data
            elif 'totalview/json/site_summary' in row['url']:
                siteInfo['gwmsmon_site_summary'] = data
            elif 'totals' in row['url']:
                siteInfo['gwmsmon_totals'] = data
            elif 'maxusedcpus' in row['url']:
                siteInfo['gwmsmon_prod_maxused'] = data
            elif 'mcore' in row['url']:
                siteInfo['mcore'] = data
            elif 'Detox' in row['url']:
                siteInfo['detox_sites'] = data
            elif 'monitoring' in row['url']:
                siteInfo['mss_usage'] = data
            elif 'stuck' in row['url']:
                for sid in sids:
                    if sid in row['url']:
                        siteInfo['stuck_%s' % sid] = data
            siteInfo['site_queues'] = getNodeQueues()
        return siteInfo

    def get(self, resource, default=None):
        "Return data about given resource"
        return self.siteInfo.get(resource, default)

def agentsSites(url):
    "Return list of sites known in CMS WMAgents"
    sites_ready_in_agent = set()
    headers = {'Accept': 'application/json'}
    params = {}
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    data = json.loads(res)
    agents = {}
    for r in [i['value'] for i in data['rows']]:
        team = r['agent_team']
        if team != 'production':
            continue
        agents.setdefault(team, []).append(r)
    for team, agents in agents.items():
        for agent in agents:
            if agent['status'] != 'ok':
                continue
            for site, sinfo in agent['WMBS_INFO']['thresholds'].iteritems():
                if sinfo['state'] in ['Normal']:
                    sites_ready_in_agent.add(site)
    return sites_ready_in_agent

def getNodes(kind):
    "Get list of PhEDEx nodes"
    params = {}
    headers = {'Accept': 'application/json'}
    url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes'
    mgr = RequestHandler()
    data = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    nodes = json.loads(data)['phedex']['node']
    return [node['name'] for node in nodes if node['kind'] == kind]


class SiteInfo(with_metaclass(Singleton, object)):
    "SiteInfo class provides info about sites"
    def __init__(self, uConfig, mode=None, logger=None):
        self.logger = getMSLogger(verbose=True, logger=logger)
        self.siteCache = SiteCache(mode, logger)
        self.config = uConfig

        self.sites_ready_in_agent = self.siteCache.get('ready_in_agent', [])

        self.sites_ready = []
        self.sites_not_ready = []
        self.all_sites = []
        self.sites_banned = self.config.get('sites_banned', [])

#         try:
#             sites_full = json.loads(open('sites_full.json').read())
#         except:
#             pass

        data = self.siteCache.get('ssb_237', {'csvdata': []})
        for siteInfo in data['csvdata']:
            if not siteInfo['Tier'] in [0, 1, 2, 3]:
                continue
            self.all_sites.append(siteInfo['VOName'])
            if siteInfo['VOName'] in self.sites_banned:
                continue
            if self.sites_ready_in_agent and siteInfo['VOName'] in self.sites_ready_in_agent:
                self.sites_ready.append(siteInfo['VOName'])
            elif self.sites_ready_in_agent and not siteInfo['VOName'] in self.sites_ready_in_agent:
                self.sites_not_ready.append(siteInfo['VOName'])
            elif siteInfo['Status'] == 'enabled':
                self.sites_ready.append(siteInfo['VOName'])
            else:
                self.sites_not_ready.append(siteInfo['VOName'])

        self.sites_auto_approve = self.config.get('sites_auto_approve')

        self.sites_eos = [s for s in self.sites_ready \
                if s in ['T2_CH_CERN', 'T2_CH_CERN_HLT']]
        self.sites_T3s = [s for s in self.sites_ready if s.startswith('T3_')]
        self.sites_T2s = [s for s in self.sites_ready if s.startswith('T2_')]
        self.sites_T1s = [s for s in self.sites_ready if s.startswith('T1_')]
        self.sites_T0s = [s for s in self.sites_ready if s.startswith('T0_')]

        self.sites_T3s_all = [s for s in self.all_sites if s.startswith('T3_')]
        self.sites_T2s_all = [s for s in self.all_sites if s.startswith('T2_')]
        self.sites_T1s_all = [s for s in self.all_sites if s.startswith('T1_')]
        self.sites_T0s_all = [s for s in self.all_sites if s.startswith('T0_')]

        self.sites_AAA = list(set(self.sites_ready) - set(['T2_CH_CERN_HLT']))
        ## could this be an SSB metric ?
        self.sites_with_goodIO = self.config.get('sites_with_goodIO', [])
        #restrict to those that are actually ON
        self.sites_with_goodIO = [s for s in self.sites_with_goodIO if s in self.sites_ready]
        self.sites_veto_transfer = []  ## do not prevent any transfer by default

        ## new site lists for better matching
        self.sites_with_goodAAA = self.sites_with_goodIO \
                + ['T3_IN_TIFRCloud', 'T3_US_NERSC'] ## like this for now
        self.sites_with_goodAAA = [s for s in self.sites_with_goodAAA if s in self.sites_ready]

        self.storage = defaultdict(int)
        self.disk = defaultdict(int)
        self.queue = defaultdict(int)
        self.free_disk = defaultdict(int)
        self.quota = defaultdict(int)
        self.locked = defaultdict(int)
        self.cpu_pledges = defaultdict(int)
        self.addHocStorage = {
            'T2_CH_CERN_T0': 'T2_CH_CERN',
            'T2_CH_CERN_HLT' : 'T2_CH_CERN',
            'T2_CH_CERN_AI' : 'T2_CH_CERN',
            'T3_IN_TIFRCloud' : 'T2_IN_TIFR',
            #'T3_US_NERSC' : 'T1_US_FNAL_Disk'
            }
        ## list here the site which can accomodate high memory requests
        self.sites_memory = {}

        self.sites_mcore_ready = []
        mcore_mask = self.siteCache.get('mcore')
        if mcore_mask:
            self.sites_mcore_ready = \
                    [s for s in mcore_mask['sites_for_mcore'] if s in self.sites_ready]
        else:
            pass

        for sname in self.all_sites:
            self.cpu_pledges[sname] = 1 # will get it later from SSB
            self.disk[self.ce2SE(sname)] = 0 # will get it later from SSB

        tapes = getNodes('MSS')
        for mss in tapes:
            if mss in self.sites_banned:
                continue # not using these tapes for MC familly
            self.storage[mss] = 0

        ## and get SSB sync
        self.fetch_ssb_info()

        mss_usage = self.siteCache.get('mss_usage')
        sites_space_override = self.config.get('sites_space_override', {})
        if mss_usage:
            use_field = 'Usable'
            for mss in self.storage:
                if not mss in mss_usage['Tape'][use_field]:
                    self.storage[mss] = 0
                else:
                    self.storage[mss] = mss_usage['Tape'][use_field][mss]
                if mss in sites_space_override:
                    self.storage[mss] = sites_space_override[mss]

        self.fetch_queue_info()
        ## and detox info
        self.fetch_detox_info(\
                buffer_level=self.config.get('DDM_buffer_level', None),\
                sites_space_override=sites_space_override)

        ## transform no disks in veto transfer
        for dse, free in self.disk.items():
            if free <= 0:
                if not dse in self.sites_veto_transfer:
                    self.sites_veto_transfer.append(dse)

        ## and glidein info
        self.fetch_glidein_info()

    def sitesByMemory(self, maxMem, maxCore=1):
        "Provides allowed list of sites for given memory thresholds"
        if not self.sites_memory:
            self.logger.debug("no memory information from glidein mon")
            return None
        allowed = set()
        for site, slots in self.sites_memory.items():
            if any([slot['MaxMemMB'] >= maxMem and \
                    slot['MaxCpus'] >= maxCore for slot in slots]):
                allowed.add(site)
        return list(allowed)

    def ce2SE(self, ce):
        "Convert CE to SE"
        if (ce.startswith('T1') or ce.startswith('T0')) and not ce.endswith('_Disk'):
            return ce+'_Disk'
        if ce in self.addHocStorage:
            return self.addHocStorage[ce]
        return ce

    def se2CE(self, se):
        "Convert SE to CE"
        if se.endswith('_Disk'):
            return se.replace('_Disk', '')
        elif se.endswith('_MSS'):
            return se.replace('_MSS', '')
        return se

    def fetch_queue_info(self):
        "Fetch queue inforation"
        self.queue = self.siteCache.get('site_queues')

    def fetch_glidein_info(self):
        "Fetch Glidein information"
        self.sites_memory = self.siteCache.get('gwmsmon_totals', {})
        for site in self.sites_memory.keys():
            if not site in self.sites_ready:
                self.sites_memory.pop(site)
        
        for_better_max_running = self.siteCache.get('gwmsmon_prod_maxused')
        if for_better_max_running:
            for site in self.cpu_pledges:
                new_max = self.cpu_pledges[site]
                if site in for_better_max_running:
                    new_max = int(for_better_max_running[site]['sixdays'])

                if new_max:
                    self.cpu_pledges[site] = new_max
        
        for_site_pressure = self.siteCache.get('gwmsmon_prod_site_summary')
        if for_site_pressure:
            self.sites_pressure = {}
            for site in self.cpu_pledges:
                pressure = 0
                cpusPending = 0
                cpusInUse = 0
                if site in for_site_pressure:
                    cpusPending = for_site_pressure[site]['CpusPending']
                    cpusInUse = for_site_pressure[site]['CpusInUse']
                    if cpusInUse:
                        pressure = cpusPending/float(cpusInUse)
                    else:
                        pressure = -1
                        self.sites_pressure[site] = (cpusPending, cpusInUse, pressure)

    def fetch_detox_info(self, buffer_level=0.8, sites_space_override=None):
        "Fetch Detox information"
        info = self.siteCache.get('detox_sites')
        if not info:
            return
        if len(info) < 15:
            info = self.siteCache.get('detox_sites')
            if len(info) < 15:
                self.logger.debug("detox info is gone")
                return
        read = False
        for line in info:
            if 'Partition:' in line:
                read = ('DataOps' in line)
                continue
            if line.startswith('#'):
                continue
            if not read:
                continue
            try:
                _, quota, _, locked, site = line.split()
            except Exception:
                break

            if 'MSS' in site:
                continue
#             queued = self.queue.get(site, 0)
            queued_used = 0
            available = int(float(quota)*buffer_level) - int(locked) - int(queued_used)

            #### .disk = 80%*quota - locked : so it's the effective space
            #### .free_disk = the buffer space that there is above the 80% quota
            self.disk[site] = available if available > 0 else 0
            ddm_free = int(float(quota) - int(locked) - self.disk[site])
            self.free_disk[site] = ddm_free if ddm_free > 0 else 0
            if sites_space_override and site in sites_space_override:
                self.disk[site] = sites_space_override[site]
            self.quota[site] = int(quota)
            self.locked[site] = int(locked)

    def fetch_ssb_info(self, talk=False):
        "Fetch SSB information"
        columns = {
            'PledgeTape' : 107,
            'realCPU' : 136,
            'prodCPU' : 159,
            'CPUbound' : 160,
            'FreeDisk' : 106,
            }

        _info_by_site = {}
        for name, column in columns.items():
            all_data = []
            try:
                ssb_data = self.siteCache.get('ssb_%d' % column, [])
                if ssb_data:
                    all_data = ssb_data['csvdata']
#                 all_data = self.siteCache.get('ssb_%d' % column)['csvdata']
            except Exception as exc:
                self.logger.exception('error %s', str(exc))
                continue
            if not all_data:
                self.logger.debug("cannot get info from ssb for %s", name)
            for item in all_data:
                site = item['VOName']
                if site.startswith('T3'):
                    continue
                value = item['Value']
                if not site in _info_by_site:
                    _info_by_site[site] = {}
                _info_by_site[site][name] = value

        if talk:
            self.logger.debug('document %s', json.dumps(_info_by_site, indent=2))

        if talk:
            self.logger.debug('disk keys: %s', self.disk.keys())
        for site, info in _info_by_site.items():
            if talk:
                self.logger.debug("Site: %s", site)
            ssite = self.ce2SE(site)
            key_for_cpu = 'CPUbound'
            if key_for_cpu in info and site in self.cpu_pledges and info[key_for_cpu]:
                if self.cpu_pledges[site] < info[key_for_cpu]:
                    if talk:
                        self.logger.debug('site %s could use %s instead of %s for CPU', site, info[key_for_cpu], self.cpu_pledges[site])
                    self.cpu_pledges[site] = int(info[key_for_cpu])
                elif self.cpu_pledges[site] > 1.5* info[key_for_cpu]:
                    if talk:
                        self.logger.debug('site %s could correct %s instead of %s for CPU', site, info[key_for_cpu], self.cpu_pledges[site])
                    self.cpu_pledges[site] = int(info[key_for_cpu])

            if 'FreeDisk' in info and info['FreeDisk']:
                if site in self.disk:
                    if self.disk[site] < info['FreeDisk']:
                        if talk:
                            self.logger.debug('site %s could use %s instead of %s for disk', site, info['FreeDisk'], self.disk[site])
                        self.disk[site] = int(info['FreeDisk'])
                else:
                    if not ssite in self.disk:
                        if talk:
                            self.logger.debug("setting freeDisk=%s for site=%s", info['FreeDisk'], ssite)
                        self.disk[ssite] = int(info['FreeDisk'])

            if 'FreeDisk' in info and site != ssite and info['FreeDisk']:
                if ssite in self.disk:
                    if self.disk[ssite] < info['FreeDisk']:
                        if talk:
                            self.logger.debug('site %s could use %s instead of %s for disk', ssite, info['FreeDisk'], self.disk[ssite])
                        self.disk[ssite] = int(info['FreeDisk'])
                else:
                    if talk:
                        self.logger.debug("setting %s disk for site %s", info['FreeDisk'], ssite)
                    self.disk[ssite] = int(info['FreeDisk'])
