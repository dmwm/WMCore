#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : WorkflowManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Workflow management tools
"""

# system modules
import os
import re
import json
import httplib

# ReqMgr modules
from ReqMgr.tools.reqMgrClient import WorkflowManager

# DBS modules
import dbs3Client as dbs3
from dbs.apis.dbsClient import DbsApi

# DBS3 helper functions
DBS3 = r'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
def getDatasets(dataset_pattern):
    "Return list of dataset for given dataset pattern"
    dbsapi = DbsApi(url=DBS3, verifypeer=False)
    reply = dbsapi.listDatasets(dataset=dataset_pattern, dataset_access_type='*')
    return reply

def getDatasetStatus(dataset):
    "Return dataset status"
    dbsapi = DbsApi(url=DBS3, verifypeer=False)
    reply = dbsapi.listDatasets(dataset=dataset, dataset_access_type='*', detail=True)
    return reply[0]['dataset_access_type']

def getWorkload(url, workflow):
    "Return workload list"
    conn = httplib.HTTPSConnection(url,
            cert_file = os.getenv('X509_USER_PROXY'),
            key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/reqmgr/view/showWorkload?requestName='+workflow)
    r2=conn.getresponse()
    workload=r2.read()
    return workload.split('\n')

class WorkflowDataOpsMgr(WorkflowManager):
    def __init__(self, workflow, **kwds):
        """
        Extend WorkflowManager and assign data-ops attributes for
        given workflow. The order of calls does matter !!!
        """
        self.kwds = kwds
        self.url = self.get('url', 'cmsweb.cern.ch')
        WorkflowManager.__init__(self, workflow, self.url)
        self.workload = getWorkload(self.url, workflow)
        self.cacheID = self.winfo.get('StepOneConfigCacheID', '')
        self.config = getConfig(self.url, self.cacheID)
        self.pileup_dataset = self._pileup_dataset()
        self.priority = self._priority()
        self.era = self.get('era', 'Summer12')
        self.lfn = self.get('lfn', '/store/mc')
        self.special_name = self.get('specialName', '')
        self.max_rss = self.get('maxRSS', 2300000)
        self.max_vsize = self.get('maxVSize', 4100000000)
        self.input_dataset = ''
        self.pileup_scenario = ''
        self.global_tag = self.get('globalTag', '')
        self.campaign = self.get('campaign', '')
        self.max_merge_events = self.get('maxMergeEvents', 50000)
        self.activity = self.get('activity', 'reprocessing')
        self.restrict = self.get('restrict', 'None')
        self.site_use = self.get('site', None)
        self.site_cust = self.get('site_cust', None)
        self.xrootd = self.get('xrootd', 0)
        self.ext_tag = self.get('ext', '')
        self.team = self.get('team', '')

        # perform various initialization
        self._init()

        # custom settings
        # Construct processed dataset version
        if self.pileup_scenario:
            self.pileup_scenario = self.pileup_scenario+'_' 

        specialprocstring = kwds.get('specialName', '')
        if  specialprocstring:
            self.special_name = specialprocstring + '_'

        # ProcessingString
        inprocstring = kwds.get('procstring', '')
        if  inprocstring:
            self.procstring = inprocstring
        else:
            self.procstring = self.special_name + self.pileup_scenario +\
                    self.global_tag + self.ext_tag

        # ProcessingVersion
        inprocversion = kwds.get('procversion', '')
        if  inprocversion:
            self.procversion = inprocversion
        else:
            self.procversion = self.dataset_version(self.era, self.procstring)

    def dataset_version(self, era, partialProcVersion):
        versionNum = 1
        outputs = self.output_datasets
        for output in outputs:
           bits = output.split('/')
           outputCheck = '/'+bits[1]+'/'+era+'-'+partialProcVersion+'*/'+bits[len(bits)-1]

           datasets = getDatasets(outputCheck)
           for dataset in datasets:
              datasetName = dataset['dataset']
              matchObj = re.match(r".*-v(\d+)/.*", datasetName)
              if matchObj:
                 currentVersionNum = int(matchObj.group(1))
                 if versionNum <= currentVersionNum:
                    versionNum=versionNum+1

        return versionNum

    ### private methods
    def _init(self):
        "Perform initialization and cross-checks"
        self.input_dataset = self._input_dataset()
        self.global_tag = self._global_tag()
        self.ext_tag = self._ext_tag()
        self.campaign = self._campaign()
        self.era, self.lfn, self.special_name = self._era_lfn_name()
        self.pileup_scenario = self._pileup_scenario()
        self.max_rss = self._max_rss()
        self.max_merge_events = self._max_merge_events()
        self.team = self._team()
        self.site_use, self.site_cust = self._sites()

        # Checks attributes
        checklist = [(self.era, ''), (self.lfn, ''), (self.pileup_scenario, 'Unknown')]
        for att, val in checklist:
            if  att == val:
                raise Exception('ERROR: %s == "%s"' % (att, val))

        # Check status of input dataset
        inputDatasetStatus = getDatasetStatus(self.input_dataset)
        if inputDatasetStatus != 'VALID' and inputDatasetStatus != 'PRODUCTION':
            raise Exception('ERROR: Input dataset is not PRODUCTION or VALID, status=%s' % inputDatasetStatus)

    def get(self, key, default=''):
        "Get extension tag"
        val = self.kwds.get(key)
        if  not val:
            val = default
        return val

    def _ext_tag(self):
        "Get extension tag"
        if  self.ext_tag:
            ext_tag = '_ext' + self.ext_tag
        else:
            ext_tag = ''
        return ext_tag

    def _global_tag(self):
        "Extract required part of global tag from workflow info"
        return self.winfo.get('GlobalTag', '').split('::')[0]

    def _campaign(self):
        "Return campaign from workflow info"
        return self.winfo.get('Campaign', '')

    def _max_rss(self):
        "Return maxRSS"
        max_rss = self.max_rss
        if ('HiFall11' in self.workflow or 'HiFall13DR53X' in self.workflow) and \
                'IN2P3' in self.site_use:
            max_rss = 4000000
        return max_rss

    def _max_merge_events(self):
        "Return max number of merge events"
        if 'DR61SLHCx' in self.workflow:
            return 5000
        return self.max_merge_events

    def _input_dataset(self):
        "Return input dataset of workflow"
        dataset = self.winfo.get('InputDataset', '')
        if  not dataset:
            raise Exception("Error: no input dataset found for %s" % self.workflow)
        return dataset

    def _era_lfn_name(self):
        """
        Return era/lfn/name for given workflow, so far we have hard-coded cases,
        later it should be stored persistently and we should have APIs: get/put
        to fetch/store/update this info in DB.
        """
        workflow = self.workflow
        campaign = self.campaign
        era = 'Summer12'
        lfn = '/store/mc'
        specialName = ''

        # Set era, lfn and campaign-dependent part of name if necessary
        if 'Summer12_DR51X' in workflow:
            era = 'Summer12'
            lfn = '/store/mc'

        if 'Summer12_DR52X' in workflow:
            era = 'Summer12'
            lfn = '/store/mc'

        if 'Summer12_DR53X' in workflow or ('Summer12' in workflow and 'DR53X' in workflow):
            era = 'Summer12_DR53X'
            lfn = '/store/mc'

        #this is incorrect for HiFall11 workflows, but is changed further down
        if 'Fall11_R' in workflow or 'Fall11R' in workflow:
            era = 'Fall11'
            lfn = '/store/mc'

        if 'Summer13dr53X' in workflow:
            era = 'Summer13dr53X'
            lfn = '/store/mc'

        if 'Summer11dr53X' in workflow:
            era = 'Summer11dr53X'
            lfn = '/store/mc'

        if 'Fall11_HLTMuonia' in workflow:
            era = 'Fall11'
            lfn = '/store/mc'
            specialName = 'HLTMuonia_'

        if 'Summer11_R' in workflow:
            era = 'Summer11'
            lfn = '/store/mc'

        if 'LowPU2010_DR42' in workflow or 'LowPU2010DR42' in workflow:
            era = 'Summer12'
            lfn = '/store/mc'
            specialName = 'LowPU2010_DR42_'

        if 'UpgradeL1TDR_DR6X' in workflow:
            era = 'Summer12'
            lfn = '/store/mc'

        if 'HiWinter13' in self.input_dataset:
            era = 'HiWinter13'
            lfn = '/store/himc'

        if 'Spring14dr' in workflow:
            era = 'Spring14dr'
            lfn = '/store/mc'
            if '_castor_' in workflow:
                specialName = 'castor_'

        if 'Winter13' in workflow and 'DR53X' in workflow:
            era = 'HiWinter13'
            lfn = '/store/himc'

        if 'Summer11LegDR' in campaign:
            era = 'Summer11LegDR'
            lfn = '/store/mc'

        if 'UpgradePhase1Age' in campaign:
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase2LB4PS_2013_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase2BE_2013_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase2LB6PS_2013_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase1Age0DES_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'
        
        if campaign == 'UpgradePhase1Age0START_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase1Age3H_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase1Age5H_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase1Age1K_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        if campaign == 'UpgradePhase1Age3K_DR61SLHCx':
            era = 'Summer13'
            lfn = '/store/mc'
            specialName = campaign + '_'

        #change back to old campaign names for UpgradePhase1
        if 'UpgradePhase1Age' in campaign and 'dr61SLHCx' in specialName:
            specialName = specialName.replace("dr61SLHCx","_DR61SLHCx")
        if 'dr61SLHCx' in specialName:
            print 'WARNING: using new campaign name format'          

        if campaign == 'HiFall11_DR44X' or campaign == 'HiFall11DR44':
            era = 'HiFall11'
            lfn = '/store/himc'
            specialName = 'HiFall11_DR44X' + '_'

        if campaign == 'HiFall13DR53X':
            era = 'HiFall13DR53X'
            lfn = '/store/himc'

        if campaign == 'UpgFall13d':
            era = campaign
            lfn = '/store/mc'

        if campaign == 'Fall13dr':
            era = campaign
            lfn = '/store/mc'
            if '_castor_tsg_' in workflow:
                specialName = 'castor_tsg_'
            elif '_castor_' in workflow:
                specialName = 'castor_'
            elif '_tsg_' in workflow:
                specialName = 'tsg_'
            elif '__' in workflow:
                specialName = ''
            else:
                print 'ERROR: unexpected special name string in workflow name'
                sys.exit(0)

        # Handle NewG4Phys
        if campaign == 'Summer12DR53X' and 'NewG4Phys' in workflow:
            specialName = 'NewG4Phys_'

        # Handle Ext30
        if campaign == 'Summer12DR53X' and 'Ext30' in workflow:
            specialName = 'Ext30_'

        # Handle BS2011
        if campaign == 'LowPU2010DR42' and 'BS2011' in workflow:
            specialName = 'LowPU2010_DR42_BS2011_'

        return era, lfn, specialName

    def _pileup_scenario(self):
        """
        Return pileup scenario name based on given workflow
        Code should be replaced with persistent store.
        """
        workflow = self.workflow
        campaign = self.campaign
        pileupDataset = self._pileup_dataset()
        if pileupDataset != 'None':
            [subscribedOurSite, subscribedOtherSite] = checkAcceptedSubscriptionRequest(self.url, pileupDataset, siteSE)
            if not subscribedOurSite:
                print 'ERROR: pileup dataset not subscribed/approved to required Disk endpoint'
                sys.exit(0)            
      
        # Determine pileup scenario
        # - Fall11_R2 & Fall11_R4 don't add pileup so extract pileup scenario from input
        pileupScenario = ''
        pileupScenario = getPileupScenario(self.winfo, self.config)
        if campaign == 'Summer12_DR53X_RD':
            pileupScenario = 'PU_RD1'
        if pileupScenario == 'Unknown' and 'MinBias' in pileupDataset and 'LowPU2010DR42' not in workflow:
            print 'ERROR: unable to determine pileup scenario'
            sys.exit(0)
        elif 'Fall11_R2' in workflow or 'Fall11_R4' in workflow or 'Fall11R2' in workflow or 'Fall11R4' in workflow:
            matchObj = re.match(r".*Fall11-(.*)_START.*", inputDataset)
            if matchObj:
                pileupScenario = matchObj.group(1)
            else:
                pileupScenario == 'Unknown'
        elif pileupScenario == 'Unknown' and 'MinBias' not in pileupDataset:
            pileupScenario = 'NoPileUp'

        if pileupScenario == 'Unknown':
            pileupScenario = ''

        if 'LowPU2010_DR42' in workflow or 'LowPU2010DR42' in workflow:
            pileupScenario = 'PU_S0'
        if 'HiWinter13' in workflow and 'DR53X' in workflow:
            pileupScenario = ''  
        if 'pAWinter13' in workflow and 'DR53X' in workflow:
            pileupScenario = 'pa' # not actually the pileup scenario of course
        if 'ppWinter13' in workflow and 'DR53X' in workflow:
            pileupScenario = 'pp' # not actually the pileup scenario of course
        return pileupScenario

    def _pileup_dataset(self):
        pileupDataset = 'None'
        for line in self.workload:
           if 'request.schema.MCPileup' in line:
              pileupDataset = line[line.find("'")+1:line.find("'",line.find("'")+1)]
        return pileupDataset


    def _priority(self): 
        priority = -1 
        for line in self.workload:
           if 'request.schema.RequestPriority' in line:
              priority = line[line.find("=")+1:line.find("<br/")]
        priority = priority.strip()
        priority = re.sub(r'\'', '', priority)
        return int(priority)

    def _team(self):
        "Return appropriate team"
        priority = self._priority()
        if self.site_use == 'HLT':
            team = 'hlt'
        elif priority < 100000:
            team = 'reproc_lowprio'
        else:
            team = 'reproc_highprio'
        return team

    def _sites(self):
        "Find appropriate site to use"
        workflow = self.workflow
        siteUse = ''
        siteCust = self.site_cust

        # Valid Tier-1 sites
        sites = ['T1_DE_KIT', 'T1_FR_CCIN2P3', 'T1_IT_CNAF', 'T1_ES_PIC',
                 'T1_TW_ASGC', 'T1_UK_RAL', 'T1_US_FNAL', 'T2_CH_CERN', 'HLT']

        if self.site_use == 'T2_US':
            siteUse = ['T2_US_Caltech', 'T2_US_Florida', 'T2_US_MIT',
                       'T2_US_Nebraska', 'T3_US_Omaha', 'T2_US_Purdue',
                       'T2_US_UCSD', 'T2_US_Vanderbilt', 'T2_US_Wisconsin']
        elif self.site_use == 'HLT':
            siteUse = ['T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CERN']
            self.team = 'hlt'
        else:
            # Determine site where workflow should be run
            count=0
            for site in sites:
                if site in workflow:
                    count=count+1
                    siteUse = site

            # Find custodial location of input dataset if workflow name contains no T1 site or multiple T1 sites
            if count==0 or count>1:
                siteUse = findCustodialLocation(self.url, self.input_dataset)
                if siteUse == 'None':
                    raise Exception('ERROR: No custodial site found for dataset=%s' % self.input_dataset)
                siteUse = siteUse[:-4]
  
        # Set the custodial location if necessary
        if not self.site_use or self.site_use != 'T2_US':
            if not self.site_cust:
                siteCust = siteUse
            else:
                siteCust = self.site_cust

        # Check if input dataset subscribed to disk endpoint
        if 'T2_CH_CERN' in siteUse:
            siteSE = 'T2_CH_CERN'
        else:
            siteSE = siteUse + '_Disk'
        subscribedOurSite, subscribedOtherSite = \
                checkAcceptedSubscriptionRequest(self.url, self.input_dataset, siteSE)
        if not subscribedOurSite and not self.xrootd and 'Fall11R2' not in workflow:
            raise Exception('ERROR: input dataset not subscribed/approved to required Disk endpoint')
        if self.xrootd and not subscribedOtherSite:
            raise Exception('ERROR: input dataset not subscribed/approved to any Disk endpoint')
        if siteUse not in sites and options.site != 'T2_US' and \
                siteUse != ['T2_CH_CERN_AI', 'T2_CH_CERN_HLT', 'T2_CH_CERN']:
            raise Exception('ERROR: invalid site=%s' % siteUse)

        if not siteCust:
            raise Exception('ERROR: A custodial site must be specified')

        return siteUse, siteCust

def getScenario(ps):
    pss = 'Unknown'

    if ps == 'SimGeneral.MixingModule.mix_E8TeV_AVE_16_BX_25ns_cfi':
       pss = 'PU140Bx25'
    if ps == 'SimGeneral.MixingModule.mix_2012_Summer_50ns_PoissonOOTPU_cfi':
       pss = 'PU_S10'
    if ps == 'SimGeneral.MixingModule.mix_E7TeV_Fall2011_Reprocess_50ns_PoissonOOTPU_cfi':
       pss = 'PU_S6'
    if ps == 'SimGeneral.MixingModule.mix_E8TeV_AVE_10_BX_25ns_300ns_spread_cfi':
       pss = 'PU10bx25'
    if ps == 'SimGeneral.MixingModule.mix_E8TeV_AVE_10_BX_50ns_300ns_spread_cfi':
       pss = 'PU10bx50'
    if ps == 'SimGeneral.MixingModule.mix_2011_FinalDist_OOTPU_cfi':
       pss = 'PU_S13'   
    if ps == 'SimGeneral.MixingModule.mix_fromDB_cfi':
       pss = 'PU_RD1'
    if ps == 'SimGeneral.MixingModule.mix_2012C_Profile_PoissonOOTPU_cfi':
       pss = 'PU2012CExt'
    if ps == 'SimGeneral.MixingModule.mixNoPU_cfi':
       pss = 'NoPileUp'
    if ps == 'SimGeneral.MixingModule.mix_POISSON_average_cfi':
       pss = 'PU'
    if ps == 'SimGeneral.MixingModule.mix_CSA14_50ns_PoissonOOTPU_cfi':
       pss = 'PU_S14'

    return pss

def getPileupScenario(winfo, config):
    "Get pileup scanario for given workflow dict and configuration"
    workflow = winfo['RequestName']
    pileup, meanPileUp, bunchSpacing, cmdLineOptions = getPileup(config)
    scenario = getScenario(pileup)
    if scenario == 'PU140Bx25' and meanPileUp != 'Unknown':
       scenario = 'PU' + meanPileUp + 'bx25'
    if scenario == 'PU140bx25' and 'Upgrade' in workflow:
       scenario = 'PU140Bx25'
    if scenario == 'PU':
       scenario = 'PU' + meanPileUp + 'bx' + bunchSpacing
       if meanPileUp == 'None' or bunchSpacing == 'None':
          print 'ERROR: unexpected pileup settings in config'
          sys.exit(0)
    if scenario == 'PU_RD1' and cmdLineOptions != 'None':
       if '--runsAndWeightsForMC [(190482,0.924) , (194270,4.811), (200466,7.21), (207214,7.631)]' in cmdLineOptions:
          scenario = 'PU_RD2'
    return scenario

def getPileup(config):
    "Helper function used in getPileupScenario"
    pu = 'Unknown'
    vmeanpu = 'None'
    bx = 'None'
    cmdLineOptions = 'None'
    lines = config.split('\n')
    for line in lines:
       if 'process.load' and 'MixingModule' in line:
          pu = line[line.find("'")+1:line.find("'",line.find("'")+1)]
       if 'process.mix.input.nbPileupEvents.averageNumber' in line:
          meanpu = line[line.find("(")+1:line.find(")")].split('.', 1)
          vmeanpu = meanpu[0]
       if 'process.mix.bunchspace' in line:
          bx = line[line.find("(")+1:line.find(")")]
       if 'with command line options' in line:
          cmdLineOptions = line
    return pu, vmeanpu, bx, cmdLineOptions

def getConfig(url, cacheID):
    "Helper function to get configuration for given cacheID"
    conn = httplib.HTTPSConnection(url, 
            cert_file = os.getenv('X509_USER_PROXY'),
            key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",'/couchdb/reqmgr_config_cache/'+cacheID+'/configFile')
    config = conn.getresponse().read()
    return config

def findCustodialLocation(url, dataset):
    "Helper function to find custodial location for given dataset"
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/blockreplicas?dataset='+dataset)
    r2=conn.getresponse()
    result = json.loads(r2.read())
    request=result['phedex']
    if 'block' not in request.keys():
            return "No Site"
    if len(request['block'])==0:
            return "No Site"
    for replica in request['block'][0]['replica']:
            if replica['custodial']=="y" and replica['node']!="T0_CH_CERN_MSS":
                    return replica['node']
    return "None"

def checkAcceptedSubscriptionRequest(url, dataset, site):
    "Helper function"
    conn = httplib.HTTPSConnection(url,
            cert_file = os.getenv('X509_USER_PROXY'),
            key_file = os.getenv('X509_USER_PROXY'))
    conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset+'&type=xfer')
    resp = conn.getresponse()
    result = json.load(resp)
    requests=result['phedex']
    if 'request' not in requests.keys():
        return [False, False]
    ourNode = False
    otherNode = False
    for request in result['phedex']['request']:
        for node in request['node']:
            if node['name']==site and node['decision']=='approved':
                ourNode = True
            elif 'Disk' in node['name'] and node['decision']=='approved':
                otherNode = True
    return ourNode, otherNode

