#!/usr/bin/env python
"""
RequestQuery

Creates a JSON file with the information for a StoreResults
Request.

"""
import os, re, traceback
from dbs.apis.dbsClient import DbsApi
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from mechanize import Browser
from bs4 import BeautifulSoup

try:
    import json
except:
    import simplejson as json

dbs_base_url = "https://cmsweb.cern.ch/dbs/prod/"
#dbs_base_url = "https://cmsweb-testbed.cern.ch/dbs/int/"

class RequestQuery:

    def __init__(self,config):
        self.br=Browser()

        self.config = config
        
        # Initialise connections
        self.mySiteDB = SiteDBJSON()
        self.dbsPhys01 = DbsApi(url = dbs_base_url+"phys01/DBSReader/")
        self.dbsPhys02 = DbsApi(url = dbs_base_url+"phys02/DBSReader/")
        self.dbsPhys03 = DbsApi(url = dbs_base_url+"phys03/DBSReader/")
        
    def __del__(self):
        self.br.close()

    def getScramArchByCMSSW(self):
        """
        Get from the list of available CMSSW releases
        return a dictionary of ScramArchitecture by CMSSW
        """
        
        # Set temporary conection to the server and get the response from cmstags
        url = 'https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML'
        br = Browser()
        br.set_handle_robots(False)
        response=br.open(url)
        soup = BeautifulSoup(response.read())
        
        # Dictionary form
        # {'CMSSW_X_X_X':[slc5_amd64_gcc472], ... }
        archByCmssw={}
        
        # Fill the dictionary
        for arch in soup.find_all('architecture'): 
            for cmssw in arch.find_all('project'): 
                # CMSSW release
                cmsswLabel = cmssw.get('label').encode('ascii', 'ignore')
                if cmsswLabel not in archByCmssw:
                    archByCmssw[cmsswLabel]=[]
                # ScramArch related to this CMSSW release
                archName = arch.get('name').encode('ascii', 'ignore')
                archByCmssw[cmsswLabel].append(archName)
        
        return archByCmssw
      
    def getDatasetOriginSites(self, dbs_url, data):
        """
        Get the origin sites for each block of the dataset.
        Return a list block origin sites.
        """
        
        sites=[]
        local_dbs = dbs_url.split('/')[5]
        if local_dbs == 'phys01':
            response = self.dbsPhys01.listBlocks(detail=True,dataset=data)
        elif local_dbs == 'phys02':
            response = self.dbsPhys02.listBlocks(detail=True,dataset=data)
        elif local_dbs == 'phys03':
            response = self.dbsPhys03.listBlocks(detail=True,dataset=data)
        
        seList = []
        for block in response:
            if block['origin_site_name'] not in seList:
                seList.append(block['origin_site_name'])
        
        siteNames = []
        for node in self.nodeMappings['phedex']['node']:
            if node['se'] in seList:
                siteNames.append(node['name']) 
        
        return siteNames, seList
    
    def setGlobalTagFromOrigin(self, dbs_url,input_dataset):
        """
        Get the global tag of the dataset from the source dbs url. If it is not set, then set global tag to 'UNKNOWN'
        """
        
        globalTag = ""
        local_dbs = dbs_url.split('/')[5]
        if local_dbs == 'phys01':
            response = self.dbsPhys01.listOutputConfigs(dataset=input_dataset)
        elif local_dbs == 'phys02':
            response = self.dbsPhys02.listOutputConfigs(dataset=input_dataset)
        elif local_dbs == 'phys03':
            response = self.dbsPhys03.listOutputConfigs(dataset=input_dataset)
        
        globalTag = response[0]['global_tag']
        # GlobalTag cannot be empty
        if globalTag == '':
            globalTag = 'UNKNOWN'
            
        return globalTag
    
    def isDataAtUrl(self, dbs_url,input_dataset):
        """
        Returns True if the dataset is at the dbs url, if not returns False
        """
        local_dbs = dbs_url.split('/')[5]
        if local_dbs == 'phys01':
            response = self.dbsPhys01.listDatasets(dataset=input_dataset)
        elif local_dbs == 'phys02':
            response = self.dbsPhys02.listDatasets(dataset=input_dataset)
        elif local_dbs == 'phys03':
            response = self.dbsPhys03.listDatasets(dataset=input_dataset)
        # This means that the dataset is not at the url
        if not response:
            return False
        else:
            return True
         
    def getLabelByValueDict(self, control):
        """
        From control items, create a dictionary by values
        """   
        d = {}
        for item in control.items:
            value = item.attrs['value']
            label = item.attrs['label']
            d[value] = label
                
        return d
    
    def getValueByLabelDict(self, control):
        """
        From control items, create a dictionary by labels
        """
        d = {}
        for item in control.items:
            value = item.attrs['value']
            label = item.attrs['label']
            d[label] = value

        return d
    
    def createRequestJSON(self, ticket, input_dataset, dbs_url, cmssw_release, group_name, version = 1):
        """
        Creates a JSON file 'Ticket_#TICKET.json' with the needed
        information for creating a requeston ReqMgr.
        Input:
            - ticket: the ticket #, for instance 110773 on https://ggus.eu/?mode=ticket_info&ticket_id=110773
            - input_dataset
            - dbs_url: only the instance name, For example: "phys01" for 
             https://cmsweb.cern.ch/dbs/prod/phys01/DBSReader
            - cmssw_release
            - group_name: the physics group name
            - version: the dataset version, 1 by default.
        It returns a dictionary that contains the request information.
        """

        scramArchByCMSSW = self.getScramArchByCMSSW()
        self.nodeMappings = self.phedex.getNodeMap()
        task = ticket
        print "Processing ticket: %s" % task
        
        #splitting input dataset       
        input_primary_dataset = input_dataset.split('/')[1].replace(' ','')
        input_processed_dataset = input_dataset.split('/')[2].replace(' ','')
        data_tier = input_dataset.split('/')[3].replace(' ','')
                
        # Transform input value to a valid DBS url
        #dbs_url = "https://cmsweb.cern.ch/dbs/prod/"+dbs_url+"/DBSReader"
        dbs_url = dbs_base_url+dbs_url+"/DBSReader"
        release_id = cmssw_release
                
        # check if deprecated release was used
        release = cmssw_release
        # check if release has not ScramArch match
        if release not in scramArchByCMSSW:
            raise Exception("Error on ticket %s due to ScramArch mismatch" % task)
        else:
            scram_arch = scramArchByCMSSW[release][-1]

        # check if dataset is not at dbs url
        try:
            data_at_url = self.isDataAtUrl(dbs_url,input_dataset)
        except:
            raise Exception('Error on ticket %s, dataset %s not available at %s' %(task, input_dataset,dbs_url))

        if not data_at_url:
            raise Exception('Error on ticket %s, dataset %s not available at %s' %(task, input_dataset,dbs_url))
                    
        ## Get Physics Group
        group_squad = 'cms-storeresults-'+group_name.replace("-","_").lower()

        ## Get Dataset Version
        dataset_version = str(version)

        # Set default Adquisition Era for StoreResults 
        acquisitionEra = "StoreResults"

        ## Construction of the new dataset name (ProcessingString)
        ## remove leading hypernews or physics group name and StoreResults+Version
        if input_processed_dataset.find(group_name)==0:
            new_dataset = input_processed_dataset.replace(group_name,"",1)
        else:
            stripped_dataset = input_processed_dataset.split("-")[1:]
            new_dataset = '_'.join(stripped_dataset)
                        
        # Get dataset site info:
        phedex_map, se_names = self.getDatasetOriginSites(dbs_url,input_dataset)
        sites = set([self.mySiteDB.PNNtoPSN(node) for node in phedex_map])

        infoDict = {}
        # Build store results json
        # First add all the defaults values
        infoDict["RequestType"] = "StoreResults"
        infoDict["UnmergedLFNBase"] = "/store/unmerged" 
        infoDict["MergedLFNBase"] = "/store/results/" + group_name.replace("-","_").lower()
        infoDict["MinMergeSize"] = 1500000000
        infoDict["MaxMergeSize"] = 5000000000
        infoDict["MaxMergeEvents"] = 100000
        infoDict["TimePerEvent"] = 40
        infoDict["SizePerEvent"] = 512.0
        infoDict["Memory"] = 2394
        infoDict["CmsPath"] = "/uscmst1/prod/sw/cms"                                        
        infoDict["Group"] = "DATAOPS"
        infoDict["DbsUrl"] = dbs_url
        
        # Add all the information pulled from Savannah
        infoDict["AcquisitionEra"] = acquisitionEra
        infoDict["GlobalTag"] = self.setGlobalTagFromOrigin(dbs_url, input_dataset)
        infoDict["DataTier"] = data_tier
        infoDict["InputDataset"] = input_dataset
        infoDict["ProcessingString"] = new_dataset
        infoDict["CMSSWVersion"] = release
        infoDict["ScramArch"] = scram_arch
        infoDict["ProcessingVersion"] = dataset_version                    
        infoDict["SiteWhitelist"] = list(sites)
        
        # Create report for Migration2Global
        report = {}
         
        #Fill json file, if status is done
        self.writeJSONFile(task, infoDict)
        report["json"] = 'y'
        report["task"] = int(task)
        report["InputDataset"] = input_dataset
        report["ProcessingString"] = new_dataset
        report["localUrl"] = dbs_url
        report["sites"] = list(sites)
        report["se_names"] = list(se_names)

        return report

    def writeJSONFile(self, task, infoDict):
        """
        This writes a JSON file at ComponentDir
        """
        ##check if file already exists
        filename = self.config["ComponentDir"]+'/Ticket_'+str(task)+'.json'
        if not os.access(filename,os.F_OK):
            jsonfile = open(filename,'w')
            request = {'createRequest':infoDict} ## CHECK THIS BEFORE FINISHING
            jsonfile.write(json.dumps(request,sort_keys=True, indent=4))
            jsonfile.close

        return

    def removeJSONFile(self,task):
        """
        This removes the JSON file at ComponentDir if it was created
        """
        filename = self.config["ComponentDir"]+'/Ticket_'+str(task)+'.json'

        if os.access(filename,os.F_OK):
            os.remove(filename)
        return

    def printReport(self, report):
        """
        Print out a report
        """
        print "%20s %5s %10s %50s %50s" %( 'Ticket','json','local DBS','Sites','se_names') 
        print "%20s %5s %10s %50s %50s" %( '-'*20,'-'*5,'-'*10,'-'*50,'-'*50 )
        
        json = report["json"]
        ticket = report["task"]
        #status = report["ticketStatus"]
        localUrl = report["localUrl"].split('/')[5]
        site = ', '.join(report["sites"])
        se_names = ', '.join(report["se_names"])
        print "%20s %5s %10s %50s %50s" %(ticket,json,localUrl,site,se_names)  

        
