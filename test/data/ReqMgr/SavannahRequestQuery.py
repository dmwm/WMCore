#!/usr/bin/env python
"""
RequestQuery
DEPRECATED
Provides an interface between the StoreResultsAccountant
and the Savannah Request Interface. Responsible for querying
the Request Interface, creation of the JSON steering file and
providing information for the bookeeping database

"""
from __future__ import print_function

import json
import os
import re
import traceback

from bs4 import BeautifulSoup
from dbs.apis.dbsClient import DbsApi
from mechanize import Browser

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

dbs_base_url = "https://cmsweb.cern.ch/dbs/prod/"
#dbs_base_url = "https://cmsweb-testbed.cern.ch/dbs/int/"

class RequestQuery:

    def __init__(self,config):
        self.br=Browser()

        self.config = config
        
        # Initialise connections
        self.phedex = PhEDEx({"endpoint":"https://cmsweb.cern.ch/phedex/datasvc/json/prod/"}, "json")
        self.dbsPhys01 = DbsApi(url = dbs_base_url+"phys01/DBSReader/")
        self.dbsPhys02 = DbsApi(url = dbs_base_url+"phys02/DBSReader/")
        self.dbsPhys03 = DbsApi(url = dbs_base_url+"phys03/DBSReader/")
        
    def __del__(self):
        self.br.close()

    def login2Savannah(self):
        """
        login2Savannah log into savannah with the given parameters in the config (username and password) 
        User must have admin priviledges for store results requests
        """
        login_page='https://savannah.cern.ch/account/login.php?uri=%2F'
        savannah_page='https://savannah.cern.ch/task/?group=cms-storeresults'
        
        self.br.open(login_page)

        ## 'Search' form is form 0
        ## login form is form 1
        self.br.select_form(nr=1)

        username = self.config["SavannahUser"]
    
        self.br['form_loginname']=username
        self.br['form_pw']=self.config["SavannahPasswd"]
        
        self.br.submit()
        
        response = self.br.open(savannah_page)
        
        # Check to see if login was successful
        if not re.search('Logged in as ' + username, response.read()):
            print('login unsuccessful, please check your username and password')
            return False
        else:
            return True
    
    def selectQueryForm(self,**kargs):       
        """
        selectQueryForm create the browser view to get all the store result tickets from savannah
        """
        if self.isLoggedIn:
            self.br.select_form(name="bug_form")

            ## Use right query form labelled Test
            control = self.br.find_control("report_id",type="select")

            for item in control.items:
                if item.attrs['label'] == "Test":
                    control.value = [item.attrs['value']]
                    
            ##select number of entries displayed per page
            control = self.br.find_control("chunksz",type="text")
            control.value = "150"

            ##check additional searching parameter
            for arg in kargs:
                if arg == "approval_status":
                    control = self.br.find_control("resolution_id",type="select")
                    for item in control.items:
                        if item.attrs['label'] == kargs[arg].strip():
                            control.value = [item.attrs['value']]

                elif arg == "task_status":
                    control = self.br.find_control("status_id",type="select")
                    for item in control.items:
                        if item.attrs['label'] == kargs[arg].strip():
                            control.value = [item.attrs['value']]
                            
                elif arg == "team":
                    control = self.br.find_control("custom_sb5",type="select")
                    for item in control.items:
                        if item.attrs['label'] == kargs[arg].strip():
                            control.value = [item.attrs['value']]

            response = self.br.submit()
            response.read()

        return

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
      
    def createValueDicts(self):       
        """
        Init dictionaries by value/label:
        - Releases by Value
        - Physics group by value
        - DBS url by value
        - DBS rul by label
        - Status of savannah request by value 
        - Status of savannah ticket by value (Open/Closed/Any)
        """      
        if self.isLoggedIn:
            self.br.select_form(name="bug_form")
            
            control = self.br.find_control("custom_sb2",type="select")
            self.ReleaseByValueDict = self.getLabelByValueDict(control)

            control = self.br.find_control("custom_sb3",type="select")
            self.GroupByValueDict = self.getLabelByValueDict(control)

            control = self.br.find_control("custom_sb4",type="select")
            self.DBSByValueDict = self.getLabelByValueDict(control)
            self.DBSByLabelDict = self.getValueByLabelDict(control)

            control = self.br.find_control("resolution_id",type="select")
            self.StatusByValueDict = self.getLabelByValueDict(control)

            control = self.br.find_control("status_id",type="select")
            self.TicketStatusByLabelDict = self.getValueByLabelDict(control)

        return
    
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
    
    def phEDExNodetocmsName(self, nodeList):
        """
        Convert PhEDEx node name list to cms names list 
        """
        names = []
        for node in nodeList:
            name = node.replace('_MSS',
                                '').replace('_Disk',
                                    '').replace('_Buffer',
                                        '').replace('_Export', '')
            if name not in names:
                names.append(name)
        return names
    
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
    
    def getRequests(self,**kargs):
        """
        getRequests Actually goes through all the savannah requests and create json files if the 
        ticket is not Closed and the status of the item is Done.
        It also reports back the summary of the requests in savannah
        """
        requests = []
        
        # Open Browser and login into Savannah
        self.br=Browser()
        self.isLoggedIn = self.login2Savannah()
        
        if self.isLoggedIn:
            if not kargs:
                self.selectQueryForm(approval_status='1',task_status='0')
            else:
                self.selectQueryForm(**kargs)
            self.createValueDicts()
        
            self.br.select_form(name="bug_form")
            response = self.br.submit()

            html_ouput = response.read()
            
            scramArchByCMSSW = self.getScramArchByCMSSW()
            self.nodeMappings = self.phedex.getNodeMap()
            
            for link in self.br.links(text_regex="#[0-9]+"):
                response = self.br.follow_link(link)
                
                try:
                    ## Get Information
                    self.br.select_form(name="item_form")

                    ## remove leading &nbsp and # from task
                    task = link.text.replace('#','').decode('utf-8').strip()
                    print("Processing ticket: %s" % task)
                    
                    ## Get input dataset name
                    control = self.br.find_control("custom_tf1",type="text")
                    input_dataset = control.value
                    input_primary_dataset = input_dataset.split('/')[1].replace(' ','')
                    input_processed_dataset = input_dataset.split('/')[2].replace(' ','')
                    data_tier = input_dataset.split('/')[3].replace(' ','')
                    
                    ## Get DBS URL by Drop Down
                    control = self.br.find_control("custom_sb4",type="select")
                    dbs_url = self.DBSByValueDict[control.value[0]]

                    ## Get DBS URL by text field (for old entries)
                    if dbs_url=='None':
                        control = self.br.find_control("custom_tf4",type="text")
                        dbs_url = control.value.replace(' ','')
                    else: # Transform input value to a valid DBS url
                        #dbs_url = "https://cmsweb.cern.ch/dbs/prod/"+dbs_url+"/DBSReader"
                        dbs_url = dbs_base_url+dbs_url+"/DBSReader"
                        
                    ## Get Release
                    control = self.br.find_control("custom_sb2",type="select")
                    release_id = control.value
                    
                    ## Get current request status
                    control = self.br.find_control("status_id",type="select")
                    request_status_id = control.value
                    RequestStatusByValueDict = self.getLabelByValueDict(control)
                    
                    # close the request if deprecated release was used
                    try:
                        release = self.ReleaseByValueDict[release_id[0]]
                    except:
                        if len(self.ReleaseByValueDict)>0 and RequestStatusByValueDict[request_status_id[0]] != "Closed":
                            msg = "Your request is not valid anymore, since the given CMSSW release is deprecated. If your request should be still processed, please reopen the request and update the CMSSW release to a more recent *working* release.\n"
                            msg+= "\n"
                            msg+= "Thanks,\n"
                            msg+= "Your StoreResults team"
                            self.closeRequest(task,msg)
                            self.br.back()
                            print("I tried to Close ticket %s due to CMSSW not valid" % task)
                            continue
                    
                    # close the request if release has not ScramArch match
                    if release not in scramArchByCMSSW:
                        if len(self.ReleaseByValueDict)>0 and RequestStatusByValueDict[request_status_id[0]] != "Closed":
                            msg = "Your request is not valid, there is no ScramArch match for the given CMSSW release.\n"
                            msg+= "If your request should be still processed, please reopen the request and update the CMSSW release according to: https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML \n"
                            msg+= "\n"
                            msg+= "Thanks,\n"
                            msg+= "Your StoreResults team"
                            self.closeRequest(task,msg)
                            self.br.back()
                            print("I tried to Close ticket %s due to ScramArch mismatch" % task)
                            continue
                    else: 
                        index=len(scramArchByCMSSW[release])
                        scram_arch = scramArchByCMSSW[release][index-1]

                    # close the request if dataset is not at dbs url
                    try:
                        data_at_url = self.isDataAtUrl(dbs_url,input_dataset)
                    except:
                        print('I got an error trying to look for dataset %s at %s, please look at this ticket: %s' %(input_dataset,dbs_url,task))
                        continue
                    if not data_at_url:
                        msg = "Your request is not valid, I could not find the given dataset at %s\n" % dbs_url
                        msg+= "If your request should be still processed, please reopen the request and change DBS url properly \n"
                        msg+= "\n"
                        msg+= "Thanks,\n"
                        msg+= "Your StoreResults team"
                        self.closeRequest(task,msg)
                        self.br.back()
                        print("I tried to Close ticket %s, dataset is not at DBS url" % task)
                        continue
                        
                    # Avoid not approved Tickets
                    #if not RequestStatusByValueDict[request_status_id[0]] == "Done":
                    #    continue

                    ## Get Physics Group
                    control = self.br.find_control("custom_sb3",type="select")
                    group_id = control.value[0]
                    group_squad = 'cms-storeresults-'+self.GroupByValueDict[group_id].replace("-","_").lower()

                    ## Get Dataset Version
                    control = self.br.find_control("custom_tf3",type="text")
                    dataset_version = control.value.replace(' ','')
                    if dataset_version == "": dataset_version = '1'
                                        
                    ## Get current status
                    control = self.br.find_control("resolution_id",type="select")
                    status_id = control.value

                    ## Get assigned to
                    control = self.br.find_control("assigned_to",type="select")
                    AssignedToByValueDict = self.getLabelByValueDict(control)
                    assignedTo_id = control.value

                    ##Assign task to the physics group squad
                    if AssignedToByValueDict[assignedTo_id[0]]!=group_squad:
                        assignedTo_id = [self.getValueByLabelDict(control)[group_squad]]
                        control.value = assignedTo_id
                        self.br.submit()

                    # Set default Adquisition Era for StoreResults 
                    acquisitionEra = "StoreResults"

                    ## Construction of the new dataset name (ProcessingString)
                    ## remove leading hypernews or physics group name and StoreResults+Version
                    if input_processed_dataset.find(self.GroupByValueDict[group_id])==0:
                        new_dataset = input_processed_dataset.replace(self.GroupByValueDict[group_id],"",1)
                    else:
                        stripped_dataset = input_processed_dataset.split("-")[1:]
                        new_dataset = '_'.join(stripped_dataset)
                    
                except Exception as ex:
                    self.br.back()
                    print("There is a problem with this ticket %s, please have a look to the error:" % task)
                    print(str(ex))
                    print(traceback.format_exc())
                    continue
                
                self.br.back()
                
                # Get dataset site info:
                phedex_map, se_names = self.getDatasetOriginSites(dbs_url,input_dataset)
                sites = self.phEDExNodetocmsName(phedex_map)
                
                infoDict = {}
                # Build store results json
                # First add all the defaults values
                infoDict["RequestType"] = "StoreResults"
                infoDict["UnmergedLFNBase"] = "/store/unmerged" 
                infoDict["MergedLFNBase"] = "/store/results/" + self.GroupByValueDict[group_id].replace("-","_").lower()
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
                infoDict["GlobalTag"] = self.setGlobalTagFromOrigin(dbs_url,input_dataset)
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
                if self.StatusByValueDict[status_id[0]]=='Done' and RequestStatusByValueDict[request_status_id[0]] != "Closed":
                    self.writeJSONFile(task, infoDict)
                    report["json"] = 'y'
                else:
                    report["json"] = 'n'
                    
                report["task"] = int(task)
                report["InputDataset"] = input_dataset
                report["ProcessingString"] = new_dataset
                report["ticketStatus"] = self.StatusByValueDict[status_id[0]]
                report["assignedTo"] = AssignedToByValueDict[assignedTo_id[0]]
                report["localUrl"] = dbs_url
                report["sites"] = list(sites)
                report["se_names"] = list(se_names)

                # if the request is closed, change the item status to report to Closed
                if report["ticketStatus"] == "Done" and RequestStatusByValueDict[request_status_id[0]] == "Closed":
                    report["ticketStatus"] = "Closed"

                requests.append(report)
                    
            # Print out report
            self.printReport(requests)
        # Close connections
        self.br.close()
        
        return requests

    def closeRequest(self,task,msg):
        """
        This close a specific savannag ticket
        Insert a message in the ticket
        """
        if self.isLoggedIn:
            #self.createValueDicts()
            
            response = self.br.open('https://savannah.cern.ch/task/?'+str(task))

            html = response.read()

            self.br.select_form(name="item_form")

            control = self.br.find_control("status_id",type="select")
            control.value = [self.TicketStatusByLabelDict["Closed"]]

            #Put reason to the comment field
            control = self.br.find_control("comment",type="textarea")
            control.value = msg
                        
            #DBS Drop Down is a mandatory field, if set to None (for old requests), it is not possible to close the request
            self.setDBSDropDown()
                        
            self.br.submit()

            #remove JSON ticket
            self.removeJSONFile(task)
            
            self.br.back()
        return

    def setDBSDropDown(self):
        ## Get DBS URL by Drop Down
        control = self.br.find_control("custom_sb4",type="select")
        dbs_url = self.DBSByValueDict[control.value[0]]

        ## Get DBS URL by text field (for old entries)
        if dbs_url=='None':
            tmp = self.br.find_control("custom_tf4",type="text")
            dbs_url = tmp.value.replace(' ','')

            if dbs_url.find("phys01")!=-1:
                control.value = [self.DBSByLabelDict["phys01"]]
            elif dbs_url.find("phys02")!=-1:
                control.value = [self.DBSByLabelDict["phys02"]]
            elif dbs_url.find("phys03")!=-1:
                control.value = [self.DBSByLabelDict["phys03"]]
            else:
                msg = 'DBS URL of the old request is neither phys01, phys02 nor phys03. Please, check!'
                print(msg)
                raise RuntimeError(msg)

        return

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

    def printReport(self, requests):
        """
        Print out a report
        """
        print("%20s %10s %5s %35s %10s %50s %50s" %( 'Savannah Ticket','Status','json','Assigned to','local DBS','Sites','se_names')) 
        print("%20s %10s %5s %35s %10s %50s %50s" %( '-'*20,'-'*10,'-'*5,'-'*35,'-'*10,'-'*50,'-'*50 ))
        
        for report in requests:
            
            json = report["json"]
            ticket = report["task"]
            status = report["ticketStatus"]
            assigned = report["assignedTo"]
            localUrl = report["localUrl"].split('/')[5]
            site = ', '.join(report["sites"])
            se_names = ', '.join(report["se_names"])
            print("%20s %10s %5s %35s %10s %50s %50s" %(ticket,status,json,assigned,localUrl,site,se_names))  

        
