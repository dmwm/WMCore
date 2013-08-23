"""
collection of functions tools to connect external resource.
"""
import re
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

def handlePreSideEffect(workloadHelper, request_args):
    # handle some thing other than standard procedure of update
    # before the REQMGR rest api succeeded
    # currently just handling store result assigment-approve and assigned
    if workloadHelper.requestType() == "StoreResult":
        if request_args.get("RequestStatus") == "assignment-approved":
            #TODO: call migration dbs
            # 1. dbsapi.migrate
            # 2. get location information from dbs and put in xrootd
            pass
        elif request_args.get("RequestStatus") == "assigned":
            # check
            pass

def handlePostSideEffect(workloadHelper, request_args):
    # handle some thing other than standard procedure of update
    # after the REQMGR rest api succeeded
    # currently just handling store result assigment-approve and assigned
    if workloadHelper.requestType() == "StoreResult":
        if request_args.get("RequestStatus") == "new":
            # 1. get physics group from site db and email address
            # 2. email them the list
            pass
        
        
def getSiteInfo(config):
    sitedb = SiteDBJSON()
    sites = sitedb.getAllCMSNames()    
    sites.sort()
    wildcardKeys = getattr(config, 'wildcardKeys', {'T1*': 'T1_*',
                                                    'T2*': 'T2_*',
                                                    'T3*': 'T3_*'})
    wildcardSites = {}
    
    for k in wildcardKeys.keys():
        reValue = wildcardKeys.get(k)
        found   = False
        for s in sites:
            if re.search(reValue, s):
                found = True
                if not k in wildcardSites.keys():
                    wildcardSites[k] = []
                wildcardSites[k].append(s)
        if found:
            sites.append(k)
    return sites
