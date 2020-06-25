"""
For more details about StoreResults workflows, please refer to:
https://github.com/dmwm/WMCore/wiki/StoreResults-requests

Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
 2. wmagent env: source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
 3. have the correct permissions in SiteDB, otherwise dataset migration won't work

Expected input json file like:
[{"InputDataset": "/EmbeddingRun2016B/MuTauFinalState-imputSep16DoubleMu_mirror_miniAOD-v2/USER",
  "DbsUrl": "phys03",
  "ScramArch": "slc6_amd64_gcc530",
  "SiteWhitelist": "T2_DE_DESY",
  "PhysicsGroup": "Tau POG",
  "CMSSWVersion": "CMSSW_8_0_26_patch1"},
  {..},
  {..}]

"""

from __future__ import print_function, division

from future import standard_library
standard_library.install_aliases()

import http.client
import json
import os
import sys
import time
from copy import copy

from dbs.apis.dbsClient import DbsApi

url = "cmsweb.cern.ch"
dbsApi = DbsApi(url='https://%s/dbs/prod/global/DBSMigrate/' % url)

DEFAULT_DICT = {
    "CMSSWVersion": "UPDATEME",
    "InputDataset": "UPDATEME",
    "PhysicsGroup": "UPDATEME",
    "PrepID": "UPDATEME",
    "ProcessingString": "UPDATEME",
    "RequestString": "UPDATEME",
    "ScramArch": "UPDATEME",
    "SiteWhitelist": "UPDATEME",  # Will be picked by Unified and posted again at assignment
    "AcquisitionEra": "UPDATEME",
    "ProcessingVersion": 1,  # will be updated too
    "Campaign": "StoreResults",
    "DbsUrl": "https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader",
    "GlobalTag": "crab3_tag",
    "Memory": 2000,
    "RequestPriority": 999999,
    "RequestType": "StoreResults",
    "SizePerEvent": 512,
    "TimePerEvent": 1}


MANDATORY_FIELDS = {"CMSSWVersion", "ScramArch", "DbsUrl",
                    "InputDataset", "SiteWhitelist", "PhysicsGroup"}


def main():
    if len(sys.argv) != 2:
        print("Usage: python injectHarvest.py <input_json_file>")
        sys.exit(0)

    inputFile = sys.argv[1]
    with open(inputFile) as fo:
        items = json.load(fo)
    # now create requests for each of the datasets
    for entry in items:
        migrateDataset(entry['InputDataset'], entry['DbsUrl'])
        newDict = buildRequest(entry)
        if newDict is None:
            # user provided incomplete data (or mistyped something)
            continue
        workflow = submitWorkflow(newDict)
        approveRequest(workflow)
    sys.exit(0)


def migrateDataset(dset, dbsInst):
    """
    Migrate dataset from the user instance to the DBS prod one.
    It returns the origin site name, which should be used for assignment
    """
    dbsInst = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader" % dbsInst
    migrateArgs = {'migration_url': dbsInst, 'migration_input': dset}
    dbsApi.submitMigration(migrateArgs)
    print("Migrating dataset %s from %s to prod/global" % (dset, dbsInst))


def buildRequest(userDict):
    """
    Expects the following user data:
      CMSSWVersion, ScramArch, DbsUrl, InputDataset, SiteWhitelist and PhysicsGroup 
    """
    if set(userDict.keys()) != MANDATORY_FIELDS:
        print("ERROR: user input data is incomplete: %s" % userDict)
        print("User json *must* have these fields: %s" % MANDATORY_FIELDS)
        return None

    newSchema = copy(DEFAULT_DICT)
    newSchema.update(userDict)
    newSchema['DbsUrl'] = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader" % newSchema['DbsUrl']
    # Remove spaces from the Physics Group value
    newSchema['PhysicsGroup'] = newSchema['PhysicsGroup'].replace(" ", "")
    # Set PrepID according to the date and time
    newSchema["PrepID"] = "StoreResults-%s" % time.strftime("%d%m%y-%H%M%S")
    # Truncate the ProcessingString, otherwise it can be larger than allowed
    primDset, procDset, _tier = newSchema['InputDataset'].split("/")[1:]
    acqEra, procStr = procDset.split("-", 1)
    newSchema["AcquisitionEra"] = acqEra  # should we worry about length limits?
    procStr, procVer = procStr.rsplit("-", 1)
    newSchema["ProcessingString"] = "StoreResults_" + procStr[:67]  # limit to 80 chars
    # ProcessingString cannot have a dash char
    newSchema["ProcessingString"] = newSchema["ProcessingString"].replace("-", "_")
    try:
        newSchema["ProcessingVersion"] = int(procVer[1:])
    except ValueError:
        newSchema["ProcessingVersion"] = 1
    # Use PrimaryDataset and ProcessedDataset in the RequestString
    newSchema["RequestString"] = primDset[:35] + "-" + procDset[:35]
    return newSchema


def submitWorkflow(schema):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    encodedParams = json.dumps(schema)
    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("POST", "/reqmgr2/data/request", encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        print("Error message: %s" % resp.msg.getheader('X-Error-Detail'))
        return None
    data = json.loads(data)
    requestName = data['result'][0]['request']
    print("  Request %s successfully created.\n" % requestName)
    return requestName


def approveRequest(workflow):
    if workflow is None:
        return

    # print("Approving request...")
    encodedParams = json.dumps({"RequestStatus": "assignment-approved"})
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("PUT", "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        if hasattr(resp.msg, "x-error-detail"):
            print("Error message: %s" % resp.msg["x-error-detail"])
            sys.exit(2)
    conn.close()
    # print("  Request successfully approved!")
    return


if __name__ == '__main__':
    main()
