#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=R0903
# R0903: too-few-public-methods
"""
File       : parseUnifiedCampaigns.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Parse Unified campaigns json record or read them
from Unified MongoDB. The yielded campaign records are ready
for CouchDB and follow the campaign schema:
```
{
  "campaign_A": [record1, record2],
  "campaign_B: [...]
}
```
where individual records have the following structure:
```
{
 "CampaignName": "blah2019",
 "PrimaryAAA": boolean,
 "SecondaryAAA": boolean,
 "SiteWhiteList": ["T1", "T2"],
 "SiteBlackList": [...],
 "SecondaryLocation": ["T1", "T2"] # list with sites to hold a whole copy of the dataset
 "Secondaries": {
    "datasetA": ["list of sites for SiteWhitelist"],
    "datasetB": ["list of sites for SiteWhitelist"]
 },
 "MaxCopies": integer,
 "PartialCopy": integer,
 "TiersToDM": ["datatiers blocked in unified configuration that can be passed to DDM"]
}
```

***** Example of execution *****
python parseUnifiedCampaigns.py --dburi=mongodb://localhost:27017
       --dbname=unified --dbcoll=campaignsConfiguration --verbose=10 --fout=output.json

or, parsing campaigns.json file
python parseUnifiedCampaigns.py --fin=/path/WmAgentScripts/campaigns.json

or yet parsing a json file filled of campaign configurations already compliant
to the WMCore schema:
python parseUnifiedCampaigns.py --fin=wmcore_campaign.json --url=https://alancc7-cloud2.cern.ch/reqmgr2
         --verbose=10 --testcamp
"""
from __future__ import print_function, division

from builtins import object
from future.utils import viewitems

import argparse
import json
import logging
# system modules
import os

# pymongo modules
try:
    from pymongo import MongoClient
except ImportError:
    print("pymongo library not found. Faking it")
    MongoClient = None

# WMCore modules
try:
    from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
except ImportError:
    print("WMCore/ReqMgrAux environemnt not found. Faking it")
    ReqMgrAux = None


class OptionParser(object):
    "Helper class to handle script options"

    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
                                 dest="fin", default="", help="Input file")
        self.parser.add_argument("--fout", action="store",
                                 dest="fout", default="",
                                 help="Output json file with all the campaigns created")
        self.parser.add_argument("--verbose", action="store",
                                 dest="verbose", default=0, help="verbosity level")
        self.parser.add_argument("--url", action="store",
                                 dest="url", default="",
                                 help="url of server where we should upload campaign configs")
        self.parser.add_argument("--dburi", action="store",
                                 dest="dburi", default="", help="MongoDB URI")
        self.parser.add_argument("--dbname", action="store",
                                 dest="dbname", default="", help="MongoDB database")
        self.parser.add_argument("--dbcoll", action="store",
                                 dest="dbcoll", default="", help="MongoDB collection")
        self.parser.add_argument("--testcamp", action="store_true",
                                 dest="testcamp", default=False,
                                 help="Insert test campaign configurations into Couch")


def intersect(slist1, slist2):
    "Helper function to intersect values from two non-empty lists"
    if slist1 and slist2:
        return list(set(slist1) & set(slist2))
    if slist1 and not slist2:
        return slist1
    if not slist1 and slist2:
        return slist2
    return []


def union(slist1, slist2):
    "Helper function to union values from two lists"
    if slist1 and slist2:
        return list(set(slist1) | set(slist2))
    if slist1 and not slist2:
        return slist1
    if not slist1 and slist2:
        return slist2
    return list(set(slist1 + slist2))


def getSecondaryAAA(initialValue, uniRecord):
    """
    SecondaryAAA: boolean to flag whether to use AAA for the secondary dataset;
    mapped from the secondary_AAA key, which can be either:
      * at top level or
      * under the secondaries dictionary.
    If it appears multiple times, we make an OR of the values.
    """
    for _, innerDict in viewitems(uniRecord.get("secondaries", {})):
        if "secondary_AAA" in innerDict:
            print("Found internal secondary_AAA for campaign: %s" % uniRecord['name'])
            initialValue = initialValue or innerDict["secondary_AAA"]
    return initialValue


def getSiteList(keyName, initialValue, uniRecord):
    """
    Parse information related to the SiteWhiteList and SiteBlackList, which corresponds
    to a list of sites where the workflow gets (or doesn't get) assigned to.
    Mapped from multiple places in the Unified campaigns, such as:
      * top level dict,
      * under the parameters key and
      * under the secondaries dictionary (no longer used, see WMCore #10332).
    If it appears multiple times, we make an intersection of the values
    """
    if keyName in uniRecord.get("parameters", {}):
        print("Found internal %s for campaign: %s" % (keyName, uniRecord['name']))
        initialValue = intersect(initialValue, uniRecord["parameters"][keyName])

    return initialValue


def getSecondaryLocation(initialValue, uniRecord):
    """
    SecondaryLocation: list of sites where the secondary PREMIX dataset has to be placed
    as a whole (not necessarily also assigned to those).
    mapped from the SecondaryLocation key, which can be either:
      * at top level or
      * under the secondaries dictionary.
    If it appears multiple times, we make an intersection of the values.
    """
    for _, innerDict in viewitems(uniRecord.get("secondaries", {})):
        if "SecondaryLocation" in innerDict:
            print("Found internal SecondaryLocation for campaign: %s" % uniRecord['name'])
            initialValue = intersect(initialValue, innerDict["SecondaryLocation"])
    return initialValue


def getSecondaries(initialValue, uniRecord):
    """
    Secondaries: dictionary with a map of allowed secondary datasets and where they are
    supposed to be placed (in conjunction with the top level location parameters;
    mapped from the secondaries top level key

    Each dataset will have a list value type, and the content is either:
      * taken from the SiteWhitelist key or
      * taken from the SecondaryLocation one
    """
    for dset, innerDict in viewitems(uniRecord.get("secondaries", {})):
        print("Found secondaries for campaign: %s" % uniRecord['name'])
        initialValue[dset] = intersect(innerDict.get("SiteWhitelist", []),
                                       innerDict.get("SecondaryLocation", []))

    return initialValue


def parse(istream, verbose=0):
    "Parse Unified campaign configuration"
    wmCampaigns = []

    # re-map Unified keys into campaign schema ones
    remap = {
        'name': 'CampaignName',
        'SiteWhitelist': 'SiteWhiteList',
        'SiteBlacklist': 'SiteBlackList',
        'primary_AAA': 'PrimaryAAA',
        'secondary_AAA': 'SecondaryAAA',
        'SecondaryLocation': 'SecondaryLocation',
        'secondaries': 'Secondaries',
        'partial_copy': 'PartialCopy',
        'toDDM': 'TiersToDM',
        'maxcopies': 'MaxCopies'}
    # campaign schema dict
    confRec = {
        'CampaignName': None,
        'SiteWhiteList': [],
        'SiteBlackList': [],
        'PrimaryAAA': False,
        'SecondaryAAA': False,
        'SecondaryLocation': [],
        'Secondaries': {},
        'PartialCopy': 1,
        'TiersToDM': [],
        'MaxCopies': 1}

    if not isinstance(istream, list):
        istream = [istream]
    for rec in istream:
        if verbose > 1:
            print("read record: %s (type=%s)" % (rec, type(rec)))

        conf = dict(confRec)
        # Set default value from top level campaign configuration
        # or use the default values defined above
        for uniKey, wmKey in viewitems(remap):
            conf[wmKey] = rec.get(uniKey, conf[wmKey])

        conf['SiteWhiteList'] = getSiteList("SiteWhitelist", conf['SiteWhiteList'], rec)
        conf['SiteBlackList'] = getSiteList("SiteBlacklist", conf['SiteBlackList'], rec)
        conf['SecondaryAAA'] = getSecondaryAAA(conf['SecondaryAAA'], rec)
        conf['SecondaryLocation'] = getSecondaryLocation(conf['SecondaryLocation'], rec)
        conf['Secondaries'] = getSecondaries(conf['Secondaries'], rec)
        if verbose:
            print("Final WMCore Campaign configuration: %s" % conf)
        wmCampaigns.append(conf)

    print("\nParser found and created %d campaign configurations\n" % len(wmCampaigns))
    return wmCampaigns


def process(recs):
    "Helper function to process given records and return unique list"
    rdict = {}
    for rec in recs:
        campaign = rec['CampaignName']
        if campaign in rdict:
            if rdict[campaign] != rec:
                print("WARNING: found new record for campaign '%s'" % campaign)
                print("new record: %s" % rec)
                print("old record: %s" % rdict[campaign])
        else:
            rdict[campaign] = rec
            yield rec


def upload(mgr, campRec):
    "Upload campaign configuration"
    if not mgr:
        return
    campaign = campRec.get('CampaignName', '')
    print("Inserting campaign record: %s" % json.dumps(campRec))
    if campaign:
        res = mgr.postCampaignConfig(campaign, campRec)
        print(res)
    else:
        print("ERROR: found a campaign record without a CampaignName value: %s" % campRec)


def insertTestCampaigns(mgr):
    """
    Insert a few campaigns configuration used for testing purposes
    NOTE: the campaign name might change according to the tests you're running.
    :param mgr: ReqMgrAux object
    """
    if not mgr:
        return

    defaultCamp = {'CampaignName': '', 'MaxCopies': 1, 'PartialCopy': 1, 'TiersToDM': [],
                   'PrimaryAAA': False, 'Secondaries': {}, 'SecondaryAAA': False,
                   'SecondaryLocation': ["T1_US_FNAL_Disk", "T2_CH_CERN"],
                   'SiteBlackList': [], 'SiteWhiteList': ["T1_US_FNAL", "T2_CH_CERN"]}

    testCamp = ("CMSSW_10_6_1_patch1_Step1", "CMSSW_10_6_1_patch1_Step2",
                "CMSSW_7_3_2__test2inwf-1510737328", "CMSSW_11_2_0_pre6__fullsim_noPU_2021_14TeV-1599843628",
                "RelVal_Generic_Campaign", "DMWM_Test",
                "Agent212_Val", "Agent214_Val", "Agent216_Val",
                "Sept2022_Val", "Oct2022_Val", "Nov2022_Val", "Dec2022_Val", "Jan2023_Val",
                "HG2209_Val", "HG2210_Val", "HG2211_Val", "HG2212_Val", "HG2301_Val")
    for campName in testCamp:
        defaultCamp['CampaignName'] = campName
        upload(mgr, defaultCamp)


def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    verbose = int(opts.verbose)
    logger = None
    mgr = None
    inputWMCore = False
    if verbose:
        logger = logging.getLogger('parse_campaign')
        logger.setLevel(logging.DEBUG)
        logging.basicConfig()
    if opts.url:
        key = os.getenv('X509_USER_KEY', '')
        cert = os.getenv('X509_USER_CERT', '')
        proxy = os.getenv('X509_USER_PROXY', '')
        if proxy and not cert:
            cert = proxy
            key = proxy
        hdict = {'cert': cert, 'key': key, 'pycurl': True}
        mgr = ReqMgrAux(opts.url, hdict, logger=logger)
    if opts.dburi:
        conn = MongoClient(host=opts.dburi)
        dbname = opts.dbname
        dbcoll = opts.dbcoll
        if verbose:
            print("### read data from '%s', %s/%s" % (opts.dburi, dbname, dbcoll))
        data = [r for r in conn[dbname][dbcoll].find()]
    elif opts.fin:
        fin = opts.fin
        if verbose:
            print("### read data from '%s'" % fin)
        data = []
        with open(fin, 'r') as istream:
            campData = json.load(istream)
            if isinstance(campData, dict):
                # then it's a Unified-like campaign schema
                for key, val in viewitems(campData):
                    rec = {'name': key}
                    rec.update(val)
                    data.append(rec)
            elif isinstance(campData, list):
                # then the input file has WMCore-like campaign schema
                print("Found %d campaigns in the input file." % len(campData))
                data = campData
                inputWMCore = True
    if not inputWMCore:
        data = parse(data, verbose)

    output = []  # in case we want to dump all records to a json file
    for rec in process(data):
        output.append(rec)
        upload(mgr, rec)
    if opts.testcamp:
        insertTestCampaigns(mgr)
    if opts.fout:
        print("Saving all %d unique campaign records to: %s\n" % (len(output), opts.fout))
        with open(opts.fout, "w") as jo:
            json.dump(output, jo, indent=2)


if __name__ == '__main__':
    main()
