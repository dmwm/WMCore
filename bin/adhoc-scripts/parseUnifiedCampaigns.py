#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=R0903,R1702,R0912
"""
File       : ParseUnifiedCampaigns.py
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
 "MaxCopies": integer
}
```
"""
from __future__ import print_function, division


# system modules
import os
import json
import logging
import argparse

# pymongo modules
from pymongo import MongoClient

# WMCore modules
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux


class OptionParser(object):
    "Helper class to handle script options"
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument(
            "--fin", action="store",
            dest="fin", default="", help="Input file")
        self.parser.add_argument(
            "--verbose", action="store",
            dest="verbose", default=0, help="verbosity level")
        self.parser.add_argument(
            "--url", action="store",
            dest="url", default="",
            help="url of server where we should upload campaign configs")
        self.parser.add_argument(
            "--dburi", action="store",
            dest="dburi", default="", help="MongoDB URI")
        self.parser.add_argument(
            "--dbname", action="store",
            dest="dbname", default="", help="MongoDB database")
        self.parser.add_argument(
            "--dbcoll", action="store",
            dest="dbcoll", default="", help="MongoDB collection")


def intersect(slist1, slist2):
    "Helper function to intersect values from two lists"
    if slist1 and slist2:
        return list(set(slist1) & set(slist2))
    if slist1 and not slist2:
        return slist1
    if not slist1 and slist2:
        return slist2
    return list(set(slist1 + slist2))


def union(slist1, slist2):
    "Helper function to union values from two lists"
    if slist1 and slist2:
        return list(set(slist1) | set(slist2))
    if slist1 and not slist2:
        return slist1
    if not slist1 and slist2:
        return slist2
    return list(set(slist1 + slist2))


def parse(istream, verbose=0):
    "Parse Unified campaign configuration"
    tot = 0
    totRecords = 0
    # re-map Unified keys into campaign schema ones
    remap = {
        'SiteWhitelist': 'SiteWhiteList',
        'SiteBlacklist': 'SiteBlackList',
        'secondary_AAA': 'SecondaryAAA',
        'primary_AAA': 'PrimaryAAA',
        'maxcopies': 'MaxCopies',
        'secondaries': 'Secondaries'}
    # campaign schema dict
    confRec = {
        'SiteWhiteList': [], 'SiteBlackList': [],
        'CampaignName': None, 'PrimaryAAA': False,
        'SecondaryAAA': False, 'SecondaryLocation': [],
        'Secondaries': {}, 'MaxCopies': 1}
    totkeys = {}
    if not isinstance(istream, list):
        istream = [istream]
    for data in istream:
        if verbose > 1:
            print("read record: %s (type=%s)" % (data, type(data)))
        recMongo = None
        if 'name' in data:  # single mongo record
            recMongo = True
        for rec in data.keys():
            if recMongo:
                campaign = data['name']
                crec = data
            else:
                campaign = rec
                crec = data[campaign]
            ckeys = crec.keys()
            if verbose > 1:
                print("campaign: %s\nrecord: %s\nckeys: %s\n" % (campaign, json.dumps(crec), ckeys))
            tot += 1
            conf = dict(confRec)
            conf['CampaignName'] = campaign
            whiteList = []
            blackList = []
            secAAA = False
            for key in ckeys:
                val = crec[key]
                totkeys[key] = 1
                if key in conf.keys():
                    conf[key] = val
                if key in remap.keys():
                    conf[remap[key]] = val
                if key == 'SiteWhiteList':
                    whiteList = intersect(whiteList, val)
                if key == 'SiteBlackList':
                    blackList = union(blackList, val)
                if key.lower() == 'secondaryaaa' or key.lower() == 'secondary_aaa':
                    secAAA = True if val else secAAA
                if key == 'parameters':
                    for kkk, vvv in crec[key].items():
                        if kkk in conf.keys():
                            conf[kkk] = vvv
                        if kkk in remap.keys():
                            conf[remap[kkk]] = vvv
                        if kkk.lower() == 'sitewhitelist':
                            whiteList = intersect(whiteList, vvv)
                        if kkk.lower() == 'siteblacklist':
                            blackList = union(blackList, vvv)
                        if kkk.lower() == 'secondaryaaa' or kkk.lower() == 'secondary_aaa':
                            secAAA = True if vvv else secAAA
            if verbose > 2:
                print("conf: %s" % conf)
            sec = dict(conf['Secondaries'])
            sdict = {}
            secLoc = []
            for key, val in sec.items():
                wlist = val.get('SiteWhitelist', [])
                blist = val.get('SiteBlacklist', [])
                whiteList = intersect(whiteList, wlist)
                blackList = union(blackList, blist)
                secLoc = val.get('SecondaryLocation', [])
                if key.lower() == 'secondaryaaa' or key.lower() == 'secondary_aaa':
                    secAAA = True if val else secAAA
                if isinstance(val, dict):
                    for kkk, vvv in val.items():
                        if kkk.lower() == 'secondaryaaa' or kkk.lower() == 'secondary_aaa':
                            secAAA = True if vvv else secAAA
                        if kkk == 'SecondaryLocation':
                            secLoc += vvv
                secLoc = list(set(secLoc))
                if secLoc:
                    sdict[key] = secLoc
                else:
                    sdict[key] = wlist
            conf['SecondaryAAA'] = secAAA
            conf['Secondaries'] = sdict
            if secLoc and conf['SecondaryLocation']:
                conf['SecondaryLocation'] = intersect(secLoc, conf['SecondaryLocation'])
            if whiteList is None:
                whiteList = []
            if blackList is None:
                blackList = []
            conf['SiteWhiteList'] = whiteList
            conf['SiteBlackList'] = blackList
            totRecords += 1
            yield conf
    if verbose:
        print("Total number of campaign configurations: %s" % tot)
        print("Total number of campaign configurations records: %s" % totRecords)
    if verbose > 1:
        print("Campaign configuration keys: %s" % json.dumps(totkeys.keys()))


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


def upload(mgr, config):
    "Upload campaign configuration"
    if not mgr:
        return
    campaign = config.get('Campaign', '')
    if campaign:
        mgr.postCampaignConfig(campaign, config)


def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    verbose = int(opts.verbose)
    logger = None
    mgr = None
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
        mgr = ReqMgrAux(opts.url, httpDict=hdict, logger=logger)
    if opts.dburi:
        conn = MongoClient(host=opts.dburi)
        dbname = opts.dbname
        dbcoll = opts.dbcoll
        if verbose:
            print("### read data from '%s', %s/%s" % (opts.dburi, dbname, dbcoll))
        data = [r for r in conn[dbname][dbcoll].find()]
    else:
        fin = opts.fin
        if verbose:
            print("### read data from '%s'" % fin)
        with open(fin, 'r') as istream:
            data = json.load(istream)
    records = process(parse(data, verbose))
    for rec in records:
        print(json.dumps(rec))
        upload(mgr, rec)


if __name__ == '__main__':
    main()
