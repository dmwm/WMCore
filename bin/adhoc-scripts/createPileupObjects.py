#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script can be used to parse the WMCore campaign documents
and extract the equivalent pileup configuration.

Attributes that are used from the campaign documents are:
 * CampaignName: to be defined under the pileup "campaignName" attr
 * Secondaries: to fetch the pileup dataset name, type and expected location

where the pileup document minimal schema is:
{
    'pileupName': "",
    'pileupType': "",
    'expectedRSEs': [],
    'campaigns': [],
    'active': False
}

Example usage is:
==> Construct pileup documents from the production campaigns
python3 createPileupObjects.py --url=https://cmsweb.cern.ch --fout=alan.json

==> Inject the previous pileup doc dump into your test cluster
python3 createPileupObjects.py --url=https://cmsweb-test9.cern.ch --fin=alan.json --inject
"""
import argparse
import json
import logging
import os
import sys
from pprint import pprint

try:
    from WMCore.Services.pycurl_manager import RequestHandler
except ImportError:
    print("WMCore/RequestHandler environment not set. Faking it")
    RequestHandler = None
try:
    from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux
except ImportError:
    print("WMCore/ReqMgrAux environment not found. Faking it")
    ReqMgrAux = None


class OptionParser():
    """Class to parse the command line arguments"""

    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
                                 dest="fin", default="", help="Input file")
        self.parser.add_argument("--fout", action="store",
                                 dest="fout", default="",
                                 help="Output json file with all the pileup configurations created")
        self.parser.add_argument("--url", action="store",
                                 dest="url", default="https://cmsweb-testbed.cern.ch",
                                 help="Server url to fetch and create pileup configurations")
        self.parser.add_argument("--inject", action="store_true",
                                 dest="inject", default=False,
                                 help="Inject pileup documents for real")


def getPUSchema(pileupName, pileupDocs, logger):
    """
    Get a minimalistic pileup data schema, or return an
    existent pileup object.

    :param pileupName: string with the pileup name
    :param pileupDocs: list of pileup documents
    :return: a dictionary with the pileup configuration
    """
    for idx, doc in enumerate(pileupDocs):
        if pileupName == doc["pileupName"]:
            logger.warning("Reusing pileup configuration for pileup: %s", pileupName)
            return pileupDocs.pop(idx)
    pileupDoc = {
        'pileupName': "",
        'pileupType': "",
        'expectedRSEs': [],
        'campaigns': [],
        'active': False}
    return pileupDoc


def parseCampaigns(campDocs, logger):
    """
    Parse campaigns from WMCore and convert them into
    Pileup documents, where:
     * campaigns: is a union of all campaigns using a given pileup.
     * expectedRSEs: is a union of all RSEs across different campaigns.
    """
    pileupDocs = []
    for camp in campDocs:
        # for each secondary dataset, create one pileup document
        for puName, puRSEs in camp.get("Secondaries", {}).items():
            puDoc = getPUSchema(puName, pileupDocs, logger)
            if puName.startswith("/Neutrino") or puName.split("/")[-1] == "PREMIX":
                puType = "premix"
            else:
                puType = "classic"
            puDoc["pileupName"] = puName
            puDoc["pileupType"] = puType
            puDoc["expectedRSEs"].extend(puRSEs)
            if camp.get("CampaignName", ""):
                puDoc["campaigns"].append(camp["CampaignName"])
            # make some of these values unique
            puDoc["expectedRSEs"] = list(set(puDoc["expectedRSEs"]))
            puDoc["campaigns"] = list(set(puDoc.get("campaigns", [])))
            pileupDocs.append(puDoc)
    logger.info("Created %d pileup docs out of %d campaigns.", len(pileupDocs), len(campDocs))
    return pileupDocs


def writePileupDocs(mspileupUrl, puDocs, certDict, logger):
    """
    Create pileup documents in MSPileup
    :param mspileupUrl: string with the MSPileup url
    :param puDocs: list with pileup documents
    :param certDict: dictionary with cert and key reference
    :param logger: logger object
    :return: None
    """
    url = mspileupUrl + "/ms-pileup/data/pileup"
    mgr = RequestHandler()
    headers = {'Content-Type': 'application/json'}
    for doc in puDocs:
        logger.info("Injecting against %s document: %s", url, doc)
        resp = mgr.getdata(url, doc, headers, verb='POST', encode=True, decode=True,
                           ckey=certDict['key'], cert=certDict['cert'])

        logger.debug("Response: %s", resp)
        if resp and resp.get("result", []):
            msg = f"Failed to inject pileup document for {doc['pileupName']}. Error: {resp}"
            logger.critical(msg)


def main():
    """Executes everything"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    logger = logging.getLogger('createPileupObjects')
    logger.setLevel(logging.INFO)
    logging.basicConfig()

    # setup proxy/cert
    cert = os.getenv('X509_USER_CERT', '')
    key = os.getenv('X509_USER_KEY', '')
    proxy = os.getenv('X509_USER_PROXY', '')
    if not cert and not proxy:
        logger.error("You need to define the X509 user cert/key or user proxy variables")
        sys.exit(1)
    if not cert and proxy:
        cert = proxy
        key = proxy
    hdict = {'cert': cert, 'key': key, 'pycurl': True}

    if opts.fin:
        fin = opts.fin
        logger.info(f"Reading pileup configuration from file: {fin}")
        with open(fin, 'r') as istream:
            puDocs = json.load(istream)
            logger.info(f"Found {len(puDocs)} pileup objects in the input file.")
    else:
        logger.info("Parsing/fetching pileup objects from ReqMgrAux")
        reqAux = ReqMgrAux(opts.url + "/reqmgr2", hdict, logger=logger)
        campDocs = reqAux.getCampaignConfig("ALL_DOCS")
        logger.info("Retrieved %d campaigns from ReqMgr.", len(campDocs))
        puDocs = parseCampaigns(campDocs, logger)
    if opts.fout:
        logger.info("Saving all %d pileup documents into file: %s", len(puDocs), opts.fout)
        with open(opts.fout, "w") as jo:
            json.dump(puDocs, jo, indent=2, sort_keys=True)

    if opts.inject:
        logger.info("Going to inject %d pileup documents into: %s", len(puDocs), opts.url)
        writePileupDocs(opts.url, puDocs, hdict, logger)
    else:
        logger.info("DRY-RUN: not injecting any pileup documents in MSPileup.")


if __name__ == '__main__':
    main()
