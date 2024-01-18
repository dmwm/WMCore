#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script can be used to fetch and update pileup objects through the
MSPileup REST API.

The user provides the MSPileup URL and a JSON file as input, which will be
used to update every single pileup document that it fetches from MSPileup.

Example usage is:
python3 updatePileupObjects.py --url=https://cmsweb.cern.ch --fin=override_data.json
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


class OptionParser():
    """Class to parse the command line arguments"""

    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", required=True,
                                 dest="fin", help="Input JSON file")
        self.parser.add_argument("--url", action="store",
                                 dest="url", default="https://cmsweb-testbed.cern.ch",
                                 help="URL for the MSPileup service")


def getPileupDocs(mspileupUrl, certDict, logger):
    """
    Fetch pileup documents from MSPileup
    :param mspileupUrl: string with the MSPileup url
    :param certDict: dictionary with cert and key reference
    :param logger: logger object
    :return: a list with the pileup documents
    """
    url = mspileupUrl + "/ms-pileup/data/pileup"
    logger.info("Fetching pileup documents from URL: %s", mspileupUrl)
    mgr = RequestHandler()
    params = {}
    headers = {'Content-Type': 'application/json'}

    resp = mgr.getdata(url, params, headers, verb='GET', encode=True, decode=True,
                       ckey=certDict['key'], cert=certDict['cert'])

    logger.debug("Response: %s", resp)
    if resp and not resp.get("result", []):
        msg = f"Failed to retrieve pileup documents. Error: {resp}"
        raise RuntimeError(msg)
    return resp["result"]


def writePileupDocs(mspileupUrl, puDocs, certDict, logger):
    """
    Update each pileup document in MSPileup
    :param mspileupUrl: string with the MSPileup url
    :param puDocs: list with pileup documents
    :param certDict: dictionary with cert and key reference
    :param logger: logger object
    :return: None
    """
    url = mspileupUrl + "/ms-pileup/data/pileup"
    mgr = RequestHandler()
    headers = {'Content-Type': 'application/json'}
    logger.info("Starting to update pileup documents against backend: %s", url)
    for doc in puDocs:
        logger.info("Updating pileup document: %s", doc.get("pileupName"))
        resp = mgr.getdata(url, doc, headers, verb='PUT', encode=True, decode=True,
                           ckey=certDict['key'], cert=certDict['cert'])

        logger.debug("Response: %s", resp)
        if resp and resp.get("result", []):
            msg = f"Failed to inject pileup document for {doc['pileupName']}. Error: {resp}"
            logger.critical(msg)


def main():
    """Executes everything"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    logger = logging.getLogger('updatePileupObjects')
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

    fin = opts.fin
    with open(fin, 'r') as istream:
        puOverride = json.load(istream)
    logger.info(f"Pileup override file: {fin} has the following content: {puOverride}")

    puDocs = getPileupDocs(opts.url, hdict, logger)
    logger.info("Found %d documents in MSPileup", len(puDocs))

    # update the documents with the override content
    for doc in puDocs:
        doc.update(puOverride)
        logger.info("New pileup document is: %s", doc)
    logger.info("\n")

    # finally, update the pileup documents in the database
    writePileupDocs(opts.url, puDocs, hdict, logger)


if __name__ == '__main__':
    main()
