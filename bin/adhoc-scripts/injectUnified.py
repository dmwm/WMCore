#!/usr/bin/env python
"""
This script can be used to create a default Unified configuration in central CouchDB
"""
import argparse
import http.client
import json
import os
import sys
from pprint import pformat

DOC = {"tiers_to_DDM": {"value": ["LHE", "GEN-SIM-DIGI-RAW-MINIAOD", "AODSIM", "MINIAODSIM", "GEN-SIM-RAW",
                                  "GEN-SIM-RECO", "GEN-SIM-RECODEBUG", "AOD", "RECO", "MINIAOD", "ALCARECO",
                                  "USER", "RAW-RECO", "RAWAODSIM", "NANOAOD", "NANOAODSIM", "FEVT", "PREMIX",
                                  "GEN-SIM-DIGI-RAW-HLTDEBUG-RECO", "FEVTDEBUGHLT"],
                        "description": "Datatiers that have a final MSOutput data placement"},
       "tiers_no_DDM": {"value": ["GEN-SIM", "GEN", "SIM", "DQM", "DQMIO", "GEN-SIM-DIGI-RAW", "RAW",
                                  "GEN-SIM-DIGI", "GEN-SIM-DIGI-RAW-HLTDEBUG"],
                        "description": "Datatiers not meant to have a final MSOutput data placement"},
       "tiers_with_no_custodial": {"value": ["DQM", "RECO", "RAWAODSIM", "FEVTDEBUGHLT"],
                                   "description": "Datatiers not meant to have a Tape final MSOutput data placement. Can be overriden at campaign level"}
       }


def parseArgs():
    """
    Parse the command line arguments, or provide default values.
    """
    msg = "Script to resolve a workflow parentage information and insert it into the DBS server"
    parser = argparse.ArgumentParser(description=msg)
    parser.add_argument("-c", "--cmsweb_url", help="CMSWEB URL (default: cmsweb-testbed.cern.ch)",
                        action="store", default='cmsweb-testbed.cern.ch')
    return parser.parse_args()


if __name__ == '__main__':

    # parse the input arguments
    args = parseArgs()
    userCert = os.getenv('X509_USER_PROXY')
    userKey = os.getenv('X509_USER_PROXY')
    if not userCert:
        print("Please set the environment variable X509_USER_PROXY with your user proxy.")
        sys.exit(1)

    headers = {"Content-type": "application/json", "Accept": "application/json"}
    encodedParams = json.dumps(DOC)

    print(f"Updating unified configuration in {args.cmsweb_url} with content: {pformat(DOC)}")
    conn = http.client.HTTPSConnection(args.cmsweb_url, cert_file=userCert, key_file=userKey)
    conn.request("PUT", "/reqmgr2/data/unifiedconfig/config", encodedParams, headers)
    resp = conn.getresponse()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        if hasattr(resp.msg, "x-error-detail"):
            print("Error message: %s" % resp.msg["x-error-detail"])
    else:
        print("  OK!")
    conn.close()
