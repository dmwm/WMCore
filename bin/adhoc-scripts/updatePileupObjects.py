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
import urllib.request
import ssl
from http.client import HTTPSConnection


class OptionParser():
    """Class to parse the command line arguments"""

    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", required=True,
                                 dest="fin", help="Input JSON file")
        self.parser.add_argument("--dry-run", action="store_true",
                                 dest="dryrun", help="Fetch docs but do not update")
        self.parser.add_argument("--url", action="store",
                                 dest="url", default="https://cmsweb-testbed.cern.ch",
                                 help="URL for the MSPileup service")


class HTTPSClientAuthHandler(urllib.request.HTTPSHandler):
    """
    Basic HTTPS class
    """

    def __init__(self, key, cert):
        urllib.request.HTTPSHandler.__init__(self)
        # Create a default ssl context manager to carry the credentials.
        # It also loads the default CA certificates
        self.sslContext = ssl.create_default_context()
        self.sslContext.load_cert_chain(cert, keyfile=key)
        # Also load the default CA certificates (apparently not needed)
        # self.sslContext.load_verify_locations(None, '/etc/grid-security/certificates/')

    def https_open(self, req):
        """
        Return https opener
        :param req: request string
        """
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=290):
        """
        Return HTTPSConnection object for given host
        :param host: host name
        :param timeout: timeout used in urllib.request call which will call this function
        """
        return HTTPSConnection(host, context=self.sslContext)


def userAgent():
    """
    Construct proper user agent string
    :return: user-agent string
    """
    major = sys.version_info.major
    minor = sys.version_info.minor
    clientId = f'updatePileupObjects::python/{major}.{minor}'
    user = os.environ.get('USER', '')
    client = f'{clientId} ({user})'
    return client


def getPileupDocs(mspileupUrl, handler, logger):
    """
    Fetch pileup documents from MSPileup
    :param mspileupUrl: string with the MSPileup url
    :param handler: HTTP handler for url opener
    :param logger: logger object
    :return: a list with the pileup documents
    """
    url = mspileupUrl + "/ms-pileup/data/pileup"
    logger.info("Fetching pileup documents from URL: %s", url)

    # place request
    req = urllib.request.Request(url)
    req.add_header('Accept', 'application/json')
    req.add_header('User-agent', userAgent())
    opener = urllib.request.build_opener(handler)
    urllib.request.install_opener(opener)
    with urllib.request.urlopen(req) as resp:
        logger.debug("Response: %s status %s", resp, resp.status)
        # check status ot HTTP response
        if resp.status != 200:
            msg = f"Bad response from server, status {resp.status}, reason {resp.reason}"
            raise RuntimeError(msg)

        # decode and load JSON data
        sdata = resp.read().decode('utf8')
        data = json.loads(sdata)

        logger.debug("Response: %s", data)
        if data and not data.get("result", []):
            msg = f"Failed to retrieve pileup documents. Error: {data}"
            raise RuntimeError(msg)
        return data["result"]


def writePileupDocs(mspileupUrl, puDocs, handler, logger):
    """
    Update each pileup document in MSPileup
    :param mspileupUrl: string with the MSPileup url
    :param puDocs: list with pileup documents
    :param handler: HTTP handler for url opener
    :param logger: logger object
    :return: None
    """
    url = mspileupUrl + "/ms-pileup/data/pileup"
    logger.info("Starting to update pileup documents against backend: %s", url)
    for doc in puDocs:
        logger.info("Updating pileup document: %s", doc.get("pileupName"))
        jdoc = json.dumps(doc)
        data = bytes(jdoc, 'utf-8')
        req = urllib.request.Request(url, data=data, method="PUT")
        req.add_header('Accept', 'application/json')
        req.add_header('Content-Type', 'application/json')
        req.add_header('User-agent', userAgent())

        opener = urllib.request.build_opener(handler)
        urllib.request.install_opener(opener)
        with urllib.request.urlopen(req) as resp:
            sdata = resp.read().decode('utf8')
            data = json.loads(sdata)
            logger.debug("Response: %s", data)
            if data and data.get("result", []):
                msg = f"Failed to inject pileup document for {doc['pileupName']}. Error: {data}"
                logger.critical(msg)


def main():
    """Executes everything"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    logger = logging.getLogger('updatePileupObjects')
    logger.setLevel(logging.INFO)
    logging.basicConfig()

    # setup proxy/cert
    proxy = os.getenv('X509_USER_PROXY', '')
    if proxy:
        cert = proxy
        key = proxy
    else:
        cert = os.getenv('X509_USER_CERT', '')
        key = os.getenv('X509_USER_KEY', '')
        if not cert or not key:
            defaultCert = os.path.join(os.getenv('HOME'), '.globus/usercert.pem')
            defaultKey = os.path.join(os.getenv('HOME'), '.globus/userkey.pem')
            if os.path.isfile(defaultCert) and os.path.isfile(defaultKey):
                cert = defaultCert
                key = defaultKey

    # final check, the cert should be already set either by proxy or itself
    if not cert:
        logger.error("You need to define the X509_USER_CERT/X509_USER_key or X509_USER_PROXY env variables")
        sys.exit(1)
    logger.info("Using cert=%s key=%s", cert, key)

    # initialize HTTPS client handler
    handler = HTTPSClientAuthHandler(key, cert)

    with open(opts.fin, 'r', encoding='utf-8') as istream:
        puOverride = json.load(istream)
    logger.info("Pileup override file: %s has the following content: %s", opts.fin, puOverride)

    puDocs = getPileupDocs(opts.url, handler, logger)
    logger.info("Found %d documents in MSPileup", len(puDocs))

    # update the documents with the override content
    for doc in puDocs:
        doc.update(puOverride)
        logger.info("New pileup document is: %s", doc)

    if opts.dryrun:
        logger.info("documents are not written due to --dry-run option\n")
    else:
        # finally, update the pileup documents in the database
        writePileupDocs(opts.url, puDocs, handler, logger)
        logger.info("documents are uploaded to MSPileup\n")


if __name__ == '__main__':
    main()
