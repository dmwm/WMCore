#!/usr/bin/env python

from __future__ import print_function, division

import httplib
import json
import sys
import urllib
import urllib2
from argparse import ArgumentParser
from urllib2 import HTTPError, URLError

main_url = "https://cmsweb.cern.ch"
phedex_url = main_url + "/phedex/datasvc/json/prod/"
dbs_url = main_url + "/dbs/prod/global/DBSReader/"


class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


def get_content(url, cert, params=None):
    opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, cert))
    try:
        if params:
            response = opener.open(url, params)
            output = response.read()
        else:
            response = opener.open(url)
            output = response.read()
    except HTTPError as e:
        print('The server couldn\'t fulfill the request. Erro code: ', e.code)
        sys.exit(1)
    except URLError as e:
        print('Failed to reach server. Reason:', e.reason)
        sys.exit(1)
    return output


def phedex_info(dataset, cert):
    """
    Query blockreplicas PhEDEx API to retrieve detailed information
    for a specific dataset
    """
    api_url = phedex_url + "blockreplicas" + "?" + urllib.urlencode([('dataset', dataset)])
    phedex_summary = json.loads(get_content(api_url, cert))
    return phedex_summary


def dbs_info(dataset, cert):
    """
    Queries 2 DBS APIs to get both summary and detailed information
    """
    dbs_out = {}
    api_url = dbs_url + "blocksummaries" + "?" + urllib.urlencode({'dataset': dataset})
    dbs_out['blocksummaries'] = json.loads(get_content(api_url, cert))
    api_url = dbs_url + "files" + "?" + urllib.urlencode({'dataset': dataset}) + "&detail=1"
    dbs_out['files'] = json.loads(get_content(api_url, cert))
    return dbs_out


def main(argv=None):
    """
    Receive either a dataset name or a logical file name
    and proxy location. Then it queries the following data
    services:
     - phedex : gets number of files
     - dbs    : gets the number of valid, invalid and total files

    It returns the number of files for this dataset/lfn available
    in PhEDEx and DBS
    """
    usage = "usage: %prog -d dataset_name -p proxy_location"
    parser = ArgumentParser(usage=usage)
    parser.add_argument('-d', '--dataset', help='Dataset name', dest='dataset')
    parser.add_argument('-l', '--lfn', help='Logical file name', dest='lfn')
    parser.add_argument('-p', '--proxy', help='Path to your proxy or cert location', dest='proxy')
    options = parser.parse_args()
    if not ((options.dataset or options.lfn) and options.proxy):
        parser.error("Please supply either dataset name or file name \
                      and certificate location")
        sys.exit(1)
    if options.dataset:
        dataset = options.dataset
    if options.lfn:
        lfn = options.lfn
        lfnAux = lfn.split('/')
        dataset = '/' + lfnAux[4] + '/' + lfnAux[3] + '-' + lfnAux[6] + '/' + lfnAux[5]
    cert = options.proxy

    print("Dataset: %s" % dataset)

    phedex_out = phedex_info(dataset, cert)
    dbs_out = dbs_info(dataset, cert)
    phedex_files = 0
    phedex_blocks = {}
    for item in phedex_out["phedex"]["block"]:
        phedex_files += item['files']
        phedex_blocks.setdefault(item['name'], item['files'])

    dbs_files = dbs_out['blocksummaries'][0]['num_file']
    dbs_blocks = {}
    dbs_file_valid = 0
    dbs_file_invalid = 0
    for item in dbs_out['files']:
        dbs_blocks.setdefault(item['block_name'], 0)
        dbs_blocks[item['block_name']] += 1
        if item['is_file_valid']:
            dbs_file_valid += 1
        else:
            dbs_file_invalid += 1

    print("Phedex file count : ", phedex_files)
    print("DBS file count    : ", dbs_files)
    print(" - valid files    : ", dbs_file_valid)
    print(" - invalid files  : ", dbs_file_invalid)
    print(" - valid+invalid  : ", (dbs_file_valid + dbs_file_invalid))
    print("Blocks in PhEDEx but not in DBS: ", set(phedex_blocks.keys()) - set(dbs_blocks.keys()))
    print("Blocks in DBS but not in PhEDEx: ", set(dbs_blocks.keys()) - set(phedex_blocks.keys()))

    for blockname in phedex_blocks:
        if phedex_blocks[blockname] != dbs_blocks.get(blockname):
            print("Block with file mismatch: %s" % blockname)
            print("\tPhEDEx: %s\t\tDBS: %s" % (phedex_blocks.get(blockname), dbs_blocks.get(blockname)))


if __name__ == "__main__":
    sys.exit(main())
