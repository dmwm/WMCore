#!/usr/bin/env python
"""
File       : DBSConcurrency.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: dedicated module to holds DBS related functions executed
concurrent calls to DBS APIs.
"""

import json
import urllib
from WMCore.Services.pycurl_manager import getdata as multi_getdata


def getBlockInfo4PU(blockNames, dbsUrl, ckey, cert):
    """
    Fetch block information details, file list and number of events, from DBS
    server. Here we use concrete set of parameters for DBS to use in this case, i.e.
    we must look-up only valid files and get full details from the DBS API (in order
    to get number of events).
    :param blockNames: list of block names
    :param dbsUrl: dbs URL
    :param ckey: user keyfile
    :param cert: user certificate
    :return: dictionary of {block: {"FileList": list of strings, "NumberOfEvents": integer}, ...}
    """
    urls = []
    for blk in blockNames:
        # need to encode block name properly
        block = urllib.parse.quote_plus(blk)
        url = f"{dbsUrl}/files?detail=true&validFileOnly=1&block_name={block}"
        urls.append(url)
    # place concurrent calls to DBS, please note that multi_getdata is generator, therefore
    # it does not put DBS results into the memory until this generator is iterated
    results = multi_getdata(urls, ckey, cert)
    # parse output of getdata in some form
    blockInfo = {}
    for row in results:
        blk = row['url'].split('block_name=')[-1]
        block = urllib.parse.unquote_plus(blk)
        data = json.loads(row['data'])
        files = [r['logical_file_name'] for r in data]
        nevents = sum([r['event_count'] for r in data])
        blockInfo[block] = {'FileList': files, 'NumberOfEvents': nevents}
    return blockInfo
