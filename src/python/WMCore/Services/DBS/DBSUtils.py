#!/usr/bin/env python
"""
_DBSUtils_

set of common utilities for DBS3Reader

"""
import json
import urllib
from urllib.parse import urlparse, parse_qs, quote_plus
from collections import defaultdict

from Utils.CertTools import cert, ckey
from dbs.apis.dbsClient import aggFileLumis, aggFileParents
from WMCore.Services.pycurl_manager import getdata as multi_getdata
from Utils.PortForward import PortForward


def dbsListFileParents(dbsUrl, blocks):
    """
    Concurrent counter part of DBS listFileParents API

    :param dbsUrl: DBS URL
    :param blocks: list of blocks
    :return: list of file parents
    """
    urls = ['%s/fileparents?block_name=%s' % (dbsUrl, quote_plus(b)) for b in blocks]
    func = aggFileParents
    uKey = 'block_name'
    return getUrls(urls, func, uKey)


def dbsListFileLumis(dbsUrl, blocks):
    """
    Concurrent counter part of DBS listFileLumis API

    :param dbsUrl: DBS URL
    :param blocks: list of blocks
    :return: list of file lumis
    """
    urls = ['%s/filelumis?block_name=%s' % (dbsUrl, quote_plus(b)) for b in blocks]
    func = aggFileLumis
    uKey = 'block_name'
    return getUrls(urls, func, uKey)


def dbsBlockOrigin(dbsUrl, blocks):
    """
    Concurrent counter part of DBS files API

    :param dbsUrl: DBS URL
    :param blocks: list of blocks
    :return: list of block origins for a given parent lfns
    """
    urls = ['%s/blockorigin?block_name=%s' % (dbsUrl, quote_plus(b)) for b in blocks]
    func = None
    uKey = 'block_name'
    return getUrls(urls, func, uKey)


def dbsParentFilesGivenParentDataset(dbsUrl, parentDataset, fInfo):
    """
    Obtain parent files for given fileInfo object

    :param dbsUrl: DBS URL
    :param parentDataset: parent dataset name
    :param fInfo: file info object
    :return: list of parent files for given file info object
    """
    portForwarder = PortForward(8443)
    urls = []
    for fileInfo in fInfo:
        run = fileInfo['run_num']
        lumis = urllib.parse.quote_plus(str(fileInfo['lumi_section_num']))
        url = f'{dbsUrl}/files?dataset={parentDataset}&run_num={run}&lumi_list={lumis}'
        urls.append(portForwarder(url))
    func = None
    uKey = None
    rdict = getUrls(urls, func, uKey)
    parentFiles = defaultdict(set)
    for fileInfo in fInfo:
        run = fileInfo['run_num']
        lumis = urllib.parse.quote_plus(str(fileInfo['lumi_section_num']))
        url = f'{dbsUrl}/files?dataset={parentDataset}&run_num={run}&lumi_list={lumis}'
        url = portForwarder(url)
        if url in rdict:
            pFileList = rdict[url]
            pFiles = {x['logical_file_name'] for x in pFileList}
            parentFiles[fileInfo['logical_file_name']] = \
                parentFiles[fileInfo['logical_file_name']].union(pFiles)
    return parentFiles


def getUrls(urls, aggFunc, uKey=None):
    """
    Perform parallel DBS calls for given set of urls and apply given aggregation
    function to the results.

    :param urls: list of DBS urls to call
    :param aggFunc: aggregation function
    :param uKey: url parameter to use for final dictionary
    :return: dictionary of resuls where keys are urls and values are obtained results
    """
    data = multi_getdata(urls, ckey(), cert())

    rdict = {}
    for row in data:
        url = row['url']
        code = int(row.get('code', 200))
        error = row.get('error')
        if code != 200:
            msg = f"Fail to query {url}. Error: {code} {error}"
            raise RuntimeError(msg)
        if uKey:
            key = urlParams(url).get(uKey)
        else:
            key = url
        data = row.get('data', [])
        res = json.loads(data)
        if aggFunc:
            rdict[key] = aggFunc(res)
        else:
            rdict[key] = res
    return rdict


def urlParams(url):
    """
    Return dictionary of URL parameters

    :param url: URL link
    :return: dictionary of URL parameters
    """
    parsedUrl = urlparse(url)
    rdict = parse_qs(parsedUrl.query)
    for key, vals in rdict.items():
        if len(vals) == 1:
            rdict[key] = vals[0]
    return rdict
