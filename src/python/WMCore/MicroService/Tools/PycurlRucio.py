#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module based on the pycurl_manager module, implementing a few
functions to call to the Rucio RESTful APIs, leveraging the
pycurl concurrency.
"""
from __future__ import print_function, division, absolute_import

from builtins import str
from future.utils import viewitems

from future import standard_library
standard_library.install_aliases()

import datetime
import json
import logging
import re

from urllib.parse import quote, unquote

from Utils.CertTools import cert, ckey
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.pycurl_manager import getdata as multi_getdata

### Amount of days that we wait for stuck rules to be sorted
### After that, the rule is not considered and a new rule is created
STUCK_LIMIT = 7  # 7 days


def parseNewLineJson(stream):
    """
    Parse newline delimited json streaming data
    """
    for line in stream.split("\n"):
        if line:
            yield json.loads(line)


def stringDateToEpoch(strDate):
    """
    Given a date/time in the format of:
        'Thu, 29 Apr 2021 13:15:42 UTC'
    it returns an integer with the equivalent EPOCH time
    :param strDate: a string with the date and time
    :return: the equivalent EPOCH time (integer)
    """
    timestamp = datetime.datetime.strptime(strDate, "%a, %d %b %Y %H:%M:%S %Z")
    return int(timestamp.strftime('%s'))


def getRucioToken(rucioAuthUrl, rucioAcct):
    """
    Provided a Rucio account, fetch a token from the authentication server
    :param rucioAuthUrl: url to the rucio authentication server
    :param rucioAcct: rucio account to be used
    :return: an integer with the expiration time in EPOCH
    """
    params = {}
    headers = {"X-Rucio-Account": rucioAcct}

    url = '%s/auth/x509' % rucioAuthUrl
    logging.info("Requesting a token to Rucio for account: %s, against url: %s", rucioAcct, rucioAuthUrl)
    mgr = RequestHandler()
    res = mgr.getheader(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    if res.getReason() == "OK":
        userToken = res.getHeaderKey('X-Rucio-Auth-Token')
        tokenExpiration = res.getHeaderKey('X-Rucio-Auth-Token-Expires')
        logging.info("Retrieved Rucio token valid until: %s", tokenExpiration)
        # convert the human readable expiration time to EPOCH time
        tokenExpiration = stringDateToEpoch(tokenExpiration)
        return userToken, tokenExpiration

    raise RuntimeError("Failed to acquire a Rucio token. Error: {}".format(res.getReason()))


def renewRucioToken(rucioAuthUrl, userToken):
    """
    Provided a user Rucio token, check it's lifetime and extend it by another hour
    :param rucioAuthUrl: url to the rucio authentication server
    :param rucioAcct: rucio account to be used
    :return: a datetime.datetime object with the new token lifetime
    """
    params = {}
    headers = {"X-Rucio-Auth-Token": userToken}

    url = '%s/auth/validate' % rucioAuthUrl
    logging.info("Renewing the Rucio token...")
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    try:
        newExpiration = eval(res)['lifetime']
    except Exception as exc:
        raise RuntimeError("Failed to renew Rucio token. Response: {} Error: {}".format(res, str(exc)))
    return newExpiration


def getPileupContainerSizesRucio(containers, rucioUrl, rucioToken, scope="cms"):
    """
    Given a list of containers, find their total size in Rucio
    :param containers: list of container names
    :param rucioUrl: a string with the Rucio URL
    :param rucioToken: a string with the user rucio token
    :param scope: a string with the Rucio scope of our data
    :return: a flat dictionary of container and their respective sizes
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    NOTE: Rucio version of getPileupDatasetSizes()
    """
    sizeByDset = {}
    if not containers:
        return sizeByDset

    headers = {"X-Rucio-Auth-Token": rucioToken}

    urls = ['{}/dids/{}/{}?dynamic=anything'.format(rucioUrl, scope, cont) for cont in containers]
    logging.info("Executing %d requests against Rucio for the container size", len(urls))
    data = multi_getdata(urls, ckey(), cert(), headers=headers)

    for row in data:
        container = row['url'].split('/dids/{}/'.format(scope))[1]
        container = container.replace("?dynamic=anything", "")
        if row['data'] is None:
            msg = "Failure in getPileupContainerSizesRucio for container {}. Response: {}".format(container, row)
            logging.error(msg)
            sizeByDset.setdefault(container, None)
            continue
        response = json.loads(row['data'])
        try:
            sizeByDset.setdefault(container, response['bytes'])
        except KeyError:
            msg = "getPileupContainerSizesRucio function did not return a valid response for container: %s. Error: %s"
            logging.error(msg, container, response)
            sizeByDset.setdefault(container, None)
            continue
    return sizeByDset


def listReplicationRules(containers, rucioAccount, grouping,
                         rucioUrl, rucioToken, scope="cms"):
    """
    List all the replication rules for the input filters provided.
    It builds a dictionary of container name and the locations where
    they have a rule locking data on, with some additional rule state
    logic in the code.
    :param containers: list of container names
    :param rucioAccount: string with the rucio account
    :param grouping: rule grouping string, only "A" or "D" are allowed
    :param rucioUrl: string with the Rucio url
    :param rucioToken: string with the Rucio token
    :param scope: string with the data scope
    :return: a flat dictionary key'ed by the container name, with a list of RSE
      expressions that still need to be resolved
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    NOTE-2: Available rule states can be found at:
    https://github.com/rucio/rucio/blob/16f39dffa1608caa0a1af8bbc0fcff2965dccc50/lib/rucio/db/sqla/constants.py#L180
    """
    locationByContainer = {}
    if not containers:
        return locationByContainer
    if grouping not in ["A", "D"]:
        raise RuntimeError("Replication rule grouping value provided ({}) is not allowed!".format(grouping))

    headers = {"X-Rucio-Auth-Token": rucioToken}
    urls = []
    for cont in containers:
        urls.append('{}/rules/?scope={}&account={}&grouping={}&name={}'.format(rucioUrl, scope, rucioAccount,
                                                                               grouping, quote(cont, safe="")))
    logging.info("Executing %d requests against Rucio to list replication rules", len(urls))
    data = multi_getdata(urls, ckey(), cert(), headers=headers)

    for row in data:
        container = unquote(row['url'].split("name=")[1])
        if "200 OK" not in row['headers']:
            msg = "Failure in listReplicationRules for container {}. Response: {}".format(container, row)
            logging.error(msg)
            locationByContainer.setdefault(container, None)
            continue
        try:
            locationByContainer.setdefault(container, [])
            for item in parseNewLineJson(row['data']):
                if item['state'] in ["U", "SUSPENDED", "R", "REPLICATING", "I", "INJECT"]:
                    msg = "Container %s has a rule ID %s in state %s. Will try to create a new rule."
                    logging.warning(msg, container, item['id'], item['state'])
                    continue
                elif item['state'] in ["S", "STUCK"]:
                    if item['error'] == 'NO_SOURCES:NO_SOURCES':
                        msg = "Container {} has a STUCK rule with NO_SOURCES.".format(container)
                        msg += " Data could be lost forever... Rule info is: {}".format(item)
                        logging.warning(msg)
                        continue

                    # then calculate for how long it's been stuck
                    utcTimeNow = int(datetime.datetime.utcnow().strftime('%s'))
                    if item['stuck_at']:
                        stuckAt = stringDateToEpoch(item['stuck_at'])
                    else:
                        # consider it to be stuck since its creation
                        stuckAt = stringDateToEpoch(item['created_at'])

                    daysStuck = (utcTimeNow - stuckAt) // (24 * 60 * 60)
                    if daysStuck > STUCK_LIMIT:
                        msg = "Container {} has a STUCK rule for {} days (limit set to: {}).".format(container,
                                                                                                     daysStuck,
                                                                                                     STUCK_LIMIT)
                        msg += " Not going to use it! Rule info: {}".format(item)
                        logging.warning(msg)
                        continue
                    else:
                        msg = "Container {} has a STUCK rule for only {} days.".format(container, daysStuck)
                        msg += " Considering it for the pileup location"
                        logging.info(msg)
                else:
                    logging.info("Container %s has rule ID %s in state %s, using it.",
                                 container, item['id'], item['state'])

                ### NOTE: this is not an RSE name, but an RSE expression that still needs to be resolved
                locationByContainer[container].append(item['rse_expression'])
        except Exception as exc:
            msg = "listReplicationRules function did not return a valid response for container: %s."
            msg += "Server responded with: %s\nError: %s"
            logging.exception(msg, container, str(exc), row['data'])
            locationByContainer.setdefault(container, None)
            continue
    return locationByContainer


def getPileupSubscriptionsRucio(datasets, rucioUrl, rucioToken, scope="cms"):
    """
    Provided a list of datasets, find dataset level subscriptions where it's
    as complete as `percent_min`.
    :param datasets: list of dataset names
    :param rucioUrl: a string with the Rucio URL
    :param rucioToken: a string with the user rucio token
    :param scope: a string with the Rucio scope of our data
    :return: a dictionary of datasets and a list of their location.
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    # FIXME: we should definitely make a feature request to Rucio...
    # so much, just to get the final RSEs for a container!!!
    locationByDset = {}
    if not datasets:
        return locationByDset

    headers = {"X-Rucio-Auth-Token": rucioToken}
    # first, resolve the dataset into blocks
    blocksByDset = getContainerBlocksRucio(datasets, rucioUrl, rucioToken, scope)
    urls = []
    for _dset, blocks in viewitems(blocksByDset):
        if blocks:
            for block in blocks:
                urls.append('{}/replicas/{}/{}/datasets'.format(rucioUrl, scope, quote(block)))

    # this is going to be bloody expensive in terms of HTTP requests
    logging.info("Executing %d requests against Rucio replicas API for blocks", len(urls))
    data = multi_getdata(urls, ckey(), cert(), headers=headers)
    for row in data:
        block = row['url'].split("/{}/".format(scope))[1]
        block = unquote(re.sub("/datasets$", "", block, 1))
        container = block.split("#")[0]
        locationByDset.setdefault(container, set())
        if row['data'] is None:
            msg = "Failure in getPileupSubscriptionsRucio container {} and block {}.".format(container, block)
            msg += " Response: {}".format(row)
            logging.error(msg)

            locationByDset[container] = None
            continue
        if locationByDset[container] is None:
            # then one of the block requests failed, skip the whole dataset
            continue
        thisBlockRSEs = set()
        for item in parseNewLineJson(row['data']):
            if item['state'] == "AVAILABLE":
                thisBlockRSEs.add(item["rse"])
        logging.info("Block: %s is available at: %s", block, thisBlockRSEs)
        # now we have the final block location
        if not locationByDset[container]:
            # then this is the first block of this dataset
            locationByDset[container] = thisBlockRSEs
        else:
            # otherwise, make an intersection of them
            locationByDset[container] = locationByDset[container] & thisBlockRSEs
    return locationByDset


def getBlocksAndSizeRucio(containers, rucioUrl, rucioToken, scope="cms"):
    """
    Given a list of containers, find all their correspondent blocks and their sizes.
    :param containers: list of container names
    :param rucioUrl: a string with the Rucio URL
    :param rucioToken: a string with the user rucio token
    :param scope: a string with the Rucio scope of our data
    :return: a dictionary in the form of:
    {"dataset":
        {"block":
            {"blockSize": 111, "locations": ["x", "y"]}
        }
    }
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    NOTE2: meant to return an output similar to Common.getBlockReplicasAndSize
    """
    contBlockSize = {}
    if not containers:
        return contBlockSize

    headers = {"X-Rucio-Auth-Token": rucioToken}
    urls = []
    for cont in containers:
        urls.append('{}/dids/{}/dids/search?type=dataset&long=True&name={}'.format(rucioUrl, scope, quote(cont + "#*")))
    logging.info("Executing %d requests against Rucio DIDs search API for containers", len(urls))
    data = multi_getdata(urls, ckey(), cert(), headers=headers)
    for row in data:
        container = row['url'].split("name=")[1]
        container = unquote(container).replace("#*", "")
        contBlockSize.setdefault(container, {})
        if row['data'] in [None, ""]:
            msg = "Failure in getBlocksAndSizeRucio function for container {}. Response: {}".format(container, row)
            logging.error(msg)
            contBlockSize[container] = None
            continue

        for item in parseNewLineJson(row['data']):
            # NOTE: we do not care about primary block location in Rucio
            contBlockSize[container][item['name']] = {"blockSize": item['bytes'], "locations": []}
    return contBlockSize


### NOTE: likely not going to be used for a while
def getContainerBlocksRucio(containers, rucioUrl, rucioToken, scope="cms"):
    """
    Provided a list of containers, find all their blocks.
    :param containers: list of container names
    :param rucioUrl: a string with the Rucio URL
    :param rucioToken: a string with the user rucio token
    :param scope: a string with the Rucio scope of our data
    :return: a dictionary key'ed by the datasets with a list of blocks.
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    blocksByDset = {}
    if not containers:
        return blocksByDset

    headers = {"X-Rucio-Auth-Token": rucioToken}
    urls = ['{}/dids/{}/{}/dids'.format(rucioUrl, scope, cont) for cont in containers]
    logging.info("Executing %d requests against Rucio DIDs API for blocks in containers", len(urls))
    data = multi_getdata(urls, ckey(), cert(), headers=headers)
    for row in data:
        container = row['url'].split("/{}/".format(scope))[1]
        container = re.sub("/dids$", "", container, 1)
        if not row['data']:
            logging.warning("Dataset: %s has no blocks in Rucio", container)
        blocksByDset.setdefault(container, [])
        for item in parseNewLineJson(row['data']):
            blocksByDset[container].append(item["name"])
    return blocksByDset


### NOTE: likely not going to be used for a while
def getBlockReplicasAndSizeRucio(datasets, rucioUrl, rucioToken, scope="cms"):
    """
    Given a list of datasets, find all their blocks with replicas
    available.
    :param datasets: list of dataset names
    :param rucioUrl: a string with the Rucio URL
    :param rucioToken: a string with the user rucio token
    :param scope: a string with the Rucio scope of our data
    :return: a dictionary in the form of:
    {"dataset":
        {"block":
            {"blockSize": 111, "locations": ["x", "y"]}
        }
    }
    NOTE: Value `None` is returned in case the data-service failed to serve a given request.
    """
    dsetBlockSize = {}
    if not datasets:
        return dsetBlockSize

    headers = {"X-Rucio-Auth-Token": rucioToken}
    # first, figure out their block names
    blocksByDset = getContainerBlocksRucio(datasets, rucioUrl, rucioToken, scope=scope)
    urls = []
    for _dset, blocks in viewitems(blocksByDset):
        for block in blocks:
            urls.append('{}/replicas/{}/{}/datasets'.format(rucioUrl, scope, quote(block)))

    # next, query the replicas API for the block location
    # this is going to be bloody expensive in terms of HTTP requests
    logging.info("Executing %d requests against Rucio replicas API for blocks", len(urls))
    data = multi_getdata(urls, ckey(), cert(), headers=headers)
    for row in data:
        block = row['url'].split("/{}/".format(scope))[1]
        block = unquote(re.sub("/datasets$", "", block, 1))
        container = block.split("#")[0]
        dsetBlockSize.setdefault(container, dict())
        if row['data'] is None:
            msg = "Failure in getBlockReplicasAndSizeRucio for container {} and block {}.".format(container, block)
            msg += " Response: {}".format(row)
            logging.error(msg)

            dsetBlockSize[container] = None
            continue
        if dsetBlockSize[container] is None:
            # then one of the block requests failed, skip the whole dataset
            continue

        thisBlockRSEs = []
        blockBytes = 0
        for item in parseNewLineJson(row['data']):
            blockBytes = item['bytes']
            if item['state'] == "AVAILABLE":
                thisBlockRSEs.append(item["rse"])
        # now we have the final block location
        if not blockBytes and not thisBlockRSEs:
            logging.warning("Block: %s has no replicas and no size", block)
        else:
            dsetBlockSize[container][block] = {"locations": thisBlockRSEs, "blockSize": blockBytes}
    return dsetBlockSize
