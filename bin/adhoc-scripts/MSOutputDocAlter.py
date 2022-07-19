#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to alter MSOutput MongoDB documents based on a set of parameters given at runtime
Usage:
      source /data/srv/current/apps/reqmgr2ms/etc/profile.d/init.sh
      python3 MSOutputDocAlter.py [--options]
"""
import sys
import os
import logging
import json
import re
import ast
import threading
import logging

from pprint import pprint, pformat

from pymongo import IndexModel, ReturnDocument, errors

from WMCore.Database.MongoDB import MongoDB
from WMCore.MicroService.MSOutput.MSOutputTemplate import MSOutputTemplate
from WMCore.Services.pycurl_manager import RequestHandler
from Utils.TwPrint import twFormat
from Utils.CertTools import ckey, cert
from WMCore.Configuration import Configuration, loadConfigurationFile

import argparse


def parseArgs():
    """
    Generic Argument Parser function
    """
    parser = argparse.ArgumentParser(
        prog='MSOutputDocAlter',
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__)

    parser.add_argument('-c', '--config', required=True,
                        help="""\
                        The path to MSOUPUT_CONFIG to be used, e.g.
                        for production: /data/srv/current/config/reqmgr2ms/config-output.py
                        """)

    parser.add_argument('-f', '--file', default=None,
                        help="""\
                        File containing list of RucioRuleIDs DIDs and RSEs.
                        """)

    parser.add_argument('-d', '--dataset', default=None,
                        help="""\
                        The dataset name for the documents to be altered.
                        """)
    parser.add_argument('-o', '--oldRuleId', default=None,
                        help="""\
                        The old Rucio Rule ID, which is to be substituted with the new one
                        """)
    parser.add_argument('-n', '--newRuleId', default=None,
                        help="""\
                        The new Rucio Rule ID, which is to substitute the old one.
                        """)
    parser.add_argument('-r', '--newRSE', default=None,
                        help="""\
                        The new RSE, which is to substitute the old one.
                        """)
    parser.add_argument('--debug', action='store_true', default=False,
                        help="""\
                        Set logging to debug mode.""")
    args = parser.parse_args()
    return args


def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    logger = logging.getLogger()
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    if logger.handlers:
        logger.handlers.clear()
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger


def getWflowByOutputDataset(outputDataset, msConfig):
    """
    Fetches workflow information from the Requestmanager REST interface by output dtaset
    :param  outputDataset:   Output dataset
    :return:                 The workflow object
    """
    headers = {'Accept': 'application/json'}
    params = {}
    maskKeys=['RequestName', 'RequestStatus', 'OutputDatasets', 'RequestType']
    mask = ""
    for maskKey in maskKeys:
        mask += "&mask=%s" % maskKey
    url = '%s/data/request?outputdataset=%s&%s' % (msConfig['reqmgr2Url'], outputDataset, mask)

    curlMgr = RequestHandler()
    reqNames = []
    # try:
    #     res = curlMgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    #     data = json.loads(res)['result']
    #     reqNames = [req['RequestName'] for req in data]
    # except Exception as ex:
    #     msg = "General exception while fetching workflownames for OutputDataset %s. "
    #     msg += "Error: %s"
    #     print(msg % (outputDataset, str(ex)))
    res = curlMgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
    res = json.loads(res)
    # print('result: %s' % pformat(res))
    if len(res['result']) > 0:
        data = res['result'][0]
    else:
        data = {}
    return data

def getDocsFromMongo(mQuery, dbColl, limit=1000):
    """
    Reads documents from MongoDB and convert them to an MSOutputTemplate
    object. Limit can be provided to control the amount of records to be
    returned:
    :param mQuery: dictionary with the Mongo query to be executed
    :param dbColl: connection object to the database/collection
    :param limit: integer with the amount of documents meant to be returned
    :return: it yields an MSOutputTemplate object
    """

    counter = 0
    for mongoDoc in dbColl.find(mQuery):
        if counter >= limit:
            return
        try:
            msOutDoc = MSOutputTemplate(mongoDoc, producerDoc=False)
            counter += 1
            yield msOutDoc
        except Exception as ex:
            msg = "Failed to create MSOutputTemplate object from mongo record: {}.".format(mongoDoc)
            msg += " Error message was: {}".format(str(ex))
            print(msg)
            raise ex

def promptQuery(msg, default=False):
    """
    A simple function to query for yes/no type question in the command prompt.
    :param msg:     The message/question to be printed in the prompt.
    :param default: The default value to be returned if an empty answer was given
    :return:        Bool depending on the answer.
    """
    trueMap = {"yes": True, "y": True, "no": False, "n": False}
    while True:
        sys.stdout.write(msg)
        response = input().lower()
        if default is not None and response == "":
            return default
        elif response in trueMap:
            return trueMap[response]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


def main():

    """
    An Utility to Alter MSoutput MongoDB documents.
    """

    # dataset = args.dataset
    ruleInfoList = []

    if args.file:
        with open(args.file) as fd:
            for line in fd.readlines():
                line = line.split()
                if line:
                    ruleInfo = {'newRuleID' : line[0],
                                'dataset': line[1],
                                'oldRuleID': line[2],
                                'newRSE': line[3]}
                    ruleInfoList.append(ruleInfo)
    else:
        ruleInfo = {'newRuleID' : args.newRuleId,
                    'dataset': args.dataset,
                    'oldRuleID': args.oldRuleId,
                    'newRSE': args.newRSE}
        ruleInfoList.append(ruleInfo)

    for ruleInfo in ruleInfoList:
        requests = getWflowByOutputDataset(ruleInfo['dataset'], msConfig)
        msg = "\n\nDataset: %s\n"
        if not requests:
            msg += "Found NO workflows\n"
            logger.info(msg, ruleInfo['dataset'])
            continue
        msg += "Found the following workflows: %s\n"
        logger.info(msg, ruleInfo['dataset'], pformat(requests))

        for requestName,request in requests.items():
            if request['RequestType'] == 'Resubmission':
                logger.info("Skipping Resubmission request: %s", requestName)
                continue
            mQuery = {'RequestName': request['RequestName']}
            for docOut in getDocsFromMongo(mQuery, msOutColl, msConfig['limitRequestsPerCycle']):
                logger.info(pformat(docOut))
                docUpdated = False
                for outMapEntry in docOut['OutputMap']:
                    if outMapEntry['Dataset'] == ruleInfo['dataset']:

                        msg = "\n\n-----------------------------------------------------\n"
                        msg += "\nFound a matching outMapEntry for \n"
                        msg += "\ndataset: \t%s; \nworkflow: \t%s; \nworkflowStatus: %s; \noutMapEntry: \n%s;\n"
                        msg += "\ncurrent RuleID: %s,\told RuleID: %s,\tnew RuleID: %s"
                        msg += "\ncurrent RSE:    %s,\t\t\told RSE:    %s,\t\t\tnew RSE:    %s\n"
                        msg += "\n\n-----------------------------------------------------\n"
                        logger.info(msg,
                                    ruleInfo['dataset'], requestName, request['RequestStatus'], pformat(outMapEntry),
                                    outMapEntry['TapeRuleID'], ruleInfo['oldRuleID'], ruleInfo['newRuleID'],
                                    outMapEntry['TapeDestination'], outMapEntry['TapeDestination'], ruleInfo['newRSE'])

                        swap = promptQuery('Swap the Rules? [n/No]:')
                        if swap:
                            outMapEntry['TapeRuleID'] = ruleInfo['newRuleID']
                            outMapEntry['TapeDestination'] = ruleInfo['newRSE']
                            docOut.updateTime()
                            docUpdated = True
                if docUpdated:
                    logger.info("Trying to upload document: %s", pformat(docOut))
                    try:
                        msOutColl.insert_one(docOut)
                    except errors.DuplicateKeyError:
                        docOut = msOutColl.find_one_and_update({'_id': docOut['_id']},
                                                               {'$set':docOut},
                                                               return_document=ReturnDocument.AFTER)


if __name__ == '__main__':

    args = parseArgs()
    if args.debug:
        logger = loggerSetup(logging.DEBUG)
    else:
        logger = loggerSetup()

    logger.info("Loading configFile: %s", args.config)
    config = loadConfigurationFile(args.config)
    msConfig = config.section_('views').section_('data').dictionary_()

    # add the database name here if needed (it is hardcoded in the msoutput code):
    msConfig['mongoDB'] = 'msOutDB'


    msOutIndex = IndexModel('RequestName', unique=True)
    msOutDBConfig = {
        'database': msConfig['mongoDB'],
        'server': msConfig['mongoDBUrl'],
        'port': msConfig['mongoDBPort'],
        'logger': logger,
        'create': False,
        'collections': [
            ('msOutRelValColl', msOutIndex),
            ('msOutNonRelValColl', msOutIndex)]}

    mongoClt = MongoDB(**msOutDBConfig)
    msOutDB = getattr(mongoClt, msConfig['mongoDB'])
    msOutColl = msOutDB['msOutNonRelValColl']

    main()
