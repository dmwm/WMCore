#!/usr/bin/env python
"""
The WorkflowUpdater poller component.
Among the actions performed by this component, we can list:
* find active workflows in the agent
* filter those that require pileup dataset
* find out the current location for the pileup datasets
* get a list of blocks available and locked by WM
* match those blocks with the current pileup config json file. In other words,
  blocks that are no longer locked and/or available need to be removed from the
  json file.
* update this json in the workflow sandbox
"""

import json
import logging
import os
import shutil
import tarfile
import tempfile
import threading

from Utils.CertTools import cert, ckey
from Utils.IteratorTools import flattenList
from Utils.FileTools import tarMode, findFiles
from Utils.Timers import timeFunction, CodeTimer
from WMCore.Services.MSUtils.MSUtils import getPileupDocs
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.WMException import WMException
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.DBS.DBSConcurrency import getBlockInfo4PU
from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException


def findJsonSandboxFiles(tfile):
    """
    Find location of sandbox JSON files
    :param tfile: sandbox tar file
    :return: list of file names
    """
    files = []
    mode = tarMode(tfile, 'r')
    with tarfile.open(tfile, mode, encoding='utf-8') as tar:
        for tarInfo in tar.getmembers():
            if tarInfo.name.endswith("pileupconf.json"):
                files.append(tarInfo.name)
    return files


def findPUName(puJsonContent):
    """
    Finds pileup name in pileup conf json file
    :param puJsonContent: content of pileupconf.json file
    :return: string (pileup name)
    """
    jsonPUName = ""
    for _puType, blocks in puJsonContent.items():
        for blockName, _content in blocks.items():
            jsonPUName = blockName.split("#")[0]
            return jsonPUName
    return jsonPUName


def extractPileupconf(tfile, fname):
    """
    Extract content of given file name from sandbox tar file
    :param tfile: sandbox tar file
    :param fname: name of the file to extract
    :return: content of file
    """
    mode = tarMode(tfile, 'r')
    with tarfile.open(tfile, mode, encoding='utf-8') as tar:
        f = tar.extractfile(fname)
        data = f.read()
        # convert our data bytes into JSON object
        return json.loads(data)


def blockLocations(jdoc):
    """
    Return dict block names and their location from provided json sandbox file.

    json structure is
    {"<type: mc>": {"<blockName>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []},
                    "<blockName>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []}, ...}

    :param jdoc: JSON document
    :return: dict {'blockName': [rses], ...}
    """
    bdict = {}
    for rec in jdoc.values():
        for key in rec.keys():
            doc = rec[key]
            bdict[key] = doc['PhEDExNodeNames']
    return bdict


def checkChanges(jdict, msPUBlockLoc):
    """
    Compare if provided json block locations are different from block locations presented
    in pileup configuration JSON file.

    :param jdict: block structure from pileupconf.json file, e.g. {'block': [rses], ...}
    :param msPUBlockLoc: block location dict obtained MSPileup service, e.g. {'block': [rses], ....}
    :return: boolean (i.e. if there are changes or not)
    """
    if sorted(jdict.keys()) != sorted(msPUBlockLoc.keys()):
        return True
    for block, msRSEs in msPUBlockLoc.items():
        if block in jdict:
            # first use-case: check for RSE match
            jsonRSEs = jdict[block]
            if sorted(msRSEs) != sorted(jsonRSEs):
                return True
        else:
            # second use-case: MSPileup has a block which is not in configuration json
            return True
    return False


def updateBlockInfo(jdoc, msPUBlockLoc, dbsUrl, logger):
    """
    Update block information within sandbox json pileup conf file using MSPileup block location info.
    The main logic is the following:
    - compare json conf blocks with those found in msPUBlockLoc
      - if block is found in msPUBlockLog we keep it and update its rses from ones found in msPUBlockLoc entry
      - if block does not exists in msPUBLockLoc we discard
    - if block from msPUBLockLoc is not found in json conf block list
      - we add this block to json conf block list and we need to have another github issue
      how to find FileList and "NumberOfEvents"

    NOTE: this function may consume lots of memory due to jdoc structure which
    we need to parse and create new dict since we can't discard keys in place
    of jdoc internal dictionary, i.e.

    jdoc structure is
    {"<type: mc>": {"<block>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []},
                    "<block>": {"FileList" [], "NumberOfEvents":1, "PhEDExNodeNames": []}, ...}
    and we need to discard block names. They are located in internal dictionary
    and in place pop-up of jdoc is not allowed since it will change size of internal dictionary.
    Therefore, to overcome this obstacles we need to copy relevant pileupName key:values
    into new dict structure and return new dict.

    :param jdoc: JSON sandbox dictionary
    :param msPUBlockLOck: dict of block with rses from MSPileup service, i.e. {'block': [rses], ... }
    :param dbsUrl: dbs Url
    :param logger: logger object
    :return: newly constructed dict
    """
    returnDict = {}
    blocksToUpdate = []
    for puType in jdoc.keys():
        newDict = {}
        # first use-case, check json blocks against MSPileup ones, if found update the rses
        for blockName in list(jdoc[puType].keys()):
            if blockName in msPUBlockLoc.keys():
                # update record rses from mspileup record
                record = jdoc[puType][blockName]
                logger.info("Block %s has locations %s, updating to %s from MSPileup",
                            blockName, record["PhEDExNodeNames"], msPUBlockLoc[blockName])
                record["PhEDExNodeNames"] = msPUBlockLoc[blockName]
                newDict[blockName] = record

        # second use-case: add MSPileup block record if they are not found in json configuration
        blocksToUpdate = []  # list of blocks to update with DBS information
        for blockName in msPUBlockLoc.keys():
            if blockName not in list(jdoc[puType].keys()):
                # add block record from MSPileup
                record = {blockName: {"PhEDExNodeNames": msPUBlockLoc[blockName]}}
                newDict[blockName] = record
                # keep track of blocks we need to update with DBS information
                blocksToUpdate.append(blockName)
        if newDict:
            returnDict[puType] = newDict

    # Update block records with DBS information.
    # Optimization note: this step is done outside of main loop above since
    # it requires fetching block information from DBS which may not be required
    # if we do not have such blocks.
    if len(blocksToUpdate) > 0:
        logger.info("Adding %s blocks from MSPileup which are not present in pileupconf.json",
                    len(blocksToUpdate))
        binfo = getBlockInfo4PU(blocksToUpdate, dbsUrl, ckey(), cert())
        for puType in returnDict.keys():
            for blockName in blocksToUpdate:
                # update block record in-place
                record = returnDict[puType][blockName]
                record.update(binfo[blockName])
    return returnDict


def writePileupJson(tfile, jdict, logger, dest=None):
    """
    Write pileup JSON sandbox files back to file system
    :param tfile: tar ball file name
    :param jdict: JSON sandbox dictionary in form: {"path-to-json1": jdoc1, "path-to-json2": jdoc2}
    :param dest: optional destination parameter to write final tar ball (use for unit tests)
    :return: nothing
    """
    bname = os.path.basename(tfile)
    dname = os.path.dirname(tfile)
    ofile = f"{dname}/new-{bname}"
    with tempfile.TemporaryDirectory() as tmpDir:
        # extract tar ball content into temporary directory
        with tarfile.open(tfile, tarMode(tfile, 'r'), encoding='utf-8') as tar:
            tar.extractall(path=tmpDir)
        # overwrite json sanbox file in temporary directory
        for jname in jdict:
            fname = os.path.join(tmpDir, jname)
            fstat = os.stat(fname)
            logger.info(f"Updating pileup file at {jname} for workflow tarball: {tfile}")
            with open(fname, 'w', encoding='utf-8') as ostream:
                json.dump(jdict[jname], ostream)
            if fstat == os.stat(fname):
                # something wrong as we did not update the file
                msg = f"File {fname} was not properly updated in {tfile}, file stat is identical"
                raise Exception(msg)
        # archive back sandbox
        with tarfile.open(ofile, tarMode(ofile, 'w'), encoding='utf-8') as tar:
            tar.add(tmpDir, arcname='')
        # overwrite existing tarball with new one
        if dest:
            shutil.move(ofile, dest)
        else:
            shutil.move(ofile, tfile)


class WorkflowUpdaterException(WMException):
    """
    Specific WorkflowUpdaterPoller exception handling.
    """


class WorkflowUpdaterPoller(BaseWorkerThread):
    """
    Poller that does the actual work for updating workflows.
    """

    def __init__(self, config):
        """
        Initialize WorkflowUpdaterPoller object
        :param config: a Configuration object with the component configuration
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.logger = myThread.logger
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listActiveWflows = self.daoFactory(classname="Workflow.GetUnfinishedWorkflows")

        # parse mandatory attributes from the configuration
        self.config = config
        dbsUrl = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader"
        self.dbsUrl = getattr(config.WorkflowUpdater, "dbsUrl", dbsUrl)
        if not self.dbsUrl:
            self.dbsUrl = dbsUrl
        self.rucioAcct = getattr(config.WorkflowUpdater, "rucioAccount")
        self.rucioUrl = getattr(config.WorkflowUpdater, "rucioUrl")
        self.rucioAuthUrl = getattr(config.WorkflowUpdater, "rucioAuthUrl")
        self.rucioCustomScope = getattr(config.WorkflowUpdater, "rucioCustomScope",
                                        "group.wmcore")
        self.msPileupUrl = getattr(config.WorkflowUpdater, "msPileupUrl")

        self.userCert = cert()
        self.userKey = ckey()
        self.rucio = Rucio(acct=self.rucioAcct,
                           hostUrl=self.rucioUrl,
                           authUrl=self.rucioAuthUrl)

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Executed in every polling cycle. The actual logic of the component is:
          1. find active workflows in the agent
          2. check if those active workflows are using pileup data
        :param parameters: not really used. But keeping same signature as
            the one defined in the super class.
        :return: only what is returned by the decorator
        """
        logging.info("Running Workflow updater injector poller algorithm...")
        try:
            # retrieve list of workflows with unfinished Production
            # or Processing subscriptions
            wflowSpecs = self.listActiveWflows.execute()
            if not wflowSpecs:
                logging.info("Agent has no active workflows at the moment")
                return

            # figure out workflows that have pileup
            puWflows = self.findWflowsWithPileup(wflowSpecs)
            if not puWflows:
                logging.info("Agent has no active workflows with pileup at the moment")
                return
            # resolve unique active pileup dataset names
            uniqueActivePU = set(flattenList([item['pileup'] for item in puWflows]))

            # otherwise, move on retrieving pileups
            msPileupList = self.getPileupDocs()

            # and resolve blocks in each container being used by workflows
            # considerations for 2024 are around 100 pileups each taking 2 seconds in Rucio
            with CodeTimer("Rucio block resolution", logger=logging):
                self.findRucioBlocks(uniqueActivePU, msPileupList)

            with CodeTimer("Adjust JSON spec", logger=logging):
                self.adjustJSONSpec(puWflows, msPileupList)
        except Exception as ex:
            msg = f"Caught unexpected exception in WorkflowUpdater. Details:\n{str(ex)}"
            logging.exception(msg)
            raise WorkflowUpdaterException(msg) from None

    def adjustJSONSpec(self, puWflows, msPileupList, dest=None):
        """
        Main logic of the algorithm:
        - for every pileup record find out location of tarball
        - get configuration files within tarball
        - extract block information
        - replace pileupconf.json files within tarball
        :param puWflows: list of active pileup workflows with the following structure:
            {"name": string with workflow name,
             "spec": string with spec path, e.g.
             /path/install/wmagentpy3/WorkQueueManager/cache/<workflow_name>/WMSandbox/WMWorkload.pkl
             "pileup": list of strings with pileup names}
        :param msPileupList: list of all pileup records in MSPileup service. It has the following structure:
            {"pileupName": string with pileup name,
             "customName": string with custom pileup name - if any,
             "rses": list of RSE names,
             "blocks": list of block names}
        :param dest: optional destination parameter to write final tar ball (use for unit tests)
        :return: nothing, it performs checks and adjust in place pileupconf.json file(s)
        """
        # loop over active pileup workflows
        for wflow in puWflows:
            # this logic implies that we have untarred workflow tar ball
            sandboxDir = wflow['spec'].split('/WMSandbox')[0]
            tarName = wflow['name'] + '-Sandbox.tar.bz2'
            tarFile = os.path.join(sandboxDir, tarName)
            self.logger.info("Processing workflow %s, sandbox: %s", wflow, sandboxDir)

            # find the pileup files
            puFiles = findFiles(sandboxDir, "pileupconf.json")
            self.logger.debug("### puFiles %s", puFiles)

            # json dictionary to write back to our tarball, it has the following structure
            # {pileupConfFile: pileupConfContent, ...}
            # for example
            # {'relative/path/pileupconf.json': {...}}
            jdict = {}

            # relative path name to our pileup configuration json file
            puConfJson = ""
            # list over each pileupconf.json and update it
            for puFile in puFiles:
                # load the JSON and figure out the dataset name
                with open(puFile, 'r', encoding='utf-8') as istream:
                    # puJsonContent has the following data-structure:
                    # {<dtype>: {<block_name>: {"FileList": [list_of_files]}}}
                    # for example
                    # {'mc': {'/path/block#123': {"FileList: ["/file1.root", "/file2.root"]}}}
                    puJsonContent = json.load(istream)

                    # our puFile is path to pileupconf.json, we need to construct
                    # relative path within our tarball,
                    # therefore we only take WMSandbox/.../pileupconf.json part
                    puConfJson = os.path.join('WMSandbox', puFile.split('WMSandbox/')[-1])

                jsonPUName = findPUName(puJsonContent)
                if jsonPUName == "":
                    self.logger.warning("Unable to find pileup name in %s", puFile)
                    continue
                self.logger.info("Found pileup name %s under path: %s", jsonPUName, puFile)

                # now that we know the pileup name, iterate over the MSPileup docs
                for pileupDoc in msPileupList:
                    # check if active pileup workflow is found in MSPileup one
                    pileupName = pileupDoc['pileupName']
                    if pileupName == jsonPUName:
                        # then we need to check whether there are any changes or not
                        jsonBlockLoc = blockLocations(puJsonContent)

                        # construct new data-structure:
                        # - dict of blocks and rses mapping, it will be used by checkChanges
                        msPUBlockLoc = {}
                        for blk in pileupDoc["blocks"]:
                            msPUBlockLoc[blk] = pileupDoc["rses"]

                        # are the block locations different between the JSON and MSPileup?
                        # jsonBlockLoc and msPUBlockLoc have identical data-structures
                        # {block1: [rses], block2: [rses], ...}
                        if checkChanges(jsonBlockLoc, msPUBlockLoc):
                            self.logger.info("Found differences between JSON and MSPileup content.")
                            puNewJsonContent = updateBlockInfo(puJsonContent, msPUBlockLoc, self.dbsUrl, self.logger)

                            # we should update a tarball only once for each pileup name,
                            # therefore we add new entry to jdict with our pilupe conf file
                            if puNewJsonContent:
                                # we update json file if we get new pileup content
                                jdict[puConfJson] = puNewJsonContent
                                self.logger.info("Mark %s to be updated in tarball %s with a fresh pileup content",
                                                 puConfJson, tarFile)
                            else:
                                self.logger.warning("updateBlockInfo did not return any results for %s, will skip update of pileup json content", pileupName)
                        else:
                            msg = "### There are no differences between JSON and MSPileup content "
                            msg += f"for pileup name {pileupName}. Not updating anything!"
                            self.logger.info(msg)

            # replace all pileupconf.json files at once within tarball
            # please note jdict contains of json file names and their new content
            if jdict:
                self.logger.info("Write pileup configuration file %s", tarFile)
                writePileupJson(tarFile, jdict, self.logger, dest)
            self.logger.info("Done updating spec: %s\n", wflow['spec'])

    def getPileupDocs(self):
        """
        Fetch all pileup documents from MSPileup and preprocess the data.

        Note that the 'blocks' field is for the moment just a placeholder,
        as it will be populated in a later stage,

        :return: a list of dictionaries in the following format:
          {"pileupName": string with pileup name,
           "customName": string with custom pileup name - if any,
           "rses": list of RSE names,
           "blocks": list of block names}
        """
        try:
            result = getPileupDocs(self.msPileupUrl, queryDict={}, method='GET')
        except RuntimeError as e:
            raise WorkflowUpdaterException from e

        logging.info("A total of %d pileup documents have been retrieved.", len(result))
        pileupMapList = []
        for puItem in result:
            logging.info("Pileup: %s, custom name: %s, expected at: %s, but currently available at: %s",
                         puItem['pileupName'], puItem['customName'],
                         puItem['expectedRSEs'], puItem['currentRSEs'])
            thisPU = {"pileupName": puItem['pileupName'],
                      "customName": puItem['customName'],
                      "rses": puItem['currentRSEs'],
                      "blocks": []}
            pileupMapList.append(thisPU)
        return pileupMapList

    @staticmethod
    def findWflowsWithPileup(listSpecs):
        """
        Given a list of workflow names and their respective specs, load each
        one of them and filter out those that don't require any pileup dataset.
        :param listSpecs: a list of dictionary with workflow name and spec path
        :return: a list of dictionaries with workflow name, spec path and list
            of pileup datasets being used, e.g.:
            {"name": string with workflow name,
             "spec": string with spec path,
             "pileup": list of strings with pileup names}
        """
        wflowsWithPU = []
        for wflowSpec in listSpecs:
            try:
                workloadHelper = WMWorkloadHelper()
                workloadHelper.load(wflowSpec['spec'])
                pileupSpecs = workloadHelper.listPileupDatasets()
                if pileupSpecs:
                    wflowSpec['pileup'] = set()
                    for pileupN in pileupSpecs.values():
                        wflowSpec['pileup'] =  wflowSpec['pileup'] | pileupN
                    logging.info("Workflow: %s requires pileup dataset(s): %s",
                                 wflowSpec['name'], wflowSpec['pileup'])
                    wflowsWithPU.append(wflowSpec)
                else:
                    logging.info("Workflow: %s does not require any pileup", wflowSpec['name'])
            except Exception as ex:
                msg = f"Failed to load spec file for: {wflowSpec['spec']}. Details: {str(ex)}"
                logging.error(msg)
        logging.info("There are %d pileup workflows out of %d active workflows.",
                     len(wflowsWithPU), len(listSpecs))
        return wflowsWithPU

    def findRucioBlocks(self, uniquePUList, msPileupList):
        """
        Given a list of unique pileup dataset names, list all of
        their blocks in Rucio. Note that if a pileup document contains
        a customName dataset, then we need to resolve the blocks for that
        instead.
        :param uniquePUList: a list with pileup names
        :param msPileupList: a list with dictionaries from MSPileup
        :return: update the msPileupList object in place, by populating
            the 'block' field with a list of block names
        """
        for pileupItem in msPileupList:
            if pileupItem["pileupName"] not in uniquePUList:
                # no active workflow requires this pileup
                continue
            try:
                if pileupItem["customName"]:
                    logging.info("Fetching blocks for custom pileup container: %s", pileupItem["customName"])
                    pileupItem["blocks"] = self.rucio.getBlocksInContainer(pileupItem["customName"],
                                                                           scope=self.rucioCustomScope)
                else:
                    logging.info("Fetching blocks for pileup container: %s", pileupItem["pileupName"])
                    pileupItem["blocks"] = self.rucio.getBlocksInContainer(pileupItem["pileupName"], scope='cms')
            except WMRucioDIDNotFoundException as ex:
                logging.error(f"Could not find Rucio DID for an active PU! Original exception: {str(ex)}" )
