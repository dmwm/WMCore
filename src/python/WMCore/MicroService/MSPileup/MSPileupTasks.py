#!/usr/bin/env python
"""
File       : MSPileupTasks.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: perform different set of tasks over MSPileup data
"""

# system modules
import time
import math
import asyncio

# WMCore modules
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.MicroService.MSPileup.DataStructs.MSPileupReport import MSPileupReport
from WMCore.Services.UUIDLib import makeUUID
from WMCore.MicroService.Tools.PycurlRucio import getPileupContainerSizesRucio, getRucioToken
from WMCore.MicroService.MSPileup.MSPileupMonitoring import flatDocuments
from WMCore.MicroService.MSPileup.MSPileupError import MSPileupSchemaError
from Utils.Timers import gmtimeSeconds


class MSPileupTasks():
    """
    MSPileupTaskManager class is resposible for data placement logic. It performs
    three main tasks:
    - monitoring task to fetch current state of rule ID
    - inactive task to lookup pileup docs that has been set to inactive state
    - active task to look-up pileup docs in active state
    """

    def __init__(self, dataManager, monitManager, logger,
                 rucioAccount, rucioClient, rucioScope='cms',
                 customRucioScope='group.wmcore', dryRun=False):
        """
        MSPileupTaskManager constructor
        :param dataManager: MSPileup Data Management layer instance
        :param logger: logger instance
        :param rucioAccount: rucio account name to use
        :param rucioClient: rucio client or WMCore Rucio wrapper class to use
        :param rucioScope: rucio scope to use in rucio APIs
        :param customRucioScope: custom rucio scope to use in rucio APIs for customName DID
        :param dryRun: dry-run mode of operations
        """
        self.mgr = dataManager
        self.monitManager = monitManager
        self.logger = logger
        self.rucioAccount = rucioAccount
        self.rucioClient = rucioClient
        self.rucioScope = rucioScope
        self.customRucioScope = customRucioScope
        self.report = MSPileupReport()
        self.dryRun = dryRun
        if dryRun:
            self.logger.info("MSPileupTasks is set to DRY-RUN mode!")

    def pileupSizeTask(self):
        """
        Execute pileup size update task
        :return: a tuple (cmsDict, cusDict) which defines cms/custom containers and their
        respective dataset sizes dicts.
        """
        cmsDict = {}
        cusDict = {}
        self.logger.info("====> Executing pileupSizeTask method...")
        try:
            # get pileup sizes and update them in DB
            spec = {}
            docs = self.mgr.getPileup(spec)
            rucioAuthUrl = self.rucioClient.rucioParams.get('auth_host', '')
            rucioHostUrl = self.rucioClient.rucioParams.get('rucio_host', '')
            rucioToken, tokenValidity = getRucioToken(rucioAuthUrl, self.rucioAccount)
            msg = f"Rucio token {rucioToken} with validity {tokenValidity}"
            self.logger.info(msg)
            rucioScope = self.rucioScope
            cmsContainers = []
            customContainers = []
            datasetSizes = {}
            for doc in docs:
                if doc.get('customName', '') != '':
                    customContainers.append(doc['customName'])
                else:
                    cmsContainers.append(doc['pileupName'])
            msg = f"Fetching pileup size for {len(cmsContainers)} cms containers "
            msg += f"and {len(customContainers)} custom containers "
            msg += f"against rucio url: {rucioHostUrl}"
            self.logger.info(msg)
            if len(cmsContainers) > 0:
                datasetSizes = getPileupContainerSizesRucio(cmsContainers, rucioHostUrl, rucioToken)
                cmsDict = datasetSizes
            if len(customContainers) > 0:
                rucioScope = spec.get('customRucioScope', 'group.wmcore')
                cusDict = getPileupContainerSizesRucio(customContainers, rucioHostUrl, rucioToken, scope=rucioScope)
                datasetSizes.update(cusDict)
            for doc in docs:
                pileupName = doc.get('customName', '')
                if pileupName == '':
                    pileupName = doc['pileupName']
                pileupSize = datasetSizes.get(pileupName, 0)
                if pileupSize:
                    doc['pileupSize'] = pileupSize
                # here we update our new document in database with document validation
                self.mgr.updatePileupDocumentInDatabase(doc)
        except Exception as exp:
            msg = f"MSPileup pileup size task failed with error {exp}"
            self.logger.exception(msg)
        return cmsDict, cusDict

    def getPileupBlocks(self, doc, previousFraction, fraction):
        """
        Return list of blocks for increase/decrease scenarios of partial pileup
        according to logic outlined in
        https://gist.github.com/amaltaro/b4f9bafc0b58c10092a0735c635538b5#logic-for-increasingdecreasing-container-fraction
        :param doc: pileup document
        :param previousFraction: container fraction of previous step transition
        :param fraction: current container fraction
        :return: list of blocks
        """
        if previousFraction < fraction:
            return self.getIncreasingBlocks(doc, fraction)
        return self.getDecreasingBlocks(doc, fraction)

    def getIncreasingBlocks(self, doc, fraction):
        """
        Return list of blocks for increase scenarios of partial pileup
        according to logic outlined in
        https://gist.github.com/amaltaro/b4f9bafc0b58c10092a0735c635538b5#logic-for-increasingdecreasing-container-fraction
        NOTE: in order to increaee pileup fraction we already should have
        in place transition record since MSPilupeObj always created with container fraction 1

        :param doc: pileup document
        :param fraction: container fraction
        :return: list of blocks
        """
        pname = doc['pileupName']
        lastTransition = doc['transition'][-1]
        cname = lastTransition['customDID']
        totalBlocks = self.rucioClient.getBlocksInContainer(pname)
        customBlocks = self.rucioClient.getBlocksInContainer(cname)
        portion = math.ceil(fraction * len(totalBlocks))
        blockList = customBlocks + [b for b in totalBlocks if b not in customBlocks]
        self.logger.info("increase scenario: use %d blocks out of %d custom blocks from %s and %d from %s",
                         len(blockList), len(customBlocks), cname, abs(portion-len(customBlocks)), pname)
        return blockList[:portion]

    def getDecreasingBlocks(self, doc, fraction):
        """
        Return list of blocks for decrease scenarios of partial pileup
        according to logic outlined in
        https://gist.github.com/amaltaro/b4f9bafc0b58c10092a0735c635538b5#logic-for-increasingdecreasing-container-fraction

        :param doc: pileup document
        :param fraction: container fraction
        :return: list of blocks
        """
        pname = doc['pileupName']
        cname = doc['customName']
        totalBlocks = self.rucioClient.getBlocksInContainer(pname)
        portion = math.ceil(fraction * len(totalBlocks))
        if cname == '':
            self.logger.info("decrease scenario: use %d blocks out of pileup %s with %d blocks",
                             len(totalBlocks[:portion]), pname, len(totalBlocks))
            return totalBlocks[:portion]
        customBlocks = self.rucioClient.getBlocksInContainer(cname)
        self.logger.info("decrease scenario: use %d blocks out of custom container %s",
                         portion, cname)
        return customBlocks[:portion]

    def partialPileupTask(self):
        """
        Execute partial pileup placement according to the following logic:

        - get rucio DIDs (datasets, in CMS name blocks) for our pileup document
        - get portion of DIDs based on ceil(containerFraction * num_rucio_datasets)
        - create new container DID in Rucio via Rucio wrapper createContainer API
          which calls rucio CLI add_container
        - update MSPileup JSON with
          - create new custom Name as pileup+extention
          - add new transition record and update MSPileup document
        - we call attachDIDs Rucio wrapper API with our set of DIDs and rses from pileup document
        - create new rules for custom DID via provided rseExpression
        - add new ruleIds to pileup document
        - set expiration date (to be 24h) for already existing ruleIds from pileup document

        :return: None
        """
        spec = {'active': True}
        docs = self.mgr.getPileup(spec)
        for doc in docs:
            self.logger.info("partialPileupTask process pileup name %s", doc['pileupName'])
            fraction = doc['containerFraction']

            # check previous fraction value and proceed only if has changed
            transition = doc.get('transition', [])
            if transition:
                previousFraction = transition[-1]['containerFraction']
            else:
                previousFraction = doc['containerFraction']
            if previousFraction == fraction:
                # do nothing since there is no transition, i.e. skip this record
                self.logger.info("Pileup name %s has no container fractions changes", doc['pileupName'])
                continue

            # get rucio DIDs (datasets, in CMS name blocks) for our pileup document as following:
            # - when container fraction is decreasing: new container is a subset of customName
            # - when container fraction is increasing: new container is a superset of customName + subset of pileupName

            # usage of block names defined in this logic:
            # https://github.com/dmwm/WMCore/pull/11807#pullrequestreview-1786778783
            customBlocks = self.getPileupBlocks(doc, previousFraction, fraction)

            # create new container DID in Rucio for transition custom name
            cname = doc['transition'][-1]['customDID']
            self.logger.info("Create container %s with scope %s", cname, self.customRucioScope)
            status = self.rucioClient.createContainer(cname, scope=self.customRucioScope)
            if not status:
                self.logger.error("Failed to create container %s with scope %s", cname, self.customRucioScope)
                continue

            # call rucio APIs to attach custom blocks to our custom container (DID)
            newRules = []
            self.logger.info("Attaching %d blocks to custom pileup name: %s", len(customBlocks), cname)
            self.rucioClient.attachDIDs(None, cname, customBlocks, scope=self.customRucioScope)
            for rse in doc['expectedRSEs']:
                # create new rule for custom DID using pileup document rse
                ruleIds = self.rucioClient.createReplicationRule(cname, rse)
                self.logger.info("Rule ids: %s created for custom pileup: %s for RSE: %s", ruleIds, cname, rse)
                newRules += ruleIds

            self.logger.info("Custom pileup: %s has the following new rules created: %s for RSEs: %s",
                             doc['customName'], newRules, doc['expectedRSEs'])
            # set expiration date (to be 24h) for already existing ruleIds from pileup document
            for rid in doc['ruleIds']:
                # set expiration date to be 24h ahead of right now
                opts = {'lifetime': 24 * 60 * 60}
                self.rucioClient.updateRule(rid, opts)
                self.logger.info("Rule id: %s has been updated with lifetime: %s", rid, opts['lifetime'])

            # update pileup document
            if newRules:
                # remove duplicate rules
                newRules = list(set(newRules))
                try:
                    # update rules
                    doc['ruleIds'] = newRules

                    # update transition record of the pileup document, see logic:
                    # https://gist.github.com/amaltaro/b4f9bafc0b58c10092a0735c635538b5

                    # get previous transition record
                    prevTranRecord = doc['transition'][-1]

                    # keep custom name value from this record as we'll update it later
                    cname = prevTranRecord['customDID']

                    # update previous transition record in place, i.e. it will be updated
                    # within doc['transition'] directly after these assingments
                    prevTranRecord['containerFraction'] = fraction
                    prevTranRecord['updateTime'] = gmtimeSeconds()

                    # update custom name from value of previous transition record
                    doc['customName'] = cname
                    self.logger.info("Finally, updating the pileup document for pileup name: %s", doc['pileupName'])

                    # update MSPileup document in MongoDB
                    # here we update our new document in database with document validation
                    self.mgr.updatePileupDocumentInDatabase(doc, rseList=doc['currentRSEs'])
                    self.logger.info("Pileup name %s had its fraction updated in the partialPileupTask function", doc['pileupName'])
                except Exception as exp:
                    msg = f"Failed to update MSPileup document, {exp}"
                    self.logger.exception(msg)
                    err = MSPileupSchemaError(doc, msg)
                    self.logger.error(err)

    def cleanupTask(self, cleanupDaysThreshold):
        """
        Execute cleanup task according to the following logic:
        1. Fetch documents from backend database for the following conditions
           - active=False, and
           - empty ruleIds list; and
           - empty currentRSEs; and
           document has been deactivated for a while (deactivatedOn=XXX),
        2. For those documents which are fetched make delete call to backend database
        :param timeThreshold: time threshold in days which will determine document clean-up readiness
        """
        self.logger.info("====> Executing cleanupTask method...")
        spec = {'active': False}
        docs = self.mgr.getPileup(spec)
        deleteDocs = 0
        seconds = cleanupDaysThreshold * 24 * 60 * 60  # convert to second
        for doc in docs:
            pileupName = doc.get('customName', '')
            if pileupName == '':
                pileupName = doc['pileupName']
            if not doc['ruleIds'] and not doc['currentRSEs'] and \
                    time.time() > doc['deactivatedOn'] + seconds:
                spec = {'pileupName': pileupName}
                if not self.dryRun:
                    self.logger.info("Cleanup task deleting pileup %s", pileupName)
                    self.mgr.deletePileup(spec)
                    deleteDocs += 1
                else:
                    self.logger.info(f"DRY-RUN: should have deleted pileup document for {pileupName}")
        self.logger.info("Cleanup task deleted %d pileup objects", deleteDocs)

    def cmsMonitTask(self):
        """
        Execute CMS MONIT task according to the following logic:

        1. Read all pileup document from MongoDB
        2. Flatten all docs
        3. Submit flatten docs to CMS MONIT
        """
        self.logger.info("====> Executing cmsMonitTask method...")
        if not self.monitManager.userAMQ or not self.monitManager.passAMQ:
            self.logger.info("MSPileupMonitoring has no AMQ credentials, will skip the upload to MONIT")
            return
        spec = {}
        msPileupDocs = self.mgr.getPileup(spec)
        docs = []
        for doc in msPileupDocs:
            for flatDoc in flatDocuments(doc):
                docs.append(flatDoc)
        results = self.monitManager.uploadToAMQ(docs)
        if results and isinstance(results, dict):
            success = results['success']
            failures = results['failures']
            msg = f"MSPileup CMS MONIT task fetched {len(msPileupDocs)} docs from MSPileup backend DB"
            msg += f", and sent {len(docs)} flatten docs to MONIT"
            msg += f", number of success docs {success} and failures {failures},"
            self.logger.info(msg)
        else:
            self.logger.error("MSPileup CMS MONIT task failed!")

    def monitoringTask(self):
        """
        Execute Monitoring task according to the following logic:

        1. Read pileup document from MongoDB with filter active=true
        2. For each rule id in ruleIds:
           - query Rucio for that rule id and fetch its state (e.g.: afd122143kjmdskj)
           - if state=OK, log that the rule has been satisfied and add that RSE to
           the currentRSEs (unique)
           - otherwise, calculate the rule completion based on the 3 locks_* field
        3. now that all the known rules have been inspected, persist the up-to-date
        pileup doc in MongoDB
        """
        self.logger.info("====> Executing monitoringTask method...")
        spec = {'active': True}
        docs = self.mgr.getPileup(spec)
        taskSpec = self.getTaskSpec()
        self.logger.info("Running the monitoring task on %d pileup objects", len(docs))
        asyncio.run(performTasks('monitoring', docs, taskSpec))

    def inactiveTask(self):
        """
        Inactive pileup task:

        This task is supposed to look at pileup documents that have been set to
        inactive. The main goal here is to ensure that there are no Rucio rules
        left in the system (of course, for the relevant DID and the Rucio
        account adopted by our microservice). Pileup documents that are updated
        as a result of this logic should have their data persisted back in
        MongoDB. A short algorithm for it can be done described as follows:

        1. Read pileup document from MongoDB with filter active=false
        2. for each DID and Rucio account, get a list of all the existent rules
           - make a Rucio call to delete that rule id, then:
             - remove the rule id from ruleIds (if any) and remove the RSE name
             from currentRSEs (if any)
        3. make a log record if the DID + Rucio account tuple does not have any
        existent rules
           - and set ruleIds and currentRSEs to an empty list
        4. once all the relevant rules have been removed, persist an up-to-date
        version of the pileup data structure in MongoDB
        """
        self.logger.info("====> Executing inactiveTask method...")
        spec = {'active': False}
        docs = self.mgr.getPileup(spec)
        taskSpec = self.getTaskSpec()
        self.logger.info("Running the inactive task on %d pileup objects", len(docs))
        asyncio.run(performTasks('inactive', docs, taskSpec))

    def activeTask(self, marginSpace=1024**4):
        """
        Active pileup task:
        :param marginSpace: minimum margin space size in bytes to have at RSE to place a dataset

        This task is supposed to look at pileup documents active in the system.
        Its main goal is to ensure that the pileup DID has all the requested
        rules (and nothing beyond them), according to the pileup object
        configuration. Pileup documents that are updated as a result of this
        logic should have their data persisted back in MongoDB.

        1. Read pileup document from MongoDB with filter active=true
        2. if expectedRSEs is different than currentRSEs, then further data placement
        is required (it's possible that data removal is required!)
        3. make a local copy of the currentRSEs value to track rules incomplete but ongoing
        4. for each rule matching the DID + Rucio account, perform:
           - if rule RSE is not in expectedRSEs, then this rule needs to be deleted.
             - upon successful rule deletion, also remove the RSE name
             from currentRSEs and ruleIds (if any)
             - make a log record
           - else, save this rule RSE in the local copy of currentRSEs.
           This rule is likely still being processed by Rucio.
        5. now that we evaluated expected versus current,
        for each expectedRSEs not in our local copy of currentRSEs:
           - first, check whether the RSE has enough available space available for that
           (we can assume that any storage with less than 1 TB available cannot be
           considered for pileup data placement)
             - in case of no space available, make a log record
           - in case there is enough space, make a Rucio rule for
           that DID + Rucio account + RSE
             - now append the rule id to the ruleIds list
        6. once all the relevant rules have been created,
        or if there was any changes to the pileup object,
        persist an up-to-date version of the pileup data structure in MongoDB
        """
        self.logger.info("====> Executing activeTask method...")
        spec = {'active': True}
        docs = self.mgr.getPileup(spec)
        taskSpec = self.getTaskSpec()
        taskSpec['marginSpace'] = marginSpace
        self.logger.info("Running the active task on %d pileup objects", len(docs))
        asyncio.run(performTasks('active', docs, taskSpec))

    def getTaskSpec(self):
        """Return task spec"""
        spec = {'manager': self.mgr, 'logger': self.logger, 'report': self.report,
                'rucioClient': self.rucioClient, 'rucioAccount': self.rucioAccount,
                'rucioScope': self.rucioScope, 'customRucioScope': self.customRucioScope,
                'dryRun': self.dryRun}
        return spec

    def getReport(self):
        """
        Return report object to upstream codebase
        """
        return self.report


def monitoringTask(doc, spec):
    """
    Perform single monitoring task over provided MSPileup document

    :param doc: MSPileup document
    :param spec: task spec dict
    """
    mgr = spec['manager']
    uuid = spec['uuid']
    rucioClient = spec['rucioClient']
    rucioAccount = spec['rucioAccount']
    rucioScope = spec['rucioScope']
    report = spec['report']
    logger = spec['logger']

    # get list of existent rule ids
    pname = doc.get('customName', '')
    if pname == '':
        pname = doc['pileupName']
    else:
        rucioScope = spec.get('customRucioScope', 'group.wmcore')
    kwargs = {'scope': rucioScope, 'account': rucioAccount}
    rules = rucioClient.listDataRules(pname, **kwargs)
    modify = False

    if not rules:
        logger.info(f"Did not find any {rucioAccount} rules for container: {pname}.")
        if not doc['expectedRSEs']:
            logger.warning(f"Container: {pname} is active but has no expected RSEs.")
        elif doc['currentRSEs'] or doc['ruleIds']:
            doc['currentRSEs'].clear()
            doc['ruleIds'].clear()
            modify = True

    for rdoc in rules:
        rses = rucioClient.evaluateRSEExpression(rdoc['rse_expression'])
        rid = rdoc['id']
        state = rdoc['state']

        # rucio state have the following values
        # https://github.com/rucio/rucio/blob/master/lib/rucio/db/sqla/constants.py
        # the states are: OK, REPLICATING, STUCK, SUSPENDED, WAITING_APPROVAL, INJECT
        if state == 'OK':
            # log that the rule has been satisfied and add that RSE to the currentRSEs (unique)
            msg = f"monitoring task {uuid}, container {pname} under the rule ID {rid}"
            msg += f" targeting RSEs {rses} in rucio account {rucioAccount} is completely available"
            logger.info(msg)
            for rse in rses:
                if rse not in doc['currentRSEs']:
                    doc['currentRSEs'].append(rse)
                    modify = True
                    msg = f"update currentRSEs with {rse}"
                    report.addEntry('monitoring', uuid, msg)
        else:
            # calculate the rule completion based on the 3 locks_* field
            sumOfLocks = rdoc['locks_ok_cnt'] + rdoc['locks_replicating_cnt'] + rdoc['locks_stuck_cnt']
            completion = rdoc['locks_ok_cnt'] / sumOfLocks
            msg = f"monitoring task {uuid}, container {pname} under the rule ID {rid}"
            msg += f" targeting RSEs {rses} in rucio account {rucioAccount} has"
            msg += f" a fraction completion of {completion}"
            logger.info(msg)
            for rse in rses:
                if rse in doc['currentRSEs']:
                    doc['currentRSEs'].remove(rse)
                    modify = True
                    msg = f"delete rse {rse} from currentRSEs"
                    report.addEntry('monitoring', uuid, msg)

        # now keep track of this rule id in the pileup document
        for rse in rses:
            if rse in doc['expectedRSEs'] and rid not in doc['ruleIds']:
                logger.info(f"Tracking rule id {rid} that was not created by MSPileup")
                doc['ruleIds'].append(rid)
                modify = True
                break

    # persist an up-to-date version of the pileup data structure in MongoDB
    if modify:
        logger.info(f"monitoring task {uuid}, update {pname}")
        # here we update our new document in database without validation since
        # we only updated rule ids and rses
        mgr.updatePileupDocumentInDatabase(doc, validate=False)
        msg = f"update pileup {pname}"
        report.addEntry('monitoring', uuid, msg)
    else:
        logger.info(f"monitoring task {uuid}, processed without update for {pname}")


def inactiveTask(doc, spec):
    """
    Perform single inactive task over provided MSPileup document

    :param doc: MSPileup document
    :param spec: task spec dict
    """
    mgr = spec['manager']
    uuid = spec['uuid']
    rucioClient = spec['rucioClient']
    rucioAccount = spec['rucioAccount']
    rucioScope = spec['rucioScope']
    report = spec['report']
    logger = spec['logger']
    dryRun = spec['dryRun']
    pname = doc.get('customName', '')
    if pname == '':
        pname = doc['pileupName']
    else:
        rucioScope = spec.get('customRucioScope', 'group.wmcore')
    kwargs = {'scope': rucioScope, 'account': rucioAccount}
    rules = rucioClient.listDataRules(pname, **kwargs)
    modify = False

    for rdoc in rules:
        # make a Rucio call to delete that rule id
        rid = rdoc['id']
        msg = f"inactive task {uuid}, container: {pname} for Rucio account {rucioAccount}"
        msg += f", delete replication rule {rid}"
        logger.info(msg)
        if not dryRun:
            rucioClient.deleteRule(rid)
        else:
            logger.info(f"DRY-RUN: rule id '{rid}' should have been deleted.")
            continue
        rses = rucioClient.evaluateRSEExpression(rdoc['rse_expression'])

        # remove the rule id from ruleIds (if any) and remove the RSE name from currentRSEs (if any)
        if rid in doc['ruleIds']:
            doc['ruleIds'].remove(rid)
            modify = True
            msg = f"remove rid {rid}"
            report.addEntry('inactive', uuid, msg)
        for rse in rses:
            if rse in doc['currentRSEs']:
                doc['currentRSEs'].remove(rse)
                modify = True
                msg = f"remove rse {rse}"
                report.addEntry('inactive', uuid, msg)

    # make a log record if the DID + Rucio account tuple does not have any existent rules
    if not rules:
        msg = f"inactive task {uuid}, container: {pname} for Rucio account {rucioAccount}"
        msg += " does not have any existing rules, proceed without update"
        logger.info(msg)

    # persist an up-to-date version of the pileup data structure in MongoDB
    if modify:
        logger.info(f"inactive task {uuid}, update {pname}")
        # here we update our new document in database without validation since
        # we only updated rule ids and rses
        mgr.updatePileupDocumentInDatabase(doc, validate=False)
        msg = f"update pileup {pname}"
        report.addEntry('inactive', uuid, msg)


def activeTask(doc, spec):
    """
    Perform single active task over provided MSPileup document

    :param doc: MSPileup document
    :param spec: task spec dict
    """
    mgr = spec['manager']
    uuid = spec['uuid']
    logger = spec['logger']
    rucioClient = spec['rucioClient']
    rucioAccount = spec['rucioAccount']
    rucioScope = spec['rucioScope']
    report = spec['report']
    marginSpace = spec['marginSpace']
    dryRun = spec['dryRun']

    # extract relevant part of our pileup document we'll use in our logic
    expectedRSEs = set(doc['expectedRSEs'])
    currentRSEs = set(doc['currentRSEs'])
    pileupSize = doc['pileupSize']
    pname = doc.get('customName', '')
    if pname == '':
        pname = doc['pileupName']
    else:
        rucioScope = spec.get('customRucioScope', 'group.wmcore')
    inputArgs = {'scope': rucioScope, 'name': pname, 'account': rucioAccount}
    kwargs = {'scope': rucioScope, 'account': rucioAccount}
    modify = False
    localCopyOfCurrentRSEs = list(currentRSEs)

    if not pileupSize:
        logger.error(f"Pileup {pname} does not have its size defined: {pileupSize}")
    elif expectedRSEs != currentRSEs:
        # this means that further data placement is required, or it could be
        # that that data removal is actually required!
        msg = f"active task {uuid}"
        msg += f", further data placement required for pileup name: {pname},"
        msg += f" with expectedRSEs: {expectedRSEs} and data currently available at: {currentRSEs}"
        logger.info(msg)

        # get list of replication rules for our scope, pileup name and account
        rules = rucioClient.listDataRules(pname, **kwargs)
        # for each rse_expression in rules get list of rses
        for rdoc in rules:
            rses = rucioClient.evaluateRSEExpression(rdoc['rse_expression'])
            rdoc['rses'] = rses

        # for each rule matching the DID + Rucio account
        for rdoc in rules:
            rid = rdoc['id']
            rses = rdoc['rses']
            for rse in rses:
                # if rule RSE is not in expectedRSEs, then this rule needs to be deleted
                if rse not in expectedRSEs:
                    # upon successful rule deletion, also remove the RSE name from currentRSEs
                    if not dryRun:
                        rucioClient.deleteRule(rid)
                    else:
                        logger.info(f"DRY-RUN: rule id '{rid}' should have been deleted.")
                    if rse in doc['currentRSEs']:
                        doc['currentRSEs'].remove(rse)
                        modify = True
                    msg = f"rse {rse} rule is deleted and remove from currentRSEs of {pname}"
                    logger.info(msg)
                    # delete rid in ruleIds (if any)
                    if rid in doc['ruleIds']:
                        doc['ruleIds'].remove(rid)
                        modify = True
                        msg = f"rule Id {rid} is removed from ruleIds for {pname}"
                        logger.info(msg)
                else:
                    # else, save this rule RSE in the local copy of currentRSEs.
                    # This rule is likely still being processed by Rucio.
                    if rse not in localCopyOfCurrentRSEs:
                        localCopyOfCurrentRSEs.append(rse)
                        msg = f"rse {rse} is added to localCopyOfCurrentRSEs"
                        logger.info(msg)

        # for each expectedRSEs not in localCopyOfCurrentRSEs
        for rse in set(expectedRSEs).difference(localCopyOfCurrentRSEs):
            # check whether the RSE has enough available space available for that
            # taken from logic of RSEQuotas, see https://bit.ly/3kDJZmO
            # but here we use explicitly rse parameter
            records = rucioClient.getAccountUsage(rucioAccount, rse)
            enoughSpace = False
            for rec in records:
                # each record has form
                # {"rse": ..., "bytes_limit": ..., "bytes": ..., "bytes_remaining": ...}
                if rec['bytes_remaining'] - pileupSize > marginSpace:
                    enoughSpace = True
                    break
            dids = [inputArgs]
            if enoughSpace:
                msg = f"active task {uuid}, for dids {dids} there is enough space at RSE {rse}"
                logger.warning(msg)
                # create the rule and append the rule id to ruleIds
                if not dryRun:
                    rids = rucioClient.createReplicationRule(pname, rse, **kwargs)
                else:
                    msg = f"DRY-RUN: rule for pileup {pname} and rse {rse} should have been created"
                    logger.info(msg)
                    continue
                # add new ruleId to document ruleIds
                for rid in rids:
                    if rid not in doc['ruleIds']:
                        doc['ruleIds'].append(rid)
                        modify = True
            else:
                # make a log record saying that there is not enough space
                msg = f"active task {uuid}, for dids {dids} there is not enough space at RSE {rse}"
                logger.warning(msg)

    # persist an up-to-date version of the pileup data structure in MongoDB
    if modify:
        logger.info(f"active task {uuid} update {pname}")
        # here we update our new document in database without validation since
        # we only updated rule ids and rses
        mgr.updatePileupDocumentInDatabase(doc, validate=False)
        msg = f"update pileup {pname}"
        report.addEntry('active', uuid, msg)
    else:
        logger.info(f"active task {uuid}, processed without update")


async def runTask(task, doc, spec):
    """
    Run specified task for given document and spec

    :param task: task to perform
    :param doc: MSPileup document to process
    :param spec: task spec dictionary
    """
    time0 = time.time()
    report = spec['report']
    logger = spec.get('logger', getMSLogger(False))
    pname = doc.get('customName', '')
    if pname == '':
        pname = doc['pileupName']

    # set report hash
    uuid = makeUUID()
    spec['uuid'] = uuid
    report.addEntry(task, uuid, 'starts')
    msg = f"MSPileup {task} task {uuid} pileup {pname}"
    try:
        if task == 'monitoring':
            monitoringTask(doc, spec)
        elif task == 'inactive':
            inactiveTask(doc, spec)
        elif task == 'active':
            activeTask(doc, spec)
        msg += ", successfully processed"
    except Exception as exp:
        msg += f", failed with error {exp}"
        logger.exception(msg)
    report.addEntry(task, uuid, msg)

    # update task report
    etime = time.time() - time0
    msg = "ends, elapsed time %.2f (sec)" % etime
    report.addEntry(task, uuid, msg)


async def performTasks(task, docs, spec):
    """
    Perform tasks via async IO co-routines

    :param task: task to perform
    :param docs: list of MSPileup documents to process
    :param spec: task spec dictionary
    """
    coRoutines = [runTask(task, doc, spec) for doc in docs]
    await asyncio.gather(*coRoutines)
