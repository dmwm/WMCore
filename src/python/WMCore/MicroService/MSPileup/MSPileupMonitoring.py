"""
File       : MSPileupMonitoring.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileupMonitoring provides bridge between MSPileup
             service and CMS Monitoring infrastructure
"""

# system modules
import time

# WMCore modules
from WMCore.MicroService.Tools.Common import getMSLogger

# CMSMonitoring modules
from CMSMonitoring.StompAMQ7 import StompAMQ7 as StompAMQ


def flatDocuments(doc):
    """
    Helper function to flat out MSPileup document

    :param doc: input MSPileup document
    :return: generator of MSPileup documents flatten from original one
    """
    docs = flatKey(doc, 'campaigns')
    docs = (f for d in docs for f in flatKey(d, 'currentRSEs'))
    docs = (f for d in docs for f in flatKey(d, 'expectedRSEs'))
    for doc in docs:
        yield doc


def flatKey(doc, key):
    """
    Helper function to flat out values of given key in a document

    :param doc: input MSPileup document
    :param key: document key to use
    :return: generator of MSPileup documents flatten from original one and given key
    """
    for item in doc[key]:
        ndoc = dict(doc)
        # convert plural to singular key name, e.g. campaigns -> campaign
        nkey = key[:-1]
        ndoc[nkey] = item
        del ndoc[key]
        yield ndoc


class MSPileupMonitoring():
    """
    MSPileupMonitoring represents MSPileup monitoring class
    """

    def __init__(self, msConfig=None):
        """
        Constructor for MSPileupMonitoring
        """
        self.userAMQ = msConfig.get('user_amq', None)
        self.passAMQ = msConfig.get('pass_amq', None)
        self.topicAMQ = msConfig.get('topic_amq', None)
        self.docTypeAMQ = msConfig.get('doc_type_amq', 'cms-ms-pileup')
        self.hostPortAMQ = msConfig.get('host_port_amq', None)
        self.producer = msConfig.get('producer', 'cms-ms-pileup')
        self.logger = msConfig.get('logger', getMSLogger(False))

    def uploadToAMQ(self, docs, producer=None):
        """
        _uploadToAMQ_

        Sends data to AMQ, which ends up in elastic search.
        :param docs: list of documents/dicts to be posted
        :param producer: service name that's providing this info
        :return: {} or {"success": ndocs, "failures": nfailures}
        """
        if not docs:
            self.logger.info("There are no documents to send to AMQ")
            return {}
        if not self.userAMQ or not self.passAMQ:
            self.logger.info("MSPileupMonitoring has no AMQ credentials, will skip the upload to MONIT")
            return {}

        producer = producer or self.producer
        ts = int(time.time())
        notifications = []

        self.logger.debug("Sending %d to AMQ", len(docs))
        try:
            stompSvc = StompAMQ(username=self.userAMQ,
                                password=self.passAMQ,
                                producer=producer,
                                topic=self.topicAMQ,
                                validation_schema=None,
                                host_and_ports=self.hostPortAMQ,
                                logger=self.logger)

            for doc in docs:
                singleNotif, _, _ = stompSvc.make_notification(payload=doc, doc_type=self.docTypeAMQ,
                                                               ts=ts, data_subfield="payload")
                notifications.append(singleNotif)

            failures = stompSvc.send(notifications)
            msg = "%i out of %i documents successfully sent to AMQ" % (len(notifications) - len(failures),
                                                                       len(notifications))
            self.logger.info(msg)
            return {"success": len(notifications) - len(failures), "failures": len(failures)}
        except Exception as ex:
            self.logger.exception("Failed to send data to StompAMQ. Error %s", str(ex))
        return {}
