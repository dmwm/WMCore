from __future__ import division

# system modules
import json
import logging
import traceback

# CMSMonitoring modules
from CMSMonitoring.NATS import NATSManager


class NATS(object):
    def __init__(self, config):
        # initialize NATS if requested
        self.nats = None
        if getattr(config, 'use_nats', False) and getattr(config, 'nats_server', None):
            topic = getattr(config, 'nats_topic', 'cms.reqmgr2')
            topics = ['%s.topic' % topic]
            self.nats = NATSManager(config.nats_server, topics=topics, default_topic=topic)
            msg = "NATS: {}".format(self.nats)
            logging.info(msg)

    def request2NATS(self, workload, request_args):
        "Publish messages about reqmgr2 workflow to NATS server"
        # send update to NATS
        if self.nats:
            try:
                name = workload.name()
                priority = request_args.get('RequestPriority', -1)
                status = request_args.get('RequestStatus', 'NA')
                proc = request_args.get('ProcessingString', '')
                if isinstance(proc, dict):
                    proc = [k for k in proc.keys()]
                if isinstance(proc, list):
                    proc = json.dumps(proc)
                outputDatasets = request_args.get('OutputDatasets', [])
                if isinstance(outputDatasets, list):
                    outputDatasets = json.dumps(outputDatasets)
                doc = {'name': name,
                       'status': status,
                       'prority': priority,
                       'processing': proc,
                       'outputDatasets': outputDatasets}
                self.nats.publish(doc)
            except Exception as exc:
                logging.error(str(exc))
                logging.error(traceback.format_exc())
