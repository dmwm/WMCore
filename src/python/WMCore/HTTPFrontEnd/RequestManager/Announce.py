"""
Main Module for announcing requests.

"""

import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
import logging
import cherrypy
import WMCore.Lexicon
from WMCore.HTTPFrontEnd.RequestManager.BulkOperations import BulkOperations
import WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools as Utilities
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrAuth import ReqMgrAuth

class Announce(BulkOperations):
    """ Page for Data Ops to announce requests """
    def __init__(self, config):
        BulkOperations.__init__(self, config)
        self.wmstatWriteURL = "%s/%s" % (config.couchUrl.rstrip('/'), config.wmstatDBName)
        self.acdcURL = "%s/%s" % (config.couchUrl.rstrip('/'), config.acdcDBName)
        self.searchFields = ["RequestName", "RequestType"]

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def index(self):
        """ Page for announcing requests """
        return self.draw(self.requests())

    def requests(self):
        """ Base list of the requests """
        return Utilities.requestsWhichCouldLeadTo('announced')

    def draw(self, requests):
        return self.templatepage("BulkOperations", operation="Announce",
                                  searchFields = ["RequestName", "RequestType"],
                                  actions=None, requests=requests)

    @cherrypy.expose
    @cherrypy.tools.secmodv2(role=ReqMgrAuth.assign_roles)
    def handleAnnounce(self, **kwargs):
        """ Handler for announcing requests """
        requests = self.requestNamesFromCheckboxes(kwargs)
        # the distinction good/bad datasets was based upon contacting
        # wrong DBS url so it was always giving rubbish,
        # perhaps the entire page is just not used at all
        datasets = []
        goodDatasets = []
        badDatasets = []
        for requestName in requests:
            WMCore.Lexicon.identifier(requestName)
            Utilities.changeStatus(requestName, 'announced', wmstatUrl = self.wmstatWriteURL,
                                   acdcUrl = self.acdcURL)
            datasets.extend(Utilities.getOutputForRequest(requestName))
        for dataset in datasets:
            # was needed for the DBS call to wrong DBS URL
            # check code before 2013-04-04
            #toks = dataset.split('/')
            #data = {'primary_ds_name': toks[0], 'processed_ds_name': toks[1],
            #        'data_tier_name': toks[2], 'is_dataset_valid': 1}
            goodDatasets.append(dataset)
        return self.templatepage("Announce", requests=requests,
                                 goodDatasets=goodDatasets,
                                 badDatasets=badDatasets)