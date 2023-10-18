"""
Hello world example using WMCore.REST handling framework.
Info class giving information about ReqMgr database.
Teams, Groups, Software versions handling for ReqMgr.

"""
from __future__ import print_function, division
from future.utils import listvalues

import json
import cherrypy

import WMCore
from WMCore.Database.CMSCouch import Document, CouchNotFoundError, CouchError
from WMCore.REST.Error import (NoSuchInstance, APINotSpecified,
                               MissingBodyData, InvalidUnifiedSchema)
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.ReqMgr.DataStructs.ReqMgrConfigDataCache import ReqMgrConfigDataCache
from WMCore.ReqMgr.Utils.AuxValidation import validateUnifiedConfig
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader


class Info(RESTEntity):
    """
    general information about reqmgr2, i.e. version, etc
    """

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_db = api.db_handler.get_db(config.couch_reqmgr_db)
        self.config = config

    def validate(self, apiobj, method, api, param, safe):
        pass

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self):
        # authorization / access control:
        # cherrypy (modified CMS web version here) can read this information
        # from HTTP headers. CMS web frontend puts this information into
        # headers as read from CRIC
        #        print "cherrypy.request", cherrypy.request
        #        print "DN: %s" % cherrypy.request.user['dn']
        #        print "Requestor/login: %s" % cherrypy.request.user['login']
        #        print "cherrypy.request: %s" % cherrypy.request
        #        print "cherrypy.request.user: %s" % cherrypy.request.user
        # from WMCore.REST.Auth import authz_match
        # authz_match(role=["Global Admin"], group=["global"])

        # implement as authentication decorator over modification calls
        # check config.py main.authz_defaults and CRIC
        # (only Admin: ReqMgr to be able to modify stuff)

        wmcore_reqmgr_version = WMCore.__version__

        reqmgr_db_info = self.reqmgr_db.info()
        reqmgr_db_info["reqmgr_couch_url"] = self.config.couch_host

        # retrieve the last injected request in the system
        # curl ... /reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1
        options = {"descending": True, "limit": 1}
        reqmgr_last_injected_request = self.reqmgr_db.loadView("ReqMgr", "bydate", options=options)
        result = {"wmcore_reqmgr_version": wmcore_reqmgr_version,
                  "reqmgr_db_info": reqmgr_db_info,
                  "reqmgr_last_injected_request": reqmgr_last_injected_request}
        # NOTE:
        # "return result" only would return dict with keys without (!!) values set
        return rows([result])


class ReqMgrConfigData(RESTEntity):
    def __init__(self, app, api, config, mount):
        # CouchDB auxiliary database name
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)

    def _validate_args(self, param, safe):
        # TODO: need proper validation but for now pass everything
        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["doc_name"] = param.args[0]
            param.args.pop()
        else:
            raise APINotSpecified()

        return

    def _validate_put_args(self, param, safe):

        args_length = len(param.args)
        if args_length == 1:
            safe.kwargs["doc_name"] = param.args[0]
            param.args.pop()

        data = cherrypy.request.body.read()

        if data:
            config_args = json.loads(data)
            # TODO need to validate the args depending on the config
            safe.kwargs["config_dict"] = config_args

    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.

        """
        if method == "GET":
            self._validate_args(param, safe)
        elif method == "PUT":
            self._validate_put_args(param, safe)

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    def get(self, doc_name):
        """
        """
        config = ReqMgrConfigDataCache.getConfig(doc_name)
        return rows([config])

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    def put(self, doc_name, config_dict=None):
        """
        """
        if doc_name == "DEFAULT":
            return ReqMgrConfigDataCache.putDefaultConfig()
        return ReqMgrConfigDataCache.replaceConfig(doc_name, config_dict)


class AuxBaseAPI(RESTEntity):
    """
    Base class for Aux db RESTEntry which contains get, post method
    """

    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        # CouchDB auxiliary database name
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)
        self.reqmgr_aux_db_service = RequestDBReader(self.reqmgr_aux_db, couchapp="ReqMgrAux")
        self.setName()

    def setName(self):
        "Sets the document name"
        raise NotImplementedError("Couch document id(name) should be specified. i.e. self.name='software'")

    def validate(self, apiobj, method, api, param, safe):
        args_length = len(param.args)
        if method in ["PUT", "POST"]:
            data = json.loads(cherrypy.request.body.read())
            if not data:
                raise MissingBodyData()
            if api == "unifiedconfig":
                try:
                    validateUnifiedConfig(data)
                except Exception as exc:
                    raise InvalidUnifiedSchema(str(exc)) from None
            # this variable is used as a kwarg for the put/post methods
            safe.kwargs["docData"] = data
        if args_length == 1:
            safe.kwargs["subName"] = param.args.pop(0)
            return
        return

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    def get(self, subName=None):
        """
        Return entire self.name document
        subName is subcategory of document which is added as postfix string
        """
        try:
            if subName:
                if subName.lower() == "all_docs":
                    return rows(self._getAllDocs())
                else:
                    docName = "%s_%s" % (self.name, subName)
            else:
                docName = self.name
            sw = self.reqmgr_aux_db.document(docName)
            del sw["_id"]
            del sw["_rev"]
        except CouchNotFoundError:
            raise NoSuchInstance()

        return rows([sw])

    def _getAllDocs(self):
        """
        Return all the documents under a given document type
        """
        try:
            option = {"include_docs": True}
            allDocs = self.reqmgr_aux_db_service.getRequestByCouchView("byconfig", option, [self.name])
        except CouchError as ex:
            msg = "ERROR: Failed to fetch ALL_DOCS for ConfigType: %s. Reason: %s" % (self.name, str(ex))
            cherrypy.log(msg)
            raise cherrypy.HTTPError(404, msg)

        return listvalues(allDocs)

    @restcall(formats=[('application/json', JSONFormat())])
    def post(self, subName=None, docData=None):
        """
        Inserts a new document into the database
        """
        if subName:
            docName = "%s_%s" % (self.name, subName)
        else:
            docName = self.name

        docData["ConfigType"] = self.name
        doc = Document(docName, docData)
        result = self.reqmgr_aux_db.commitOne(doc)
        return result

    @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
    def put(self, subName=None, docData=None):
        """
        Update document for the given self.name and subName.
        It assumes the client has provided the entire entity, i.e., the old
        content gets completely replaced by the new one.

        Given that each couch document contains a revision number, these PUT calls
        are not going to be idempotent.
        """
        result = None
        if subName:
            docName = "%s_%s" % (self.name, subName)
        else:
            docName = self.name

        try:
            existDoc = self.reqmgr_aux_db.document(docName)
            # replace original document
            newDoc = Document(existDoc['_id'], inputDict={'_rev': existDoc['_rev'],
                                                          'ConfigType': existDoc['ConfigType']})
            newDoc.update(docData)
            result = self.reqmgr_aux_db.commitOne(newDoc)
        except CouchNotFoundError:
            cherrypy.log("Document %s not found. Creating one." % docName)
            doc = Document(docName, docData)
            doc.update({'ConfigType': self.name})
            result = self.reqmgr_aux_db.commitOne(doc)

        return result

    @restcall(formats=[('application/json', JSONFormat())])
    def delete(self, subName):
        """
        Delete a document from ReqMgrAux
        """
        docName = "%s_%s" % (self.name, subName)
        try:
            res = self.reqmgr_aux_db.delete_doc(docName)
        except (CouchError, CouchNotFoundError) as ex:
            msg = "ERROR: failed to delete document: %s\nReason: %s" % (docName, str(ex))
            cherrypy.log(msg)
            res = None
        return res


class CMSSWVersions(AuxBaseAPI):
    """
    Handle CMSSW versions and scram architectures.
    Stored in the ReqMgr reqmgr_auxiliary database,
    """

    def setName(self):
        self.name = "CMSSW_VERSIONS"


class WMAgentConfig(AuxBaseAPI):
    """
    Handle WMAgent configuration files, which is
    a sub-set of the common config.py agent configuration
    """

    def setName(self):
        self.name = "WMAGENT_CONFIG"


class CampaignConfig(AuxBaseAPI):
    """
    Handle central production campaign configuration documents.
    """

    def setName(self):
        self.name = "CAMPAIGN_CONFIG"


class UnifiedConfig(AuxBaseAPI):
    """
    Handle central production unified configuration
    """

    def setName(self):
        self.name = "UNIFIED_CONFIG"


class TransferInfo(AuxBaseAPI):
    """
    Handle information related to the automatic transfers made
    by the MicroServices.
    """

    def setName(self):
        self.name = "TRANSFER"


class ParentLocks(AuxBaseAPI):
    """
    Handle list of locked parent datasets
    """

    def setName(self):
        self.name = "PARENT_LOCKS"
