"""
Hello world example using WMCore.REST handling framework.
Info class giving information about ReqMgr database.
Teams, Groups, Software versions handling for ReqMgr.

"""

import logging
import cherrypy
import urllib
import xml.dom.minidom
from xml.parsers.expat import ExpatError

import WMCore
from WMCore.Configuration import loadConfigurationFile
from WMCore.Database.CMSCouch import CouchServer, Database, Document
from WMCore.Database.CMSCouch import CouchError, CouchNotFoundError
from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str

import WMCore.ReqMgr.Service.RegExp as rx
from WMCore.REST.Format import *



class HelloWorld(RESTEntity):
    
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate request input data.
        Has to be implemented, otherwise the service fails to start.
        If it's not implemented correctly (e.g. just pass), the arguments
        are not passed in the method at all.
        
        """
        validate_str("name", param, safe, rx.RX_REQUEST_NAME, optional=True)


    @restcall(formats = [('application/json', JSONFormat())])
    @tools.expires(secs=-1)
    def get(self, name):
        """
        Hello world API call.
        
        :arg str name: name to appear in the result message.
        :returns: row with response, here 1 item list with message.
        
        """
        msg = "Hello "
        msg += name or "world"
        #return rows(["Hello: world"]) returns the same as above
        return msg
    

class Info(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_db = api.db_handler.get_db(config.couch_reqmgr_db)
        self.config = config
                
    def validate(self, apiobj, method, api, param, safe):
        pass

    
    @restcall
    @tools.expires(secs=-1)
    def get(self):
        # authorization / access control:
        # cherrypy (modified CMS web version here) can read this information
        # from HTTP headers. CMS web frontend puts this information into
        # headers as read from SiteDB (or on private VM from a fake
        # SiteDB file)
        print "DN: %s" % cherrypy.request.user['dn']
        print "Requestor/login: %s" % cherrypy.request.user['login']
        print "cherrypy.request: %s" % cherrypy.request
        print "cherrypy.request.user: %s" % cherrypy.request.user
        # from WMCore.REST.Auth import authz_match
        # authz_match(role=["Global Admin"], group=["global"])
        # check SiteDB/DataWhoAmI.py
        
        # implement as authentication decorator over modification calls
        # check config.py main.authz_defaults and SiteDB
        # (only Admin: ReqMgr to be able to modify stuff)
        
        wmcore_reqmgr_version = WMCore.__version__
        
        reqmgr_db_info = self.reqmgr_db.info()
        reqmgr_db_info["reqmgr_couch_url"] = self.config.couch_host 
        
        # retrieve the last injected request in the system
        # curl ... /reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1
        options = {"descending": True, "limit": 1} 
        reqmgr_last_injected_request = self.reqmgr_db.loadView("ReqMgr",
                                                    "bydate",
                                                    options=options)
        result = {"wmcore_reqmgr_version": wmcore_reqmgr_version,
                  "reqmgr_db_info": reqmgr_db_info,
                  "reqmgr_last_injected_request": reqmgr_last_injected_request}
        # NOTE:
        # "return result" only would return dict with keys without (!!) values set
        return rows([result])
    


class Group(RESTEntity):
    """
    Groups are stored in the ReqMgr reqmgr_auxiliary database.
    "group" is used as id of the document, but the document
        itself has to be JSON, so we use only keys in the dictionary
        for group names, other data, if necessary, may be stored as values.
        Currently: {"group1": None, "group2": None, ...}
        
    Newly injected request will specify Group (it's needed down the
    WorkQueue/WMAgent chain), but there is no association between requesting
    user (Requestor) and this Group. SiteDB groups is something different.
        
    """
    
    def __init__(self, app, api, config, mount):
        # CouchDB auxiliary database name
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)    
        

    def validate(self, apiobj, method, api, param, safe):
        if method in ("GET", "HEAD"):
            return
        elif method in ("DELETE, PUT"):
            validate_str("group_name", param, safe, rx.RX_GROUP_NAME, optional=False)
        # from SiteDB
        # authz_match(role=["Global Admin"], group=["global"])


    @restcall
    def get(self):
        """
        Return list of all groups.
            
        """
        groups = self.reqmgr_aux_db.document("groups")
        del groups["_id"]
        del groups["_rev"]
        return rows(groups.keys())
                                          
        
    @restcall
    def delete(self, group_name):
        """
        Removes an existing group from the database, raises an error
        if group_name doesn't exist.
        
        """
        groups = self.reqmgr_aux_db.document("groups")
        if group_name in groups:
            del groups[group_name]
            # TODO
            # this should ideally also wrap try-except
            self.reqmgr_aux_db.commitOne(groups)
            return rows(["OK"])
        else:
            msg = "ERROR: Group '%s' not found in the database." % group_name
            cherrypy.log(msg)
            raise cherrypy.HTTPError(404, msg)            
                
     
    @restcall
    def put(self, group_name):
        """
        Adds group of group_name into the database.
        Creates groups document if it doesn't exist.
        
        """
        try:
            groups = self.reqmgr_aux_db.document("groups")
        except CouchNotFoundError, ex:
            msg = ("ERROR: Retrieving groups document failed, reason: %s"
                   " Creating the document ..." % ex)
            cherrypy.log(msg)
            try:
                doc = Document(id="groups", inputDict={group_name: None})
                self.reqmgr_aux_db.commitOne(doc)
                return
            except CouchError, ex:
                msg = "ERROR: Creating document groups failed, reason: %s" % ex
                cherrypy.log(msg)
                raise cherrypy.HTTPError(400, msg)
        
        if group_name in groups:
            return rows(["Already exists."])
        else:
            groups[group_name] = None
            # TODO
            # this should ideally also wrap try-except            
            self.reqmgr_aux_db.commitOne(groups)
            return rows(["OK"])
            
        

class Team(RESTEntity):
    """
    Teams are stored in the ReqMgr reqmgr_auxiliary database.
    "teams" is used as id of the document, but the document
        itself has to be JSON: we use {"teams": [list, of, group, names]
        
    When request goes through assignment status, it gets assigned
    to a team.
    Where a request gets run is controlled by SiteBlack/White list.
        
    """
    
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        # CouchDB auxiliary database name
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)                
        
        
    def validate(self, apiobj, method, api, param, safe):
        if method in ("GET", "HEAD"):
            return
        elif method in ("DELETE, PUT"):
            validate_str("team_name", param, safe, rx.RX_TEAM_NAME, optional=False)
                
    
    @restcall
    def get(self):
        """
        Return list of all teams.
            
        """
        teams = self.reqmgr_aux_db.document("teams")
        del teams["_id"]
        del teams["_rev"]
        return rows(teams.keys())
    
    
    @restcall
    def delete(self, team_name):
        """
        Removes an existing team from the database, raises an error
        if team_name doesn't exist.
        
        """
        teams = self.reqmgr_aux_db.document("teams")
        if team_name in teams:
            del teams[team_name]
            # TODO
            # this should ideally also wrap try-except
            self.reqmgr_aux_db.commitOne(teams)
            return rows(["OK"])
        else:
            msg = "ERROR: Team '%s' not found in the database." % team_name
            cherrypy.log(msg)
            raise cherrypy.HTTPError(404, msg)            
        
        
    @restcall
    def put(self, team_name):
        """
        Adds team of team_name into the database.
        Creates teams document if it doesn't exist.
        
        """
        try:
            teams = self.reqmgr_aux_db.document("teams")
        except CouchNotFoundError, ex:
            msg = ("ERROR: Retrieving teams document failed, reason: %s"
                   " Creating the document ..." % ex)
            cherrypy.log(msg)
            try:
                doc = Document(id="teams", inputDict={team_name: None})
                self.reqmgr_aux_db.commitOne(doc)
                return
            except CouchError, ex:
                msg = "ERROR: Creating document teams failed, reason: %s" % ex
                cherrypy.log(msg)
                raise cherrypy.HTTPError(400, msg)
        
        if team_name in teams:
            return rows(["Already exists."])
        else:
            teams[team_name] = None
            # TODO
            # this should ideally also wrap try-except            
            self.reqmgr_aux_db.commitOne(teams)
            return rows(["OK"])
        
        
    
class Software(RESTEntity):
    """
    Software - handle CMSSW versions and scram architectures.
    Stored in stored in the ReqMgr reqmgr_auxiliary database, document
        id "software".
        
    """
    
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
        # CouchDB auxiliary database name
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)
        
        
    def validate(self, apiobj, method, api, param, safe):
        pass


    @restcall
    def get(self):
        """
        Return entire "software" document - all versions and scramarchs.
            
        """
        sw = self.reqmgr_aux_db.document("software")
        del sw["_id"]
        del sw["_rev"]
        return rows([sw])
    
    

def _get_all_scramarchs_and_versions(url):
    """
    Downloads a list of all ScramArchs and Versions from the tag collector.
    Uses XML tag collector resourse.
    
    Result is a dictionary with scramarch as keys and values are lists of
        corresponding CMSSW releases.
    
    """
    result = {}
    try:
        logging.debug("Getting data from %s ..." % url)
        f = urllib.urlopen(url)
        dom_doc = xml.dom.minidom.parse(f)
    except ExpatError, ex:
        logging.error("Could not get data from CMS tag collector, abort."
                      " Reason: %s" % ex)
        return {}
    arch_doms = dom_doc.firstChild.getElementsByTagName("architecture")
    for arch_dom in arch_doms:
        arch = arch_dom.attributes.item(0).value
        sw_releases = []
        for node in arch_dom.childNodes:
            # Somehow we can get extraneous ('\n') text nodes in
            # certain versions of Linux
            if str(node.__class__) == "xml.dom.minidom.Text":
                continue
            if not node.hasAttributes():
                # Then it's an empty random node created by the XML
                continue
            for i in range(node.attributes.length):
                attr = node.attributes.item(i)
                if str(attr.name) == "label":
                    sw_releases.append(str(attr.value))
        result[str(arch)] = sw_releases
    return result
    
    
def update_software(config_file):
    """
    Functions retrieves CMSSW versions and scramarchs from CMS tag collector.
    
    """
    config = loadConfigurationFile(config_file)
    # source of the data
    tag_collector_url = config.views.data.tag_collector_url
    # store the data into CouchDB auxiliary database under "software" document
    couch_host = config.views.data.couch_host
    reqmgr_aux_db = config.views.data.couch_reqmgr_aux_db
    
    # get data from tag collector
    all_archs_and_versions = _get_all_scramarchs_and_versions(tag_collector_url)
    if not all_archs_and_versions:
        return
    
    # get data already stored in CouchDB    
    couchdb = Database(dbname=reqmgr_aux_db, url=couch_host)
    try:
        sw_already_stored = couchdb.document("software")
        del sw_already_stored["_id"]
        del sw_already_stored["_rev"]
    except CouchNotFoundError:
        logging.error("Document id software, does not exist, creating it ...")
        doc = Document(id="software", inputDict=all_archs_and_versions)
        couchdb.commitOne(doc)
        return
    
    # now compare recent data from tag collector and what we already have stored
    if all_archs_and_versions != sw_already_stored:
        logging.debug("ScramArch/CMSSW releases changed, updating software document ...")
        doc = Document(id="software", inputDict=all_archs_and_versions)
        couchdb.commitOne(doc)
        # TODO
        # remove this, only to observe differences during update attempts
        #msg = ("Really changed? Compare:\n"
        #       "sw_already_stored:\n%s\n"
        #       "all_archs_and_versions (returned from TC):\n%s\n" %
        #       (sw_already_stored, all_archs_and_versions))
        #print msg
