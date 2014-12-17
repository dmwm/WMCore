#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-

"""
web server.
"""

__author__ = "Valentin Kuznetsov"

# system modules
import os
import sys
import time
import json
import pprint
from types import GeneratorType
try:
    import cStringIO as StringIO
except:
    import StringIO

# cherrypy modules
import cherrypy
from cherrypy import expose, response, tools
from cherrypy.lib.static import serve_file
from cherrypy import config as cherryconf

# ReqMgrSrv modules
from WMCore.ReqMgr.Web.tools import exposecss, exposejs, exposejson, TemplatedPage
from WMCore.ReqMgr.Web.utils import json2table, genid, checkargs, tstamp, sort
from WMCore.ReqMgr.Utils.url_utils import getdata
from WMCore.ReqMgr.Tools.cms import dqm_urls, dbs_urls, releases, architectures
from WMCore.ReqMgr.Tools.cms import scenarios, cms_groups, couch_url
from WMCore.ReqMgr.Tools.cms import web_ui_names, next_status, sites
from WMCore.ReqMgr.Tools.cms import lfn_bases, lfn_unmerged_bases
from WMCore.ReqMgr.Tools.cms import site_white_list, site_black_list
from WMCore.ReqMgr.Tools.cms import cust_sites, non_cust_sites, auto_approve_sites

# WMCore modules
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.ReqMgr.Service.Auxiliary import Info, Group, Team, Software
from WMCore.ReqMgr.Service.Request import Request
from WMCore.ReqMgr.Service.RestApiHub import RestApiHub
from WMCore.REST.Main import RESTMain
#from WMCore.REST.Auth import authz_fake

# WMCore specs
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory
from WMCore.WMSpec.StdSpecs.StoreResults import StoreResultsWorkloadFactory
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing
from WMCore.WMSpec.StdSpecs.Resubmission import ResubmissionWorkloadFactory
from WMCore.WMSpec.StdSpecs.ReDigi import ReDigiWorkloadFactory
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE, REQUEST_STATE_TRANSITION

# new reqmgr2 APIs
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr

def sort_bold(docs):
    "Return sorted list of bold items from provided doc list"
    return ', '.join(['<b>%s</b>'%m for m in sorted(docs)])

def set_headers(itype, size=0):
    """
    Set response header Content-type (itype) and Content-Length (size).
    """
    if  size > 0:
        response.headers['Content-Length'] = size
    response.headers['Content-Type'] = itype
    response.headers['Expires'] = 'Sat, 14 Oct 2027 00:59:30 GMT'

def set_no_cache_flags():
    "Set cherrypy flags to prevent caching"
    cherrypy.response.headers['Cache-Control'] = 'no-cache'
    cherrypy.response.headers['Pragma'] = 'no-cache'
    cherrypy.response.headers['Expires'] = 'Sat, 01 Dec 2001 00:00:00 GMT'

def set_cache_flags():
    "Set cherrypy flags to prevent caching"
    headers = cherrypy.response.headers
    for key in ['Cache-Control', 'Pragma']:
        if  key in headers:
            del headers[key]

def minify(content):
    """
    Remove whitespace in provided content.
    """
    content = content.replace('\n', ' ')
    content = content.replace('\t', ' ')
    content = content.replace('   ', ' ')
    content = content.replace('  ', ' ')
    return content

def menus(active='search'):
    "Return dict of menus"
    items = ['admin', 'assign', 'approve', 'create', 'requests']
    mdict = dict(zip(items, ['']*len(items)))
    mdict[active] = 'active'
    return mdict

def request_attr(doc, attrs=None):
    "Return request attributes/values in separate document"
    if  not attrs:
        attrs = ['RequestName', 'Requestdate', 'Inputdataset', \
                'Prepid', 'Group', 'Requestor', 'RequestDate', \
                'RequestStatus']
    rdict = {}
    for key in attrs:
        if  key in doc:
            if  key=='RequestDate':
                tval = doc[key]
                if  isinstance(tval, list):
                    while len(tval) < 9:
                        tval.append(0)
                    gmt = time.gmtime(time.mktime(tval))
                    rdict[key] = time.strftime("%Y-%m-%d %H:%M:%S GMT", gmt)
                else:
                    rdictp[key] = tval
            else:
                rdict[key] = doc[key]
    return rdict

class ActionMgr(object):
    def __init__(self, reqmgr):
        "Action manager"
        self.reqmgr = reqmgr
        self.cache = {} # cache which keeps actions, TODO make it persistent
        self.specs = {}

        self.example =  {
    "CMSSWVersion": "CMSSW_4_4_2_patch2",
    "GlobalTag": "FT_R_44_V11::All",
    "Campaign": "Campaign-OVERRIDE-ME",
    "RequestString": "RequestString-OVERRIDE-ME",
    "RequestPriority": 1000,
    "ScramArch": "slc5_amd64_gcc434",
    "RequestType": "ReReco",
    "RunWhitelist": [175866, 176933, 180250],
    "FilterEfficiency": 1,
    "RunBlacklist": [],
    "BlockWhitelist": [],
    "BlockBlacklist": [],
    "ConfigCacheUrl": "https://cmsweb-testbed.cern.ch/couchdb",
    "ConfigCacheID": "7559cf7a427be20436483f82fd108936",
    "PrepID": "RERECOTEST-0001",
    "Group": "DATAOPS",
    "SplittingAlgo": "LumiBased",
    "LumisPerJob": 8,
    "TimePerEvent": 60,
    "Memory": 2394,
    "SizePerEvent":500,
    "InputDataset": "/BTag/Run2011B-v1/RAW",
    "DbsUrl": "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader",
    "inputMode": "couchDB",
    "Scenario": "pp",
    "OpenRunningTimeout" : 0
    }

    def parse_data(self, jdict):
        "Parse json dictionary and align its values"
        # TODO: I need to convert strings to int's for some attributes, e.g. RunList, etc.
        for key, val in jdict.items():
            if  isinstance(val, basestring) and val.find(",") != -1: # comma list
                jdict[key] = val.split(",")
        print pprint.pformat(jdict)
        # TMP stuff to test insertion with example document
        doc = self.example
        for key in ['Campaign', 'RequestString']:
            if  key in jdict.keys():
                doc.update({key:jdict[key]})
        print pprint.pformat(doc)
        # END OF TMP
        return doc

    def create(self, req):
        """
        Create action:
        create new request and send it to Requset Manager via POST method
        """
        self.add_request('create', req)
        if  isinstance(req, dict):
            docs = [req]
        elif isinstance(req, list) or isinstance(req, GeneratorType):
            docs = req
        else:
            raise Exception('Unsupported request type')
        for jsondata in docs:
            doc = self.parse_data(jsondata)
            print "self.reqmgr.insertRequests(jsondata)"
            print pprint.pformat(doc)
            try:
                response = self.reqmgr.insertRequests(doc)
                print "### ActionMgr::create response", pprint.pformat(response)
            except Exception as exc:
                print "ERROR", str(exc)
                return 'fail'
        return 'ok'

    def approve(self, req, new_status):
        """
        Approve action
        should get list of requests to approve via Request::get(status)
        and change request status from new to status (assigned/rejected)
        """
        self.add_request('approve', req)
        status = req.get('status', '')
        docs = self.get_request_names(req)
        for rname in docs:
            print "self.reqmgr.updateRequestStatus(%s, %s)" % (rname, new_status)
            try:
                self.reqmgr.updateRequestStatus(rname, new_status)
            except Exception as exc:
                print "ERROR", str(exc)
                return 'fail'
        return 'ok'

    def assign(self, req, new_status, kwds):
        """
        Assign action
        should get list of requests to assign via Request::get(status)
        and change request status from assignment-approved to assigned/rejected.
        Additional parameters are passed via kwds dict.
        """
        self.add_request('assign', req)
        docs = self.get_request_names(req)
        req.update({"status": new_status})
        if  kwds and isinstance(kwds, dict):
            req.update(kwds)
        for rname in docs:
            print "self.reqmgr.updateRequestProperty(%s, %s)" % (rname, req)
            try:
                response = self.reqmgr.updateRequestProperty(rname, req)
                print "response", response
            except Exception as exc:
                print "ERROR", str(exc)
                return 'fail'
        return 'ok'

    def add_request(self, action, req):
        """
        Add request to internal cache
        """
        print "\n### add_request %s\n%s" % (action, pprint.pformat(req))

    def get_request_names(self, doc):
        "Extract request names from given documents"
        docs = []
        for key in doc.keys():
            if  key.startswith('request'):
                rid = key.split('request-')[-1]
                if  rid != 'all':
                    docs.append(rid)
                del doc[key]
        return docs

    def actions(self):
        "Return list of actions from the cache"
        return self.cache

class ReqMgrService(TemplatedPage):
    """
    Request Manager web service class
    """
    def __init__(self, app, config, mount):
        print "\n### Configuration:"
        print config
        self.base = config.base
        if  config and not isinstance(config, dict):
            web_config = config.dictionary_()
        if  not config:
            web_config = {'base': self.base}
        pprint.pprint(web_config)
        TemplatedPage.__init__(self, web_config)
        imgdir = os.environ.get('RM_IMAGESPATH', os.getcwd()+'/images')
        self.imgdir = web_config.get('imgdir', imgdir)
        cssdir = os.environ.get('RM_CSSPATH', os.getcwd()+'/css')
        self.cssdir = web_config.get('cssdir', cssdir)
        jsdir  = os.environ.get('RM_JSPATH', os.getcwd()+'/js')
        self.jsdir = web_config.get('jsdir', jsdir)
        # read scripts area and initialize data-ops scripts
        self.sdir = os.environ.get('RM_SCRIPTS', os.getcwd()+'/scripts')
        self.sdir = web_config.get('sdir', self.sdir)
        self.sdict_thr = web_config.get('sdict_thr', 600) # put reasonable 10 min interval
        self.sdict = {'ts':time.time()} # placeholder for data-ops scripts
        self.update_scripts(force=True)

        # To be filled at run time
        self.cssmap = {}
        self.jsmap  = {}
        self.imgmap = {}
        self.yuimap = {}

        # keep track of specs
        self.specs = {'StdBase': StdBase().getWorkloadArguments(),
                'ReReco': ReRecoWorkloadFactory().getWorkloadArguments(),
                'MonteCarlo': MonteCarloWorkloadFactory().getWorkloadArguments(),
                'StoreResults': StoreResultsWorkloadFactory().getWorkloadArguments(),
                'DataProcessing': DataProcessing().getWorkloadArguments(),
                'Resubmission': ResubmissionWorkloadFactory().getWorkloadArguments(),
                'ReDigi': ReDigiWorkloadFactory().getWorkloadArguments()}

        # Update CherryPy configuration
        mime_types  = ['text/css']
        mime_types += ['application/javascript', 'text/javascript',
                       'application/x-javascript', 'text/x-javascript']
        cherryconf.update({'tools.encode.on': True,
                           'tools.gzip.on': True,
                           'tools.gzip.mime_types': mime_types,
                          })
        self._cache    = {}

        # initialize rest API
        statedir = '/tmp'
        app = RESTMain(config, statedir) # REST application
        mount = '/rest' # mount point for cherrypy service
        api = RestApiHub(app, config.reqmgr, mount)
        # old access to reqmgr APIs
#        self.reqmgr = Request(app, api, config.reqmgr, mount=mount+'/reqmgr')

        # initialize access to reqmgr2 APIs
#        url = "https://localhost:8443/reqmgr2"
        self.reqmgr = ReqMgr(config.reqmgr.reqmgr2_url)

        # admin helpers
        self.admin_info = Info(app, api, config.reqmgr, mount=mount+'/info')
        self.admin_group = Group(app, api, config.reqmgr, mount=mount+'/group')
        self.admin_team = Team(app, api, config.reqmgr, mount=mount+'/team')

        # action manager (will be replaced with appropriate class
        self.actionmgr = ActionMgr(self.reqmgr)

    def user(self):
        """
        Return user name associated with this instance.
        This method should implement fetching user parameters through passed DN
        """
        return 'testuser'

    def user_dn(self):
        """
        Return user DN.
        This method should implement fetching user DN
        """
        return '/CN/bla/foo/'

    def update_scripts(self, force=False):
        "Update scripts dict"
        if  force or abs(time.time()-self.sdict['ts']) > self.sdict_thr:
            for item in os.listdir(self.sdir):
                with open(os.path.join(self.sdir, item), 'r') as istream:
                    self.sdict[item.split('.')[0]] = istream.read()
            self.sdict['ts'] = time.time()

    def abs_page(self, tmpl, content):
        """generate abstract page"""
        menu = self.templatepage('menu', menus=menus(tmpl))
        if  tmpl == 'main':
            body = self.templatepage('generic', menu=menu, content=content)
            page = self.templatepage('main', content=body, user=self.user())
        else:
            body = self.templatepage(tmpl, menu=menu, content=content)
            page = self.templatepage('main', content=body, user=self.user())
        return page

    def page(self, content):
        """
        Provide page wrapped with top/bottom templates.
        """
        return self.templatepage('main', content=content)

    def error(self, content):
        "Generate common error page"
        content = self.templatepage('error', content=content)
        return self.abs_page('generic', content)

    @expose
    def index(self, **kwds):
        """Main page"""
        apis = {}
        scripts = {}
        for idx in range(5):
            key = 'api_%s' % idx
            val = '%s description' % key
            apis[key] = val
            skey = 'script_%s' % idx
            sval = '%s description' % skey
            scripts[skey] = sval
        content = self.templatepage('apis', apis=apis, scripts=scripts)
        return self.abs_page('generic', content)

    ### Admin actions ###

    @expose
    def admin(self, **kwds):
        """admin page"""
        print "\n### ADMIN PAGE"
#        authz_fake()
        rows = self.admin_info.get()
        print "rows", [r for r in rows]

        content = self.templatepage('admin')
        return self.abs_page('generic', content)

    @expose
    def add_user(self, **kwds):
        """add_user action"""
        rid = genid(kwds)
        status = "ok" # chagne to whatever it would be
        content = self.templatepage('confirm', ticket=rid, user=self.user(), status=status)
        return self.abs_page('generic', content)

    @expose
    def add_group(self, **kwds):
        """add_group action"""
        rows = self.admin_group.get()
        print "\n### GROUPS", [r for r in rows]
        rid = genid(kwds)
        status = "ok" # chagne to whatever it would be
        content = self.templatepage('confirm', ticket=rid, user=self.user(), status=status)
        return self.abs_page('generic', content)

    @expose
    def add_team(self, **kwds):
        """add_team action"""
        rows = self.admin_team.get()
        print "\n### TEAMS", kwds, [r for r in rows]
        print "request to add", kwds
        rid = genid(kwds)
        status = "ok" # chagne to whatever it would be
        content = self.templatepage('confirm', ticket=rid, user=self.user(), status=status)
        return self.abs_page('generic', content)

    ### Request actions ###

    @expose
    @checkargs(['status', 'sort'])
    def assign(self, **kwds):
        """assign page"""
        if  not kwds:
            kwds = {}
        if  'status' not in kwds:
            kwds.update({'status': 'assignment-approved'})
        docs = []
        attrs = ['RequestName', 'RequestDate', 'Group', 'Requestor', 'RequestStatus']
        data = self.reqmgr.getRequestByStatus(statusList=[kwds['status']])
        for row in data:
            for key, val in row.items():
                docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = sort(docs, sortby)
        content = self.templatepage('assign', sort=sortby,
                site_white_list=site_white_list(),
                site_black_list=site_black_list(),
                cust_sites=cust_sites(), non_cust_sites=non_cust_sites(),
                auto_approve_sites=auto_approve_sites(),
                user=self.user(), user_dn=self.user_dn(), requests=docs,
                cmssw_versions=releases(), scram_arch=architectures(),
                sites=sites(), lfn_bases=lfn_bases(),
                lfn_unmerged_bases=lfn_unmerged_bases())
        return self.abs_page('generic', content)

    @expose
    @checkargs(['status', 'sort'])
    def approve(self, **kwds):
        """
        Approve page: get list of request associated with user DN.
        Fetch their status list from ReqMgr and display if requests
        were seen by data-ops.
        """
        if  not kwds:
            kwds = {}
        if  'status' not in kwds:
            kwds.update({'status': 'new'})
        kwds.update({'_nostale':True})
        docs = []
        attrs = ['RequestName', 'RequestDate', 'Group', 'Requestor', 'RequestStatus']
        data = self.reqmgr.getRequestByStatus(statusList=[kwds['status']])
        for row in data:
            for key, val in row.items():
                docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = sort(docs, sortby)
        content = self.templatepage('approve', requests=docs, date=tstamp(),
                sort=sortby)
        return self.abs_page('generic', content)

    @expose
    def ajax_action(self, action, ids, new_status, **kwds):
        """
        AJAX action creates request dictionary and pass it to
        action manager method.
        """
        req = {}
        if  isinstance(ids, list):
            for rid in ids:
                req[rid] = 'on'
        elif isinstance(ids, basestring):
            req[ids] = 'on'
        else:
            raise NotImplemented
        if  action == 'approve':
            status = getattr(self.actionmgr, action)(req, new_status)
        elif action == 'assign':
            status = getattr(self.actionmgr, action)(req, new_status, kwds)
        elif action == 'create':
            script = kwds.get('script', '')
            if  script:
                docs = self.generate_objs(script, kwds)
            else:
                docs = [kwds]
            status = getattr(self.actionmgr, action)(docs)
        else:
            raise NotImplemented()

    @expose
    def create(self, **kwds):
        """create page"""
        spec = kwds.get('form', 'ReReco')
        fname = 'json/%s' % str(spec)
        # request form
        jsondata = self.templatepage(fname,
                user=json.dumps(self.user()),
                dn=json.dumps(self.user_dn()),
                groups=json.dumps(cms_groups()),
                releases=json.dumps(releases()),
                arch=json.dumps(architectures()),
                scenarios=json.dumps(scenarios()),
                dqm_urls=json.dumps(dqm_urls()),
                couch_url=json.dumps(couch_url()),
                couch_dbname=json.dumps('some_db'), # TODO get elsewhere
                couch_wdbname=json.dumps('some_db'), # TODO get elsewhere
                dbs_url=json.dumps(dbs_urls()[0]),
                cc_url=json.dumps("https://cmsweb-testbed.cern.ch/couchdb"), # TODO: get elsewhere
                cc_id=json.dumps("some_id"), # TODO: get it elsewhere
                acdc_url=json.dumps("https://cmsweb-testbed.cern.ch/couchdb"), # TODO: get elsewhere
                acdc_dbname=json.dumps("some_db"), # TODO: get it elsewhere
                )
        try:
            jsondata = json.loads(jsondata)
        except Exception as exp:
            msg  = '<div class="color-gray">Fail to load JSON for %s workflow</div>\n' % spec
            msg += '<div class="color-red">Error: %s</div>\n' % str(exp)
            msg += '<div class="color-gray-light">JSON: %s</div>' % jsondata
            return self.error(msg)

        # check if JSON template provides all required attributes
        required = [k for k,v in self.specs[spec].items() if v['optional']==False]
        if  set(jsondata.keys()) & set(required) != set(required):
            missing = list(set(required)-set(jsondata.keys()))
            content = '%s spec template does not contain all required attributes.' \
                    % spec
            content += '<br/><span class="color-red">Missing attributes:</span> %s' \
                    % sort_bold(missing)
            return self.error(content)

        # check if JSON template contains all required values
        vdict = {} # dict of empty values
        tdict = {} # dict of type mismatches
        dropdowns = ['ScramArch', 'Group', 'CMSSWVersion']
        for key in required:
            value = jsondata[key]
            if  not value:
                vdict[key] = jsondata[key]
            type1 = str if type(value) == unicode else type(value)
            stype = self.specs[spec][key]['type']
            type2 = str if stype == unicode else stype
            if  type1 != type2 and key not in dropdowns:
                tdict[key] = (type(value), self.specs[spec][key]['type'])
        if  vdict.keys():
            content = '<span class="color-red">Empty values in %s spec</span>: %s'\
                    % (spec, sort_bold(vdict.keys()))
            return self.error(content)
        if  tdict.keys():
            types = []
            for key, val in tdict.items():
                type0 = str(val[0]).replace('>', '').replace('<', '')
                type1 = str(val[1]).replace('>', '').replace('<', '')
                types.append('<b>%s:</b> %s, should be %s' % (key, type0, type1))
            content = '<div class="color-red">Type mismatches in %s spec:</div> %s'\
                    % (spec, '<br/>'.join(types))
            return self.error(content)

        # create templatized page out of provided forms
        self.update_scripts()
        content = self.templatepage('create', table=json2table(jsondata, web_ui_names()),
                jsondata=json.dumps(jsondata, indent=2), name=spec,
                scripts=[s for s in self.sdict.keys() if s!='ts'])
        return self.abs_page('generic', content)

    @expose
    def confirm_action(self, **kwds):
        """
        Confirm action method is called from web UI forms. It grabs input parameters
        and passed them to Action manager.
        """
        try:
            action = kwds.pop('action')
            if  'script' in kwds: # we got a script to apply
                script = kwds.pop('script')
                jsondict = json.loads(kwds.get('jsondict', "{}"))
                if  not jsondict:
                    jsondict = kwds
                if  script == "":
                    docs = [jsondict]
                    rids = genid(jsondict)
                else:
                    docs = self.generate_objs(script, jsondict)
                    rids = [genid(o) for o in docs]
                status = getattr(self.actionmgr, action)(docs)
            else:
                status = getattr(self.actionmgr, action)(kwds)
                rids = genid(kwds)
            content = self.templatepage('confirm', ticket=rids, user=self.user(), status=status)
            return self.abs_page('generic', content)
        except:
            msg = '<div class="color-red">No action is specified</div>'
            self.error(msg)

    def generate_objs(self, script, jsondict):
        """Generate objects from givem JSON template"""
        self.update_scripts()
        code = self.sdict.get(script, '')
        if  code.find('def genobjs(jsondict)') == -1:
            return self.error("Improper python snippet, your code should start with <b>def genobjs(jsondict)</b> function")
        exec(code) # code snippet must starts with genobjs function
        return [r for r in genobjs(jsondict)]

    @exposejson
    def fetch(self, rid):
        "Fetch document for given id"
        return self.reqmgr.getRequestByNames(rid)

    def doc(self, rid):
        "Fetch document for given id"
        return self.reqmgr.getRequestByNames(rid)

    @expose
    def requests(self, **kwds):
        """Check status of requests"""
        if  not kwds:
            kwds = {}
        if  'status' not in kwds:
            kwds.update({'status': 'acquired'})
        results = self.reqmgr.getRequestByStatus(kwds['status'])
        docs = []
        for req in results:
            for key, doc in req.items():
                docs.append(request_attr(doc))
        sortby = kwds.get('sort', 'status')
        docs = sort(docs, sortby)
        content = self.templatepage('requests', requests=docs, sort=sortby)
        return self.abs_page('generic', content)

    @expose
    def request(self, **kwargs):
        "Get data example and expose it as json"
        dataset = kwargs.get('uinput', '')
        if  not dataset:
            return {'error':'no input dataset'}
        url = 'https://cmsweb.cern.ch/reqmgr/rest/outputdataset/%s' % dataset
        params = {}
        headers = {'Accept': 'application/json;text/json'}
        wdata = getdata(url, params)
        wdict = dict(date=time.ctime(), team='Team-A', status='Running', ID=genid(wdata))
        winfo = self.templatepage('workflow', wdict=wdict,
                dataset=dataset, code=pprint.pformat(wdata))
        content = self.templatepage('search', content=winfo)
        return self.abs_page('generic', content)

    ### Aux methods ###

    @expose
    def images(self, *args, **kwargs):
        """
        Serve static images.
        """
        args = list(args)
        self.check_scripts(args, self.imgmap, self.imgdir)
        mime_types = ['*/*', 'image/gif', 'image/png',
                      'image/jpg', 'image/jpeg']
        accepts = cherrypy.request.headers.elements('Accept')
        for accept in accepts:
            if  accept.value in mime_types and len(args) == 1 \
                and args[0] in self.imgmap:
                image = self.imgmap[args[0]]
                # use image extension to pass correct content type
                ctype = 'image/%s' % image.split('.')[-1]
                cherrypy.response.headers['Content-type'] = ctype
                return serve_file(image, content_type=ctype)

    def serve(self, kwds, imap, idir, datatype='', minimize=False):
        "Serve files for high level APIs (yui/css/js)"
        args = []
        for key, val in kwds.items():
            if  key == 'f': # we only look-up files from given kwds dict
                if  isinstance(val, list):
                    args += val
                else:
                    args.append(val)
        scripts = self.check_scripts(args, imap, idir)
        return self.serve_files(args, scripts, imap, datatype, minimize)

    @exposecss
    @tools.gzip()
    def css(self, **kwargs):
        """
        Serve provided CSS files. They can be passed as
        f=file1.css&f=file2.css
        """
        resource = kwargs.get('resource', 'css')
        if  resource == 'css':
            return self.serve(kwargs, self.cssmap, self.cssdir, 'css', True)

    @exposejs
    @tools.gzip()
    def js(self, **kwargs):
        """
        Serve provided JS scripts. They can be passed as
        f=file1.js&f=file2.js with optional resource parameter
        to speficy type of JS files, e.g. resource=yui.
        """
        resource = kwargs.get('resource', 'js')
        if  resource == 'js':
            return self.serve(kwargs, self.jsmap, self.jsdir)

    def serve_files(self, args, scripts, resource, datatype='', minimize=False):
        """
        Return asked set of files for JS, YUI, CSS.
        """
        idx = "-".join(scripts)
        if  idx not in self._cache.keys():
            data = ''
            if  datatype == 'css':
                data = '@CHARSET "UTF-8";'
            for script in args:
                path = os.path.join(sys.path[0], resource[script])
                path = os.path.normpath(path)
                ifile = open(path)
                data = "\n".join ([data, ifile.read().\
                    replace('@CHARSET "UTF-8";', '')])
                ifile.close()
            if  datatype == 'css':
                set_headers("text/css")
            if  minimize:
                self._cache[idx] = minify(data)
            else:
                self._cache[idx] = data
        return self._cache[idx]

    def check_scripts(self, scripts, resource, path):
        """
        Check a script is known to the resource map
        and that the script actually exists
        """
        for script in scripts:
            if  script not in resource.keys():
                spath = os.path.normpath(os.path.join(path, script))
                if  os.path.isfile(spath):
                    resource.update({script: spath})
        return scripts
