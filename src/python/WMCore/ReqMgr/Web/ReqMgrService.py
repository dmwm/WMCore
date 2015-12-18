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
from WMCore.ReqMgr.Web.utils import json2table, json2form, genid, checkargs, tstamp, sort
from WMCore.ReqMgr.Utils.url_utils import getdata
from WMCore.ReqMgr.Tools.cms import releases, architectures
from WMCore.ReqMgr.Tools.cms import scenarios, cms_groups, couch_url
from WMCore.ReqMgr.Tools.cms import web_ui_names, next_status, sites
from WMCore.ReqMgr.Tools.cms import lfn_bases, lfn_unmerged_bases
from WMCore.ReqMgr.Tools.cms import site_white_list, site_black_list

# WMCore modules
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.ReqMgr.Service.Auxiliary import Info, Group, Team, Software
from WMCore.ReqMgr.Utils.Validation import get_request_template_from_type
from WMCore.ReqMgr.Service.Request import Request
from WMCore.ReqMgr.Service.RestApiHub import RestApiHub
from WMCore.ReqMgr.DataStructs.RequestStatus import get_modifiable_properties
from WMCore.REST.Main import RESTMain
from WMCore.Services.LogDB.LogDB import LogDB
# import WMCore itself to determine path of modules
import WMCore

# new reqmgr2 APIs
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_START_STATE, REQUEST_STATE_TRANSITION
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS

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

def menus():
    "Return dict of menus"
    items = ['home', 'admin', 'create', 'approve', 'assign', 'batches']
    return items

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
                    rdict[key] = tval
            else:
                rdict[key] = doc[key]
    return rdict

def spec_list(root, spec_path):
    "Return list of specs from given root directory"
    specs = []
    for fname in os.listdir(root):
        if  not fname.endswith('.py') or fname == '__init__.py':
            continue
        sname = fname.split('.')[0]
        clsName = "%sWorkloadFactory" % sname
        if  clsName in open(os.path.join(root, fname)).read():
            specs.append(sname)
    return specs

class ReqMgrService(TemplatedPage):
    """
    Request Manager web service class
    """
    def __init__(self, app, config, mount):
        self.base = config.base
        self.rootdir = '/'.join(WMCore.__file__.split('/')[:-1])
        if  config and not isinstance(config, dict):
            web_config = config.dictionary_()
        if  not config:
            web_config = {'base': self.base}
        TemplatedPage.__init__(self, web_config)
        imgdir = os.environ.get('RM_IMAGESPATH', os.getcwd()+'/images')
        self.imgdir = web_config.get('imgdir', imgdir)
        cssdir = os.environ.get('RM_CSSPATH', os.getcwd()+'/css')
        self.cssdir = web_config.get('cssdir', cssdir)
        jsdir  = os.environ.get('RM_JSPATH', os.getcwd()+'/js')
        self.jsdir = web_config.get('jsdir', jsdir)
        spdir  = os.environ.get('RM_SPECPATH', os.getcwd()+'/specs')
        self.spdir = web_config.get('spdir', spdir)
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

        std_specs_dir = os.path.join(self.rootdir, 'WMSpec/StdSpecs')
        self.std_specs = spec_list(std_specs_dir, 'WMSpec.StdSpecs')
        self.std_specs.sort()

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

        # initialize access to reqmgr2 APIs
        self.reqmgr = ReqMgr(config.reqmgr.reqmgr2_url)
        # only gets current view (This might cause to reponse time much longer, 
        # If upto date view is not needed overwrite Fale)
        self.reqmgr._noStale = True

        # admin helpers
        self.admin_info = Info(app, api, config.reqmgr, mount=mount+'/info')
        self.admin_group = Group(app, api, config.reqmgr, mount=mount+'/group')
        self.admin_team = Team(app, api, config.reqmgr, mount=mount+'/team')

        # get fields which we'll use in templates
        cdict = config.reqmgr.dictionary_()
        self.couch_url = cdict.get('couch_host', '')
        self.couch_dbname = cdict.get('couch_reqmgr_db', '')
        self.couch_wdbname = cdict.get('couch_workload_summary_db', '')
        self.acdc_url = cdict.get('acdc_host', '')
        self.acdc_dbname = cdict.get('acdc_db', '')
        self.configcache_url = cdict.get('couch_config_cache_url', self.couch_url)
        self.dbs_url = cdict.get('dbs_url', '')
        self.dqm_url = cdict.get('dqm_url', '')
        self.sw_ver = cdict.get('default_sw_version', 'CMSSW_7_6_1')
        self.sw_arch = cdict.get('default_sw_scramarch', 'slc6_amd64_gcc493')

        # LogDB holder
        centralurl = cdict.get("central_logdb_url", "")
        identifier = cdict.get("log_reporter", "reqmgr2")
        self.logdb = LogDB(centralurl, identifier)

    def user(self):
        """
        Return user name associated with this instance.
        """
        try:
            return cherrypy.request.user['login']
        except:
            return 'testuser'

    def user_dn(self):
        "Return user DN"
        try:
            return cherrypy.request.user['dn']
        except:
            return '/CN/bla/foo'

    def update_scripts(self, force=False):
        "Update scripts dict"
        if  force or abs(time.time()-self.sdict['ts']) > self.sdict_thr:
            for item in os.listdir(self.sdir):
                with open(os.path.join(self.sdir, item), 'r') as istream:
                    self.sdict[item.split('.')[0]] = istream.read()
            self.sdict['ts'] = time.time()

    def abs_page(self, tmpl, content):
        """generate abstract page"""
        menu = self.templatepage('menu', menus=menus(), tmpl=tmpl)
        body = self.templatepage('generic', menu=menu, content=content)
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
        return self.abs_page('error', content)

    @expose
    def index(self, **kwds):
        """Main page"""
        content = self.templatepage('index', requests=ACTIVE_STATUS, rdict=REQUEST_STATE_TRANSITION)
        return self.abs_page('main', content)

    @expose
    def home(self, **kwds):
        """Main page"""
        return self.index(**kwds)

    ### Admin actions ###

    @expose
    def admin(self, **kwds):
        """admin page"""
        print "\n### ADMIN PAGE"
        rows = self.admin_info.get()
        print "rows", [r for r in rows]

        content = self.templatepage('admin')
        return self.abs_page('admin', content)

    @expose
    def add_user(self, **kwds):
        """add_user action"""
        rid = genid(kwds)
        status = "ok" # chagne to whatever it would be
        content = self.templatepage('confirm', ticket=rid, user=self.user(), status=status)
        return self.abs_page('admin', content)

    @expose
    def add_group(self, **kwds):
        """add_group action"""
        rows = self.admin_group.get()
        print "\n### GROUPS", [r for r in rows]
        rid = genid(kwds)
        status = "ok" # chagne to whatever it would be
        content = self.templatepage('confirm', ticket=rid, user=self.user(), status=status)
        return self.abs_page('admin', content)

    @expose
    def add_team(self, **kwds):
        """add_team action"""
        rows = self.admin_team.get()
        print "\n### TEAMS", kwds, [r for r in rows]
        print "request to add", kwds
        rid = genid(kwds)
        status = "ok" # chagne to whatever it would be
        content = self.templatepage('confirm', ticket=rid, user=self.user(), status=status)
        return self.abs_page('admin', content)

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
        for key, val in data.items():
            docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        misc_json = {'CMSSW Releases':releases(),
                'CMSSW architectures':architectures(),
                'SubscriptionPriority':['Low', 'Normal', 'High'],
                'CustodialSubType':['Move', 'Replica'],
                'NonCustodialSubType':['Move', 'Replica'],
                'MinMergeSize':2147483648,
                'MaxMergeSize':4294967296,
                'MaxMergeEvents':50000,
                'MaxRSS':20411724,
                'MaxVSize':20411724,
                'SoftTimeout':129600,
                'GracePeriod':300,
                'BlockCloseMaxWaitTime':66400,
                'BlockCloseMaxFiles':500,
                'BlockCloseMaxEvents':250000000,
                'BlockCloseMaxSize':5000000000000,
                'AcquisitionEra':'',
                'ProcessingVersion':1,
                'ProcessingString':'',
                'MergedLFNBase':lfn_bases(),
                'UnmergedLFNBase':lfn_unmerged_bases(),}
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('assign', sort=sortby,
                filter_sort_table=filter_sort,
                sites=sites(),
                site_white_list=site_white_list(),
                site_black_list=site_black_list(),
                user=self.user(), user_dn=self.user_dn(), requests=docs,
                misc_table=json2table(misc_json, web_ui_names()),
                misc_json=json2form(misc_json, indent=2, keep_first_value=True))
        return self.abs_page('assign', content)

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
        for key, val in data.items():
            docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('approve', requests=docs, date=tstamp(),
                sort=sortby, filter_sort_table=filter_sort)
        return self.abs_page('approve', content)

    @expose
    def create(self, **kwds):
        """create page"""
        # get list of standard specs from WMCore and new ones from local area
        #loc_specs_dir = os.path.join(self.spdir, 'Specs') # local specs
        #loc_specs = spec_list(loc_specs_dir, 'Specs')
        #all_specs = list(set(self.std_specs + loc_specs))
        #all_specs.sort()
        all_specs = self.std_specs
        spec = kwds.get('form', '')
        if  not spec:
            spec = self.std_specs[0]
        # make spec first in all_specs list
        if  spec in all_specs:
            all_specs.remove(spec)
        all_specs = [spec] + all_specs
        jsondata = get_request_template_from_type(spec)
        # create templatized page out of provided forms
        self.update_scripts()
        content = self.templatepage('create', table=json2table(jsondata, web_ui_names()),
                jsondata=json2form(jsondata, indent=2, keep_first_value=True), name=spec,
                scripts=[s for s in self.sdict.keys() if s!='ts'],
                specs=all_specs)
        return self.abs_page('create', content)

    def generate_objs(self, script, jsondict):
        """Generate objects from givem JSON template"""
        self.update_scripts()
        code = self.sdict.get(script, '')
        if  code.find('def genobjs(jsondict)') == -1:
            return self.error("Improper python snippet, your code should start with <b>def genobjs(jsondict)</b> function")
        exec(code) # code snippet must starts with genobjs function
        return [r for r in genobjs(jsondict)]

    @expose
    def fetch(self, rid, **kwds):
        "Fetch document for given id"
        rid = rid.replace('request-', '')
        doc = self.reqmgr.getRequestByNames(rid)
        transitions = []
        tstamp = time.time()
        if len(doc) == 1:
            try:
                doc = doc[rid]
            except:
                pass
            name = doc.get('RequestName', 'NA')
            title = 'Request %s' % name
            status = doc.get('RequestStatus', '')
            transitions = REQUEST_STATE_TRANSITION.get(status, [])
            if  status in transitions:
                transitions.remove(status)
            visible_attrs = get_modifiable_properties(status)
            content = self.templatepage('doc', title=title, status=status, name=name,
                    table=json2table(doc, web_ui_names(), visible_attrs),
                    jsondata=json2form(doc, indent=2, keep_first_value=False),
                    transitions=transitions, ts=tstamp, user=self.user(), userdn=self.user_dn())
        elif len(doc) > 1:
            jsondata = [pprint.pformat(d) for d in doc]
            content = self.templatepage('doc', title='Series of docs: %s' % rid,
                    table="", jsondata=jsondata,
                    transitions=transitions, ts=tstamp, user=self.user(), userdn=self.user_dn())
        else:
            doc = 'No request found for name=%s' % rid
        return self.abs_page('request', content)

    @expose
    def record2logdb(self, **kwds):
        """LogDB submission page"""
        print(kwds)
        request = kwds['request']
        msg = kwds['message']
        self.logdb.post(request, msg)
        msg = '<h6>Confirmation</h6>Your request has been entered to LogDB.'
        return self.abs_page('generic', msg)

    @expose
    def requests(self, **kwds):
        """Page showing requests"""
        if  not kwds:
            kwds = {}
        if  'status' not in kwds:
            kwds.update({'status': 'acquired'})
        results = self.reqmgr.getRequestByStatus(kwds['status'])
        docs = []
        for key, doc in results.items():
            docs.append(request_attr(doc))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('requests', requests=docs, sort=sortby,
                status=kwds['status'], filter_sort_table=filter_sort)
        return self.abs_page('requests', content)

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
        return self.abs_page('request', content)

    @expose
    def batch(self, **kwds):
        """batch page"""
        # TODO: we need a template for batch attributes
        #       and read it from separate area, like DASMaps
        name = kwds.get('name', '')
        batch = {}
        if  name:
#            batch = self.reqmgr.getBatchesByName(name)
            batch = {'Name':'Batch1', 'Description': 'Bla-bla', 'Creator':'valya', 'Group':'test',
                    'Workflows':['workflow1', 'workflow2'],
                    'Attributes':{'HeavyIon':['true', 'false']}}
        attributes = batch.get('Attributes', {})
        workflows = batch.get('Workflows', [])
        description = batch.get('Description', '')
        creator = batch.get('Creator', self.user_dn())
        content = self.templatepage('batch', name=name,
                attributes=json2table(attributes, web_ui_names()),
                workflows=workflows, creator=creator,
                description=description)
        return self.abs_page('batch', content)

    @expose
    def batches(self, **kwds):
        """Page showing batches"""
        if  not kwds:
            kwds = {}
        if  'name' not in kwds:
            kwds.update({'name': ''})
        sortby = kwds.get('sort', 'name')
#        results = self.reqmgr.getBatchesByName(kwds['name'])
        results = [
                {'Name':'Batch1', 'Description': 'Bla-bla', 'Creator':'valya', 'Group':'test',
                    'Workflows':['workflow1', 'workflow2'],
                    'Date': 'Fri Feb 13 10:36:41 EST 2015',
                    'Attributes':{'HeavyIon':['true', 'false']}},
                {'Name':'Batch2', 'Description': 'lksdjflksjdf', 'Creator':'valya', 'Group':'test',
                    'Workflows':['workflow1', 'workflow2'],
                    'Date': 'Fri Feb 10 10:36:41 EST 2015',
                    'Attributes':{'HeavyIon':['true', 'false']}},
                   ]
        docs = [r for r in sort(results, sortby)]
        filter_sort = self.templatepage('filter_sort')
        content = self.templatepage('batches', batches=docs, sort=sortby,
                filter_sort_table=filter_sort)
        return self.abs_page('batches', content)

    ### Aux methods ###

    @expose
    def put_request(self, *args, **kwds):
        "PUT request callback to reqmgr server, should be used in AJAX"
        reqname = kwds.get('RequestName', '')
        status = kwds.get('RequestStatus', '')
        if  not reqname:
            msg = 'Unable to update request status, empty request name'
            raise cherrypy.HTTPError(406, msg)
        if  not status:
            msg = 'Unable to update request status, empty status value'
            raise cherrypy.HTTPError(406, msg)
        return self.reqmgr.updateRequestStatus(reqname, status)

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
