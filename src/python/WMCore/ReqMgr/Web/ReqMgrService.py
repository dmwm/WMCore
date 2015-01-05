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
from WMCore.ReqMgr.Tools.cms import releases, architectures
from WMCore.ReqMgr.Tools.cms import scenarios, cms_groups, couch_url
from WMCore.ReqMgr.Tools.cms import web_ui_names, next_status, sites
from WMCore.ReqMgr.Tools.cms import lfn_bases, lfn_unmerged_bases
from WMCore.ReqMgr.Tools.cms import site_white_list, site_black_list

# WMCore modules
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.ReqMgr.Service.Auxiliary import Info, Group, Team, Software
from WMCore.ReqMgr.Service.Request import Request
from WMCore.ReqMgr.Service.RestApiHub import RestApiHub
from WMCore.REST.Main import RESTMain

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

def menus():
    "Return dict of menus"
    items = ['admin', 'create', 'approve', 'assign', 'requests']
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
                    rdictp[key] = tval
            else:
                rdict[key] = doc[key]
    return rdict

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

        # initialize access to reqmgr2 APIs
#        url = "https://localhost:8443/reqmgr2"
        self.reqmgr = ReqMgr(config.reqmgr.reqmgr2_url)

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
        self.sw_ver = cdict.get('default_sw_version', 'CMSSW_5_2_5')
        self.sw_arch = cdict.get('default_sw_scramarch', 'slc5_amd64_gcc434')

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
        return self.abs_page('main', content)

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
        for row in data:
            for key, val in row.items():
                docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        content = self.templatepage('assign', sort=sortby,
                site_white_list=site_white_list(),
                site_black_list=site_black_list(),
                user=self.user(), user_dn=self.user_dn(), requests=docs,
                cmssw_versions=releases(), scram_arch=architectures(),
                sites=sites(), lfn_bases=lfn_bases(),
                lfn_unmerged_bases=lfn_unmerged_bases())
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
        for row in data:
            for key, val in row.items():
                docs.append(request_attr(val, attrs))
        sortby = kwds.get('sort', 'status')
        docs = [r for r in sort(docs, sortby)]
        content = self.templatepage('approve', requests=docs, date=tstamp(),
                sort=sortby)
        return self.abs_page('approve', content)

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
                releases=json.dumps(self.sw_ver),
                arch=json.dumps(self.sw_arch),
                scenarios=json.dumps(scenarios()),
                dqm_urls=json.dumps(self.dqm_url),
                couch_url=json.dumps(self.couch_url),
                couch_dbname=json.dumps(self.couch_dbname),
                couch_wdbname=json.dumps(self.couch_wdbname),
                dbs_url=json.dumps(self.dbs_url),
                cc_url=json.dumps(self.configcache_url),
                cc_id=json.dumps("REPLACE-ID"),
                acdc_url=json.dumps(self.acdc_url),
                acdc_dbname=json.dumps(self.acdc_dbname),
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
        return self.abs_page('create', content)

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
