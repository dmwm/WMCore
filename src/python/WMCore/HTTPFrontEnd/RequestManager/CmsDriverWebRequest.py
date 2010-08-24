import cherrypy
import sys
import os
import tempfile
import pickle
import subprocess
from WMCore.RequestManager.RequestMaker.ConfigUpload import uploadConfigFile
from WMCore.Services.Requests import JSONRequests
from WMCore.WebTools.Page import TemplatedPage


class CmsDriverWebRequest(TemplatedPage):
    def __init__(self, config):
        self.templatedir = __file__.rsplit('/', 1)[0]
        self.jsonSender = JSONRequests(config.reqMgrHost)
        self.cmsswInstallation = config.cmsswInstallation
        self.cmsswVersion = config.cmsswDefaultVersion
        self.configCache = config.configCacheUrl
        self.componentDir = config.componentDir
        self.urlPrefix = '%s/download?filepath=' % config.reqMgrHost
        self.versions = []
        cherrypy.config.update({'tools.sessions.on': True})


    def cmsswArea(self, cmsswVersion, arch):
        area = self.cmsswInstallation+'/'+arch+'/cms/cmssw/'+cmsswVersion
        if os.path.isdir(area):
            return area
        raise RuntimeError("Cannot find installation for CMSSW version " + cmsswVersion)
        
    def loadVersion(self, cmsswVersion):
        curdir = os.curdir
        os.chdir(self.cmsswArea(cmsswVersion))
        os.system('cmsenv')
        os.chdir(curdir)
         


    def index(self, gen=False, reco=False):
        schema = cherrypy.session.get("schema", None)
        if schema == None:
           raise RuntimeError("Cannot find schema")
        # find all the Generator cfis
        area = self.cmsswArea(schema['CMSSWVersion'], schema['ScramArch'])
        genfiles = os.listdir(area+'/src/Configuration/Generator/python')
        gencfis =  [f for f in genfiles if f.endswith('cfi.py')]
        gencfis.sort()

        if self.versions == []:
            self.versions = self.jsonSender.get('/reqMgr/version')[0]
            self.versions.sort()

        return self.templatepage("CmsDriverWebRequest", versions=self.versions, gencfis=gencfis,
            cmsswVersion=schema['CMSSWVersion'], defaultArch=schema['ScramArch'], number=schema["RequestSizeEvents"], 
            filein=schema["InputDataset"], outfile_name=schema["UnmergedLFNBase"],
            requestName=schema["RequestName"], n=schema["RequestSizeEvents"], genchecked=gen, recochecked=reco)
    index.exposed = True

    def CreateJob(self, **kwargs):
        # will be used to send the pickled kwargs, and 
        # retrieve the output
        #tmpfile = tempfile.NamedTemporaryFile(suffix=".py")
        schema = cherrypy.session.get("schema", None)
        if schema == None:
           raise RuntimeError("Cannot find schema again")

        tmpfilename = '/tmp/tmp.py'
        tmpfile = open('/tmp/tmp.py', 'w+')
        pickle.dump(kwargs, tmpfile)
        tmpfile.seek(0)
        print "REQMANEM " + kwargs['requestName']
        configLFN = kwargs['requestName']+'_cfg.py'
        configPFN = self.componentDir+'/'+configLFN
        area = self.cmsswArea(kwargs['version'], kwargs['arch'])
        cmd = ['runCmsDriver.sh', area, tmpfile.name, configPFN]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        retcode = proc.wait()

        stdout =  proc.stdout.read()
        stderr =  proc.stderr.read()
        if retcode != 0:
            return  "<pre>" + stdout + stderr
        stdout =  proc.stdout.read()
        stderr =  proc.stderr.read()
        if kwargs.has_key('showConfig'):
            # the tempfile gets overwritten
            outfile = open(configPFN, 'r')
            result = "<pre>" + outfile.read()
            outfile.close()
            return result
        elif kwargs.has_key('upload'):
            try:
                #cherrypy.session['configCacheID'] = uploadConfigFile(tmpfilename, self.configCache)
                cherrypy.session.schema['ProcessingConfig'] = self.urlPrefix+configLFN
                raise cherrypy.HTTPRedirect('../submit')
            except RuntimeError, e:
                return "<pre>" + str(e)
    CreateJob.exposed = True

