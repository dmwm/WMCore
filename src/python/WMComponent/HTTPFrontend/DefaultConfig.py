import socket
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.10 2009/04/11 01:09:12 rpw Exp $"
__version__ = "$Revision: 1.10 $"

from os import environ

from WMCore.Configuration import Configuration

config = Configuration()


config.component_('HTTPFrontEnd')

# This component has all the configuration of CherryPy
config.component_('Webtools')
config.Webtools.host = 'cmssrv49.fnal.gov'
config.Webtools.port = 8585
# This is the application
config.Webtools.application = 'HTTPFrontEnd'
views = config.HTTPFrontEnd.section_('views')
active = views.section_('active')

active.section_('download')
active.download.object = 'WMCore.HTTPFrontEnd.Downloader'
active.download.dir = '/home/rpw/work'

#active.section_('assignmentManager')
#active.assignmentManager.object = 'ReqMgr.Component.AssignmentManager.AssignmentManager'
#active.assignmentManager.requestSpecDir= active.download.dir
#active.assignmentManager.Host = config.Webtools.host
#active.assignmentManager.Port = config.Webtools.port

active.section_('requestDataService')
active.requestDataService.object = 'ReqMgr.Component.RequestDataService.RequestDataService'

active.section_('requestListener')
active.requestListener.object = 'ReqMgr.Component.RequestListener.RequestListener'

active.section_('CmsDriverWebRequest')
active.CmsDriverWebRequest.object = 'ReqMgr.RequestInterface.WWW.CmsDriverWebRequest'
active.CmsDriverWebRequest.cmsswInstallation = '/uscmst1/prod/sw/cms/slc4_ia32_gcc345/cms/cmssw'
active.CmsDriverWebRequest.cmsswDefaultVersion = 'CMSSW_2_2_3'
