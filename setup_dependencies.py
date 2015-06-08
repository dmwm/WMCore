#!/usr/bin/env python
"""
Manage dependancies by declaring systems here.
A system can depend on packages or other systems.
If a package ends with a + include all subpackages.
"""
dependencies = {'wmc-rest':{
                        'bin': ['wmc-dist-patch', 'wmc-dist-unpatch','wmc-httpd'],
                        'packages' : ['WMCore.REST'],
                        'modules': ['WMCore.Configuration'],
                        'systems':['wmc-base']
                        },
                'wmc-base':{
                        'bin': ['wmc-dist-patch', 'wmc-dist-unpatch'],
                        'packages' : ['WMCore.DataStructs'],
                        'modules': ['WMCore.WMFactory', 'WMCore.WMException', 'WMCore.Configuration',
                                    'WMCore.WMExceptions', 'WMCore.WMFactory', 'WMCore.Lexicon',
                                    'WMCore.WMBase', 'WMCore.WMLogging', 'WMCore.Algorithms.Permissions'],
                        },
                'wmc-component':{
                        'packages': ['WMCore.MsgService', 'WMCore.WorkerThreads', 'WMCore.Alerts+', 'WMCore.ThreadPool'],
                        'modules': ['WMComponent.__init__'],
                        'systems': ['wmc-base']
                        },
                'wmc-database':{
                        'packages': ['WMCore.Wrappers+', 'WMCore.GroupUser', 'WMCore.DataStructs', 'WMCore.Database',
                                    'WMCore.Algorithms', 'WMCore.Services', 'WMCore.Cache'],
                        'modules': ['WMCore.Action', 'WMCore.WMConnectionBase', 'WMCore.DAOFactory', 'WMCore.WMInit'],
                        'systems':['wmc-base']
                        },
                'wmc-runtime':{
                        'packages': ['WMCore.WMRuntime+', 'WMCore.WMSpec+', 'PSetTweaks', 'WMCore.FwkJobReport', 'WMCore.Storage+'],
                        'modules' : ['WMCore.Algorithms.ParseXMLFile'],
                        'systems':['wmc-base']
                        },
                'wmc-web':{
                        'packages': ['WMCore.WebTools', 'WMCore.Agent+', 'WMCore.WorkerThreads', 'WMCore.Alerts+'],
                        'systems':['wmc-database', 'wmc-base'],
                        'statics': ['src/javascript/WMCore/WebTools',
                                'src/javascript/external/yui',
                                'src/css/WMCore/WebTools',
                                'src/css/WMCore/WebTools/Masthead',
                                'src/css/external/yui',
                                'src/templates/WMCore/WebTools',
                                'src/templates/WMCore/WebTools/Masthead',]
                        },
                'reqmgr':{
                        'packages': ['WMCore.RequestManager+',
                                     'WMCore.HTTPFrontEnd',
                                     'WMCore.HTTPFrontEnd.RequestManager+',
                                     'WMCore.HTTPFrontEnd.GlobalMonitor+',
                                     'WMCore.Services.RequestManager',
                                     'WMCore.Services+',
                                     'WMCore.ACDC'],

                        'systems':['wmc-web', 'wmc-runtime'],
                        'statics': ['src/templates/WMCore/WebTools/RequestManager',
                                    'src/html/GlobalMonitor',
                                    'src/javascript/WMCore/WebTools/GlobalMonitor+',
                                    'src/html/RequestManager',
                                    'src/couchapps/ReqMgr+',
                                    'src/couchapps/ConfigCache+',
                                    'src/couchapps/WMStats+'],
                        },
                'reqmgr2':{
                        'packages': ['WMCore.ReqMgr+',
                                     'WMCore.WMDataMining+',
                                     'WMCore.Services+',
                                     'WMCore.ACDC'
                                    ],
                        'systems': ['wmc-rest', 'wmc-runtime', 'wmc-database'],
                        'statics': ['src/couchapps/ReqMgr+',
                                    'src/couchapps/ReqMgrAux+',
                                    'src/couchapps/ConfigCache+',
                                    'src/couchapps/WMStats+',
                                    'src/couchapps/WMDataMining+',
                                    'src/html/ReqMgr+'
                                   ],
                          },
                'workqueue':{
                        'packages': ['WMCore.WorkQueue+', 'WMCore.Wrappers+',
                                     'WMCore.Services+',
                                     'WMCore.WMSpec', 'WMCore.WMSpec.Steps', 'WMCore.WMSpec.Steps.Templates',
                                     'WMCore.ACDC', 'WMCore.GroupUser', 'WMCore.Alerts', 'WMCore.ReqMgr.DataStruct+'],
                        'modules' : ['WMCore.Algorithms.__init__', 'WMCore.Algorithms.Permissions',
                                     'WMCore.Algorithms.MiscAlgos', 'WMCore.Algorithms.ParseXMLFile',
                                     'WMCore.Database.__init__', 'WMCore.Database.CMSCouch',
                                     'WMCore.Database.CouchUtils'],
                        'systems': ['wmc-base'],
                        'statics': ['src/couchapps/WorkQueue+'],
                        },
                'wmagent':{
                        'packages': ['WMCore.Agent+', 'WMCore.Algorithms+',
                                    'WMCore.JobStateMachine', 'WMComponent+',
                                    'WMCore.HTTPFrontEnd+', 'WMCore.ThreadPool',
                                    'WMCore.BossAir+', 'WMCore.Credential',
                                    'WMCore.JobSplitting+', 'WMCore.ProcessPool',
                                    'WMCore.Services+', 'WMCore.WMSpec+',
                                    'WMCore.WMBS+', 'WMCore.ResourceControl+'],
                        'systems':['wmc-web', 'wmc-database', 'workqueue', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/Agent',
                                    'src/javascript/WMCore/WebTools/WMBS',
                                    'src/javascript/external/graphael',
                                    'src/templates/WMCore/WebTools/WMBS'],
                        },
                'asyncstageout':{
                        'packages': ['WMCore.Agent+', 'WMCore.Storage+', 'WMComponent.DBSUpload',
                                     'WMCore.Credential', 'WMCore.WorkerThreads', 'WMCore.Services.SiteDB+',
                                     'WMCore.Services.PhEDEx+', 'WMCore.ACDC', 'WMCore.Alerts+',
                                     'WMCore.Services.UserFileCache'],
                        'modules': ['WMQuality.TestInitCouchApp', 'WMCore.Services.Service',
                                    'WMCore.Services.pycurl_manager', 'WMComponent.__init__'],
                        'systems': ['wmc-database'],
                        'statics': ['src/couchapps/Agent+'],
                        },
                'crabcache':{
                        'systems': ['wmc-rest'],
                        },
                'crabserver':{
                        'packages': ['WMCore.Credential', 'WMCore.Services+', 'WMCore.RequestManager+',
                                     'WMCore.WMSpec+', 'WMCore.HTTPFrontEnd+', 'WMCore.ACDC'],
                        'modules' : ['WMCore.DataStructs.LumiList'],
                        'systems' : ['wmc-rest', 'wmc-database'],
                        },
                'crabclient':{
                        'packages': ['WMCore.Wrappers+', 'WMCore.Credential', 'PSetTweaks', 'WMCore.Services.UserFileCache+', 'WMCore.Services.SiteDB+', 'WMCore.Services.PhEDEx+'],
                        'systems': ['wmc-base'],
                        'modules': ['WMCore.FwkJobReport.FileInfo', 'WMCore.Services.Requests', 'WMCore.DataStructs.LumiList',
                                    'WMCore.Services.Service', 'WMCore.Services.pycurl_manager', 'WMCore.Services.EmulatorSwitch'],
                        },
                'crabtaskworker':{
                        'packages':['WMCore.WorkQueue', 'WMCore.Credential', 'WMCore.Algorithms+', 'WMCore.WMSpec+',
                                    'WMCore.JobSplitting', 'WMCore.Services.SiteDB+', 'WMCore.Services.DBS+',
                                    'WMCore.Services.UserFileCache+', 'WMCore.Services.PhEDEx+'],
                        'modules': ['WMCore.WMBS.File', 'WMCore.WMBS.WMBSBase', 'WMCore.WMBS.__init__'],
                        'systems': ['wmc-database', 'wmc-runtime'],
                        },
                'wmclient':{
                        'systems': ['wmc-runtime', 'wmc-database']
                        },
                'reqmon':{
                        'statics': ['src/couchapps/WMStats+',
                                    'src/couchapps/WMStatsErl+',
                                    'src/couchapps/WorkloadSummary+',
                                    'src/couchapps/T0Request+',
                                    'src/couchapps/LogDB+'],
                        },
                'alertscollector':
                {
                        'statics': ['src/couchapps/AlertsCollector+'],
                },
                'acdcserver': {
                        'packages': ['WMCore.ACDC', 'WMCore.GroupUser', 'WMCore.DataStructs',
                                     'WMCore.Wrappers+', 'WMCore.Database'],
                        'modules' : ['WMCore.Configuration',
                                     'WMCore.Algorithms.ParseXMLFile', 'WMCore.Algorithms.Permissions',
                                     'WMCore.Lexicon', 'WMCore.WMException', 'WMCore.Services.Requests',
                                     'WMCore.Services.pycurl_manager'],
                       'statics' : ['src/couchapps/ACDC+',
                                    'src/couchapps/GroupUser+']
                       }
               }
