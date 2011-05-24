#!/usr/bin/env python
"""
Manage dependancies by declaring systems here.
A system can depend on packages or other systems.
If a package ends with a + include all subpackages.
"""
dependencies = {
                'wmc-base':{
                        'packages' : ['WMCore.DataStructs'],
                        'modules': ['WMCore.WMFactory', 'WMCore.WMException', 'WMCore.Configuration',
                                    'WMCore.WMExceptions', 'WMCore.WMFactory', 'WMCore.Lexicon',
                                    'WMCore.WMBase', 'WMCore.WMLogging', 'WMCore.Algorithms.Permissions'],
                        },
                'wmc-component':{
                        'packages': ['WMCore.MsgService', 'WMCore.WorkerThreads', 'WMCore.ThreadPool'],
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
                        'packages': ['WMCore.WMRuntime', 'WMCore.WMSpec+', 'PSetTweaks', 'WMCore.FwkJobReport'],
                        'systems':['wmc-base']
                        },
                'wmc-web':{
                        'packages': ['WMCore.WebTools', 'WMCore.Agent+', 'WMCore.WorkerThreads'],
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
                                     'WMCore.HTTPFrontEnd', 'WMCore.HTTPFrontEnd.RequestManager+', # first one gets init.py
                                     'WMCore.Services.WorkQueue', 'WMCore.Services.WMBS', 'WMCore.Services.WMAgent'],
                        'systems':['wmc-web', 'wmc-runtime'],
                        'statics': ['src/templates/WMCore/WebTools/RequestManager',
                                    'src/couchapps/ReqMgr+',
                                    'src/couchapps/ConfigCache+'],
                        },
                'workqueue':{
                        'packages': ['WMCore.WorkQueue+', 'WMCore.Services+', 'WMCore.Wrappers+',
                                     'WMCore.WMSpec+', 'WMCore.ACDC', 'WMCore.GroupUser'],
                        'modules' : ['WMCore.Algorithms.__init__', 'WMCore.Algorithms.Permissions',
                                     'WMCore.Database.__init__', 'WMCore.Database.CMSCouch',
                                     'WMCore.Algorithms.ParseXMLFile'],
                        'systems': ['wmc-base'],
                        'statics': ['src/couchapps/WorkQueue+'],
                        },
                'wmagent':{
                        'packages': ['WMCore.Agent+', 'WMCore.Algorithms+',
                                    'WMCore.JobStateMachine', 'WMComponent+',
                                    'WMCore.HTTPFrontEnd', 'WMCore.ThreadPool',
                                    'WMCore.BossAir', 'WMCore.Storage', 'WMCore.Credential',
                                    'WMCore.JobSplitting', 'WMCore.ProcessPool'],
                        'systems':['wmc-web', 'wmc-database', 'workqueue', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/Agent',
                                'src/javascript/WMCore/WebTools/WMBS',
                                'src/javascript/external/graphael',
                                'src/templates/WMCore/WebTools/WMBS',],
                        },
                'crabserver':{
                        'packages': ['WMCore.Services+'],
                        'systems': ['wmagent', 'reqmgr']
                        },
                'wmclient':{
                        'systems': ['wmc-runtime', 'wmc-database']
                        },
               }
