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
                        'packages': ['WMCore.WebTools', 'WMCore.Agent', 'WMCore.WorkerThreads'],
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
                        'packages': ['WMCore.Agent+', 'WMCore.RequestManager+', 'WMCore.HTTPFrontEnd.RequestManager+',
                                     'WMCore.Services.WorkQueue', 'WMCore.Services.WMBS', 'WMCore.Services.WMAgent'],
                        'systems':['wmc-web', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/RequestManager',
                                    'src/templates/WMCore/WebTools/RequestManager',
                                    'src/html/RequestManager'],
                        },
                'workqueue':{
                        'packages': ['WMComponent.WorkQueueManager', 'WMCore.WorkerThreads',
                                    'WMCore.WorkQueue+','WMCore.Services+', 'WMCore.Wrappers+',
                                    'WMQuality.Emulators',' WMCore.WMSpec+'],
                        'modules' : ['WMQuality.__init__'],
                        'systems': ['wmc-web', 'wmc-database', 'wmc-base', 'wmc-component'],
                        'statics': ['src/templates/WMCore/WebTools/WorkQueue',]
                        },
                'wmagent':{
                        'packages': ['WMCore.Agent+', 'WMCore.WMBS', 'WMCore.Algorithms',
                                    'WMCore.JobStateMachine', 'WMComponent.DBSBuffer',
                                    'WMCore.HTTPFrontEnd', 'WMCore.ThreadPool',
                                    'WMCore.BossAir',
                                    'WMCore.ResourceControl'],
                        'systems':['wmc-web', 'wmc-database', 'workqueue', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/Agent',
                                'src/javascript/WMCore/WebTools/WMBS',
                                'src/javascript/external/graphael',
                                'src/templates/WMCore/WebTools/WMBS',],
                        },
               }
