#!/usr/bin/env python
"""
Manage dependancies by declaring systems here.
A system can depend on packages or other systems.

"""
dependencies = {
               'wmc-base':{
                        'modules': ['WMCore.WMFactory', 'WMCore.WMException', 'WMCore.Configuration',
                                    'WMCore.WMExceptions', 'WMCore.WMFactory', 'WMCore.Lexicon',
                                    'WMCore.WMBase', 'WMCore.WMLogging']
                        },
               'wmc-database':{
                        'packages': ['WMCore.Wrappers', 'WMCore.GroupUser', 'WMCore.DataStructs', 'WMCore.Database',
                                    'WMCore.Algorithms', 'WMCore.Services', 'WMCore.Cache'],
                        'modules': ['WMCore.Action', 'WMCore.WMConnectionBase', 'WMCore.DAOFactory', 'WMCore.WMInit'],
                        'systems':['wmc-base']
                        },
               'wmc-runtime':{
                        'packages': ['WMCore.WMRuntime', 'WMCore.WMSpec'],
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
                        'packages': ['WMCore.RequestManager', 'WMCore.HTTPFrontEnd'],
                        'systems':['wmc-web', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/RequestManager',
                                    'src/templates/WMCore/WebTools/RequestManager',
                                    'src/html/RequestManager'],
                        },
               'workqueue':{
                        'packages': ['WMComponent.WorkQueueManager', 'WMCore.WorkQueue'],
                        'systems': ['wmc-web', 'wmc-database', 'wmc-base'],
                        'statics': ['src/templates/WMCore/WebTools/WorkQueue',]
                        },
               'wmagent':{
                        'packages': [],
                        'systems':['wmc-web', 'wmc-database', 'workqueue', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/Agent',
                                'src/javascript/WMCore/WebTools/WMBS',
                                'src/javascript/external/graphael',
                                'src/templates/WMCore/WebTools/WMBS',],
                        },
               }
