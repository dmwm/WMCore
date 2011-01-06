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
                        'packages': ['WMCore.Database'],
                        'modules': ['WMCore.Action', 'WMCore.WMConnectionBase', 'WMCore.DAOFactory', 'WMCore.WMInit'],
                        'systems':['wmc-base']
                        },
               'wmc-runtime':{
                        'packages': ['WMCore.WMRuntime', 'WMCore.WMSpec'],
                        'systems':['wmc-base']
                        },
               'wmc-web':{
                        'packages': ['WMCore.WebTools'],
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
                        'packages': ['WMCore.RequestManager'],
                        'systems':['wmc-web'],
                        'statics': ['src/javascript/WMCore/WebTools/RequestManager',
                                    'src/templates/WMCore/WebTools/RequestManager'],
                        },
               'workqueue':{
                        'packages': ['WMComponent.WorkQueueManager', 'WMCore.WorkQueue'],
                        'systems': ['wmc-web', 'wmc-database', 'wmc-base'],
                        'statics': ['src/templates/WMCore/WebTools/WorkQueue',]
                        },
               'wmagent':{
                        'packages': ['WMCore.Agent'],
                        'systems':['wmc-web', 'wmc-database', 'workqueue', 'wmc-runtime'],
                        'statics': ['src/javascript/WMCore/WebTools/Agent',
                                'src/javascript/WMCore/WebTools/WMBS',
                                'src/javascript/external/graphael',
                                'src/templates/WMCore/WebTools/WMBS',],
                        },
               }
