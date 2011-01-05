#!/usr/bin/env python
# Manage dependancies by declaring systems here.
# A system can depend on packages or other systems.
dependencies = {
               'wmc-database':{
                        'packages': ['WMCore.Database']
                        }, # DAO etc
               'wmc-runtime':{
                        'packages': ['WMCore.WMRuntime', 'WMCore.WMSpec'],
                        'systems':[]
                        },
               'wmc-web':{
                        'packages': ['WMCore.WebTools'],
                        'systems':['wmc-database'],
                        'statics': ['src/javascript/WMCore/WebTools',
                                'src/javascript/external/yui',
                                'src/css/WMCore/WebTools',
                                'src/css/WMCore/WebTools/Masthead',
                                'src/css/external/yui',
                                'src/templates/WMCore/WebTools',
                                'src/templates/WMCore/WebTools/Masthead',]
                     }, # CherryPy, REST
               'reqmgr':{
                        'packages': ['WMCore.RequestManager'],
                        'systems':['wmc-web'],
                        'statics': ['src/javascript/WMCore/WebTools/RequestManager',],
                        },
               'workqueue':{
                        'packages': ['WMComponent.WorkQueue'],
                        'systems': ['wmc-web'],
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
