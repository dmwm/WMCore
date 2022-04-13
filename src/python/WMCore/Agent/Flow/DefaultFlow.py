#!/usr/bin/env python
#pylint: disable=E1101,E1103,C0103,R0902
"""
DefaultConfig.py

Sample configuration for generating workflow.

"""





import os
import pickle

from WMCore.Agent.Configuration import Configuration
config = Configuration()
config.section_('General')

# output directory of generated files.
config.General.baseDir = '/tmp/PRODAGENT'
config.General.srcDir = os.path.join(config.General.baseDir, 'src/python/PA/Component')
config.General.testDir =  os.path.join(config.General.baseDir, 'test/python/PA_t/Component_t')
# module prefix for python files.
config.General.pythonPrefix = 'PA.Component'
config.General.pythonTestPrefix = 'PA_t.Component_t'
config.General.handlers = []
config.General.synchronizers = []
config.General.plugins = []

# synchronizer is for trigger module.
synchronizer = {'ID' : 'JobPostProcess', \
                'action' : 'PA.Core.Trigger.PrepareCleanup'}
config.General.synchronizers.append(pickle.dumps(synchronizer))

# A handler is a piece of code that takes as input a message (and its payload)
# and performs certain actions. For example: a submit job handler takes as
# input messages of type SubmitJob with payload a job specification, and
# submits the #job to a particular site. Handlers are grouped into components.
# For example a submit failure handler and process failure handler can be
# grouped into an autonomous

# a handler spec has (at most) 5 attributes:
# -messageIn. (what message the component subscribes to should this handler
# handle.
# -messageOut (optional). What messages should this message publish.
# Messages separated
# by a comma mean that all these messages need to be published. Messages
# separated by a | means either or.
# -component. To which cmponent is this handler associated.
# -threading (optional). Are the messages for this handler dispatched to threads
# or handled sequential.
# createSynchronizer (optional). When this handler is finished handling a
# message should it create a trigger (so other components can set a flag).
# -synchronize. After handling the message this handler needs to set a flag
# in a particular synchronizer (aka trigger).
# configurable. Should the handlers that are associated to a message be
# configurable in the associated config file of the component.

handler = {'messageIn'   : 'CreateJob', \
           'messageOut'  : 'SubmitJob|JobCreateFailed', \
           'component'   : 'JobCreator'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'ReCreateJob', \
           'messageOut'  : 'SubmitJob|JobCreateFailed', \
           'component'   : 'JobCreator'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'SubmitJob', \
           'messageOut'  : 'TrackJob|JobSubmitFailed', \
           'component'   : 'JobSubmitter', \
           'threading'   : 'yes', \
           'createSynchronizer' : 'JobPostProcess'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'ReSubmitJob', \
           'messageOut'  : 'TrackJob|JobSubmitFailed', \
           'component'   : 'JobSubmitter', \
           'createSynchronizer' : 'JobPostProcess'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'TrackJob', \
           'messageOut'  : 'JobProcessSuccess|JobProcessFailed', \
           'component'   : 'JobTracker', \
           'threading'   : 'yes'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobProcessFailed', \
           'configurable': 'yes', \
           'messageOut'  : 'ResubmitJob|JobFailed', \
           'component'   : 'ErrorHandler'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobSubmitFailed', \
           'configurable': 'yes', \
           'messageOut'  : 'ResubmitJob|JobFailed', \
           'component'   : 'ErrorHandler'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobCreateFailed', \
           'configurable': 'yes', \
           'messageOut'  : 'ResubmitJob|JobFailed', \
           'component'   : 'ErrorHandler'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'CleanJob', \
           'messageOut'  : '', \
           'component'   : 'JobCleanup'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobFailed', \
           'messageOut'  : '', \
           'component'   : 'JobCleanup'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobSuccess', \
           'messageOut'  : '', \
           'component'   : 'JobCleanup'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobProcessSuccess', \
           'messageOut'  : '', \
           'component'   : 'DBS', \
           'synchronize' : 'JobPostProcess'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobProcessSucess', \
           'messageOut'  : 'AccountData', \
           'component'   : 'MergeSensor', \
           'synchronize' : 'JobPostProcess', \
           'threading'   : 'yes'}
config.General.handlers.append(pickle.dumps(handler))

handler = {'messageIn'   : 'JobProcessSuccess', \
           'messageOut'  : '', \
           'component'   : 'Phedex', \
           'synchronize' : 'JobPostProcess'}
config.General.handlers.append(pickle.dumps(handler))


plugin = {'component'  : 'JobSubmitter', \
          'plugins'    : 'PyCondor', \
          'handler'    : 'SubmitJob'}
config.General.plugins.append(pickle.dumps(plugin))
