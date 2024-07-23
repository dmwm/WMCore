#!/usr/bin/env python
"""
_Tier0Plugin_

Plugin which collects information from the agent and updates
the state of the current tier-0 workflows in the system.

Created on Nov 2, 2012

@author: dballest
"""

from builtins import filter

import re
import threading
import traceback

from WMComponent.AnalyticsDataCollector.Plugins.PluginInterface import PluginInterface
from WMCore.DAOFactory import DAOFactory
from WMCore.WMException import WMException
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def getTier0Regex():
    """
    _getTier0Regex_

    Define the Tier0 states and matching regex out of the object
    so it can be fetched without instancing the plugin.
    This are the uncompiled regular expressions in the correct
    order of the states
    """
    regexDict = {'Repack': [('Merge', [r'^/Repack_Run[0-9]+_Stream[\w]+/Repack$']),
                            ('Processing Done', [r'^/Repack_Run[0-9]+_Stream[\w]+/Repack/RepackMerge[\w]+$'])],

                 'PromptReco': [('AlcaSkim', [r'^/PromptReco_Run[0-9]+_[\w]+/Reco$']),
                                ('Merge', [r'^/PromptReco_Run[0-9]+_[\w]+/Reco/AlcaSkim$']),
                                ('Harvesting', [r'^/PromptReco_Run[0-9]+_[\w]+/Reco/AlcaSkim/AlcaSkimMerge[\w]+$',
                                                r'^/PromptReco_Run[0-9]+_[\w]+/Reco/RecoMerge[\w]+$']),
                                ('Processing Done',
                                 [r'^/PromptReco_Run[0-9]+_[\w]+/Reco/RecoMerge[\w]+/RecoMerge[\w]+DQMHarvest[\w]+$'])
                                ],

                 'Express': [('Merge', [r'^/Express_Run[0-9]+_Stream[\w]+/Express$']),
                             ('Harvesting', [r'^/Express_Run[0-9]+_Stream[\w]+/Express/ExpressMerge[\w]+$',
                                             r'^/Express_Run[0-9]+_Stream[\w]+/Express/ExpressAlcaSkim[\w]+$']),
                             ('Processing Done', [
                                 r'^/Express_Run[0-9]+_Stream[\w]+/Express/ExpressMerge[\w]+/ExpressMerge[\w]+DQMHarvest[\w]+$',
                                 r'^/Express_Run[0-9]+_Stream[\w]+/Express/ExpressAlcaSkim[\w]+/ExpressAlcaSkim[\w]+AlcaHarvest[\w]+$'])
                             ]}
    return regexDict


class Tier0PluginError(WMException):
    """
    _Tier0PluginError_

    An error in the Tier0 plugin
    """

    def __init__(self, msg):
        """
        __init__

        Initialize the error, just accepts a message without error code
        """
        WMException.__init__(self, msg)
        return


class Tier0Plugin(PluginInterface):
    """
    _Tier0Plugin_

    Tier0 plugin main class
    """

    def __init__(self):
        """
        __init__

        Initialize the plugin object
        """
        PluginInterface.__init__(self)

        self.myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package='WMCore.WMBS',
                                     logger=self.myThread.logger,
                                     dbinterface=self.myThread.dbi)

        self.logger = self.myThread.logger

        # To get finished subscriptions it needs the cleanout state index
        getCleanoutState = self.daoFactory(classname='Jobs.GetStateID')
        self.cleanoutState = getCleanoutState.execute(state='cleanout')

        # Load the DAOs
        self.getFinishedTasks = self.daoFactory(classname='Workflow.GetFinishedTasks')
        self.getFinishedTasksNoInjection = self.daoFactory(classname='Subscriptions.GetSemiFinishedTasks')

        # To avoid doing many I/O operations, cache the list of tasks for each workflow
        self.taskCache = {}

        self.setupStateRegex()

        return

    def setupStateRegex(self):
        """
        _setupStateRegex_

        Each state is mapped to a regular expression in the taskpaths
        this depends on the current implementation of the T0. The regular
        expressions obtained from the function Tier0Plugin.getTier0Regex
        are compiled and stored in a dictionary.

        The structure of the dictionary is as follows:

        { <workflowType> : [(<state>, [<matchObject>, <matchObject>]), ...],
          ...
        }
        """
        rawRegex = getTier0Regex()

        self.tier0Regex = {}

        for workflowType in rawRegex:
            self.tier0Regex[workflowType] = []
            for pair in rawRegex[workflowType]:
                compiledRegex = (pair[0], [re.compile(x).match for x in pair[1]])
                self.tier0Regex[workflowType].append(compiledRegex)
        return

    def __call__(self, requestDocs, localSummaryCouchDB, centralRequestCouchDB):
        """
        __call__

        Call to this plugin, this method takes care of executing
        the logic of the plugin, see guidelines in parent class documentation.
        """
        # Get all current finished tasks (i.e. all associated subscriptions are finished)
        finishedTasks = self.getFinishedTasks.execute()
        self.logger.debug("Found %d finished tasks" % len(finishedTasks))
        # Repack workflows are not marked injected and therefore its subscriptions
        # are not marked as finished until the injection happens (48h delay)
        # get those tasks with a looser criteria
        finishedTasksNoInjection = self.getFinishedTasksNoInjection.execute(state=self.cleanoutState,
                                                                            pattern='%Repack%')
        self.logger.debug("Found %d finished Repack tasks" % len(finishedTasksNoInjection))
        # Get workflows which are not closed yet or completed
        notClosedWorkflows = centralRequestCouchDB.getRequestByStatus(['new'])

        # Aggregate results by workflow name
        finishedTasks.extend(finishedTasksNoInjection)
        workflows = {}
        for entry in finishedTasks:
            workflowName = entry['name']
            if workflowName not in workflows:
                workflows[workflowName] = []
            if workflowName not in self.taskCache:
                self.loadTasks(workflowName, entry['spec'])
            workflows[workflowName].append(entry['task'])

        self.logger.info('Updating the status of %d workflows' % len(workflows))
        # Go through the reported workflows
        for workflowName in workflows:
            try:
                if workflowName in notClosedWorkflows:
                    # Nothing is completed for this workflow
                    continue

                # Sanity checks on workflow name
                tokens = workflowName.split('_')
                if not len(tokens):
                    self.logger.warning('This workflow does not match the Tier-0 naming structure, it will be ignored.')
                    continue
                workflowType = tokens[0]
                if workflowType not in self.tier0Regex:
                    self.logger.warning('This workflow does not match the Tier-0 naming structure, it will be ignored.')
                    continue

                completedTaskList = workflows[workflowName]

                workflowStatus = self.determineCurrentStatus(workflowName, workflowType, completedTaskList)
                if workflowStatus is not None:
                    centralRequestCouchDB.updateRequestStatus(workflowName, workflowStatus)
            except Tier0PluginError as t0ex:
                # More specific exception, just log it anyway
                self.logger.error('Error occurred while processing a doc:\n%s' % str(t0ex))
            except Exception as ex:
                # Plugins are meant to be not-critical
                # If something fails then just keep going
                self.logger.error('Error occurred while processing docs:\n%s' % str(ex))
                self.logger.error(traceback.format_exc())

        # Clean the task cache based on the documents we reported this cycle
        self.cleanTaskCache([x['workflow'] for x in requestDocs])

        return

    def loadTasks(self, workflowName, spec):
        """
        _loadTasks_

        Loads the list of tasks for the workflow,
        stores them in the cache if not present
        """
        if workflowName in self.taskCache:
            return
        try:
            workloadHelper = WMWorkloadHelper()
            workloadHelper.load(spec)
            tasks = workloadHelper.listAllTaskPathNames()
            self.taskCache[workflowName] = tasks
        except IOError as ex:
            msg = "Failed to load spec file %s\n" % spec
            msg += "Original IOError: %s" % str(ex)
            raise Tier0PluginError(msg)
        return

    def cleanTaskCache(self, reportedWorkflows):
        """
        _cleanTaskCache_

        Keep the memory footprint of this plugin smaller
        clean unused task lists
        """
        self.logger.debug('Cleaning up task cache')
        for workflow in list(self.taskCache):
            if workflow not in reportedWorkflows:
                self.taskCache.pop(workflow)
        return

    def determineCurrentStatus(self, workflowName, workflowType, completedTasks):
        """
        _determineCurrentStatus_

        Process a completed task list for a workflow and
        get the most advanced status of the workflow.
        """
        self.logger.debug('Calculating status for %s' % workflowName)
        currentStatus = None
        typeRegex = self.tier0Regex[workflowType]
        fullTaskList = self.taskCache[workflowName]
        for pair in typeRegex:
            completedTasksForStatus = 0
            totalTasksForStatus = 0
            for regex in pair[1]:
                completedTasksForStatus += len(list(filter(regex, completedTasks)))
                totalTasksForStatus += len(list(filter(regex, fullTaskList)))
            if completedTasksForStatus == totalTasksForStatus:
                currentStatus = pair[0]
            else:
                break
        self.logger.debug('Status is %s' % currentStatus)
        self.logger.debug('Completed task list: %s' % str(completedTasksForStatus))

        return currentStatus
