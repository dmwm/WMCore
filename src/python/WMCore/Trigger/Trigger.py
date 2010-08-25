#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
Class that implements trigger functionality for
Different components to synchronize work
"""





import base64
import cPickle
import threading

from WMCore.WMExceptions import WMEXCEPTION
from WMCore.WMException import WMException
from WMCore.WMFactory import WMFactory

#FIXME: do we want to have a multi queu version of this?

class Trigger:

    """
    Class that implements trigger functionality for
    Different components to synchronize work
    """

    def __init__(self):
        myThread = threading.currentThread()
        self.query = myThread.factory['trigger'].loadObject(\
            myThread.dialect+'.Queries')
        self.actionFactory = WMFactory("actions")

    def setFlag(self, args):
        """
        _setFlag_
     
        Sets a flag of a trigger associated to a jobspec. If this 
        is the last flag to be set (all other flags associated to this
        trigger have been set) the action will be invoked.

        input a dictionary (or array of dictionaries):
        -trigger_id (string). Id of the trigger
        -flag_id (string). Id of the flag
        -id (string). Id of the job specification
     
        output:
        
        nothing or an exception if either the flag, trigger or jobSpec id
        does not exists.
     
        """
        # lock trigger
        # prepare input:
        if type(args) != list:
            args = [args]
        argsIn = []
        for arg in args:
            argsIn.append({'trigger_id':arg['trigger_id'], 'id':arg['id']}) 
        self.query.lockTrigger(argsIn)
        # remove the flags
        self.query.removeFlag(args)
        #check if all flags are set:
        notSets = self.query.allFlagsSet(argsIn)
        notToBSet = []
        # single out triggers that should not trigger yet
        for notSet in notSets:
            notToBSet.append([notSet[1], notSet[2]])
        toBset = []
        # compare with the input which triggers should triggers.
        for arg in args:
            if not [arg['trigger_id'], arg['id']] in notToBSet:
                toBset.append({'trigger_id':arg['trigger_id'], 'id':arg['id']})
        # if flags are set invoke action (trigger it)
        if len(toBset) > 0:
            result = self.query.selectAction(args)
            # check which actions can be put in bulk
            # we are now going to check if certain
            # actions can handle bulk input.
            # and at the same time we de-pickle the payload.
            actions = {}
            for entry in result:
                if not actions.has_key(entry['action_name']):
                    actions[entry['action_name']] = []
                payload = cPickle.loads(base64.decodestring(entry['payload']))
                actions[entry['action_name']].append(\
                    {'payload' : payload, 'id': entry['id']})
            for actionName in actions.keys():
                action = self.actionFactory.loadObject(actionName)
                # e.g. you can imagine that if an action has threading
                # capability it can handle bulkd and spread it over the threads.
                if action.bulk:
                    action.__call__(actions[actionName])
                else:
                    for ied in actions[actionName]:
                        action.__call__(ied)

   
    def addFlag(self, args):
        """
        _addFlag_
        
        Adds a flag to a trigger. If this is the first flag for this
        trigger a new trigger will be created.
  
        input dictionary of (or array of dictionaries):

        -trigger_id (string). Id of the trigger
        -flag_id (string). Id of the flag
        -id (string). Id of the job specification
     
        output:
        
        nothing or an exception if the flag already existed.
     
        """
        try:
            self.query.insertFlag(args)
        except Exception, ex:
            msg = WMEXCEPTION['WMCORE-9'] 
            msg += str(args)
            msg += '  '+str(ex) 
            raise WMException(msg, 'WMCORE-9')
  
    def setAction(self, args):
        """
        _setAction_
  
        Sets the associated action that will be called
        if all flags are set. This action is registered in the action
        registery. If this trigger already had an action, this action
        will replace it.
  
        input dictionary (or array of dictionaries):
        -id (string)
        -trigger_id (string). Id of the trigger
        -action_name (string). Name of the action
        -payload. object that will be picled and passed on to the action.
  
        output:
        nothing or an exception if the trigger does not exists.
  
        output:
     
        nothing or an exception if the flag already existed.
  
        """

        try:
            # pickle and base64 the payload.
            if type(args) != list:
                args = [args]
            for arg in args:
                arg['payload'] = base64.encodestring(\
                    cPickle.dumps(arg['payload'])) 
            self.query.setAction(args)
        except Exception,ex:
            msg = WMEXCEPTION['WMCORE-10'] 
            msg += '  '+str(ex) 
            msg += str(args)
            raise WMException(msg, 'WMCORE-10')

        
 
