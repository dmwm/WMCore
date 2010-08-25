#!/usr/bin/env python
"""
Data objects for TQ APIs use.
"""




import xml.dom.minidom


############ MODULE CONSTANTS ################
TASK_REQUIRED = ['id', 'spec', 'wkflow', 'type', 'sandbox', 'reqs', 'req_se']
TASK_DEFAULTS = {'wkflow': None, 'type': None, 'reqs': None, \
                 'req_se': None, 'sandbox': 'sandboxFromSpec'}

# The fields in the task related tables (those that can be queried upon)
TASK_FIELDS = ['id', 'spec', 'wkflow', 'type', 'sandbox', 'state', 'pilot', \
               'reqs', 'req_se', 'creat_time', 'current_state_time']

# The fields in the pilot table (those that can be queried upon)
PILOT_FIELDS = ['id', 'host', 'se', 'site', 'cachedir', 'ttl', \
                'ttl_time', 'last_hearbeat']


############### CLASSES ####################

class Task(dict):
    """
    Class representing a task. It is just an extension of dict with
    a couple of methods to verify compliance.
    
    A task must contain the fields defined by the TASK_REQUIRED constant.

    If an object of Task is instantiated, the fields defined by the
    TASK_DEFAULTS constant can be omitted as they will get default values.
     """

    def __init__(self, arg = None, **kwd):
        """
        Constructor.
        
        Lets parent (dict) create the data structures. Then checks that 
        necessary keys are present (completes with default values for 
        those that are optional). 
        """
        if arg: 
            dict.__init__(self, arg)
        else:
            dict.__init__(self, kwd)

        for key in TASK_REQUIRED:
            if not key in self:
                if key in TASK_DEFAULTS:
                    if TASK_DEFAULTS[key] == 'sandboxFromSpec': 
                        try:
                            self[key] = sandboxFromSpec(self['spec'])
                        except Exception, inst:
                            messg = "Unable to extract sandbox from spec file"
                            messg += ": %s" % inst
                            raise ValueError(messg)
                    else:
                        self[key] = TASK_DEFAULTS[key]
                else:
                    raise ValueError('Not compliant task. Missing key: %s' % key)

    def validate(self):
       """
       Validate this task with function validateTask in the same module.
       """
       return validateTask(self)


    def sandboxFromSpec(self, xmlfile):
        """
        Utility to extract the value of the sandbox from the spec file.
        """
        
        def __getText__(self, nodelist):
            rc = ""
            for node in nodelist:
                if node.nodeType == node.TEXT_NODE:
                    rc = rc + node.data
            return rc.strip()

        dom = xml.dom.minidom.parse(xmlfile)
        params = dom.getElementsByTagName("JobSpec")[0].getElementsByTagName("Parameter")
        for param in params:
            if param.getAttribute("Name") == "BulkInputSandbox":
                result = self.__getText__(param.childNodes)
        return result





############## Non-class methods ############

def validateTask(task):
    """
    Checks that the passed object contains 
    the appropriate keys to comply with what is expected.
    If so, returns True. Otherwise, False.
    """
    for key in TASK_REQUIRED:
        if not key in task:
            messg = 'Not compliant task. Missing key: %s.' % key
            if 'spec' in task:
               messg += " Task spec: %s " % task['spec']
            raise ValueError(messg)

    return True
