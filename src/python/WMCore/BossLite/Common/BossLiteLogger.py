#!/usr/bin/env python
"""
BossLite logging facility
"""




from WMCore.BossLite.Common.Exceptions import BossLiteError

class BossLiteLogger(object):
    """
    logs informations from the task and eventual exception raised
    """

    # default values for fields
    defaults = [ 'errors', 'warnings', 'type', 'description', 'message', \
                 'command', 'jobWarnings', 'jobErrors', 'partialOutput' ]

    def __init__(self, task=None, exception=None):
        """
        __init__
        """

        self.data = {}
        self.data['type'] = 'log'
        errors = {}
        warnings = {}

        # handle task
        if task is not None :

            if task.warnings != [] :
                self.data['type'] = 'warning'
                self.data['warnings'] = task.warnings

            for job in task.jobs:
                # evaluate errors
                if job.runningJob.isError() :
                    errors[job['jobId']] = job.runningJob.errors
                    
                # evaluate warning
                if job.runningJob.warnings != [] :
                    warnings[job['jobId']] = job.runningJob.warnings

            if warnings != {}:
                self.data['type'] = 'warning'
                self.data['jobWarnings'] = warnings
            
            if errors != {} :
                self.data['type'] = 'error'
                self.data['jobErrors'] = errors

        # handle exception
        if exception is not None:

            if not isinstance( exception, BossLiteError ) :
                name = exception.__class__.__name__
                exception = BossLiteError( str(exception) )
                exception.value = name
                
            self.data['type'] = 'error'
            self.data['description'] = exception.value
            self.data['message'] = exception.message()
            for key, val in exception.data.iteritems():
                self.data[key] = val


    def __getitem__(self, field):
        """
        return one of the fields (in a dictionary form)
        """

        # get mapped field name
        return self.data[field]


    def __setitem__(self, field, value):
        """
        set one of the fields (in a dictionary form)
        """

        # set mapped field name
        if field in self.defaults:
            self.data[field] = value
            return

        # not there
        raise KeyError(field)


    def __str__(self):
        """
        return a printed representation of the situation
        """

        # get field names
        fields = self.data.keys()
        fields.sort()

        # the object can be empty unless for the type: return an empty string
        if len( fields ) == 1:
            return ''

        # show id first
        string = "Log Event type : %s \n" % self.data['type']
        fields.remove('type')

        # add the other fields
        for key in fields:
            string += "   %s : %s\n" % (str(key), str(self.data[key]))

        # return it
        return string
