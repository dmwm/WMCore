#!/usr/bin/env python

"""
_TrivialMatchmaker_

This module contains a class that is a very basic sample of matchmaker. For any 
pilot requesting a task it just returns the first available task in the queue
(with no dependencies or requirements considerations). It is here just as 
a reference example and not meant to be used in production.
"""

__revision__ = "$Id: TrivialMatchmaker.py,v 1.1 2009/06/01 09:55:08 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"


from Constants import taskStates


class TrivialMatchmaker(object):
    """ 
    _TrivialMatchmaker_. This matchmaker just returns the first available
    task in the queue. Don't use it!
    """
    def __init__(self, params):
        """
        Constructor. The 'params' argument can be used as a dict for any
        parameter (if needed).

        The required params are as follow:
           queries
        """
        required = ["queries"]
        for param in required:
            if not param in params:
                messg = "TrivialMatchmaker object requires params['%s']" % param
                raise ValueError(messg)

        self.queries = params["queries"]
        

    def matchTask(self, pilot):
        """
        Returns the ID of the best matching task, or None if there is no
        task available.
        """
#        return self.queries.getTaskAtState(taskStates['Queued'])
        filter = {'state': taskStates['Queued']}
        return self.queries.getTasksWithFilter(filter, asDict = True)
        
