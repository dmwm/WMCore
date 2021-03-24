#!/usr/bin/env python
"""
_GeneratorInterface_

API definition for Generator objects

"""

from builtins import object


class GeneratorInterface(object):
    """
    _GeneratorInterface_

    Define the APIs to be implemented for Generator objects invoked
    by the JobFactory

    """
    def __init__(self, **options):
        self.task = options.get("task", None)
        self.options = options


    def start(self):
        """
        _start_

        Invoked by the factory before calling on any jobs.
        Override to initialise any state required

        """
        #print "%s.start()" % self.__class__.__name__
        pass

    def finish(self):
        """
        _finish_

        Invoked by the factory after it has completed generating
        a set of jobs.
        Override to save state as required

        """
        #print "%s.finish()" % self.__class__.__name__
        pass

    def startGroup(self, jobGroup):
        """
        _startGroup_

        Hook to respond to the start of a new job group

        """
        #print "%s.startGroup(%s)" % (self.__class__.__name__, jobGroup)
        pass

    def finishGroup(self, jobGroup):
        """
        _finishGroup_

        Hook to respond to end of a job group

        """
        #print "%s.finishGroup(%s)" % (self.__class__.__name__, jobGroup)
        pass

    def __call__(self, wmbsJob):
        """
        _operator(wmbsJob)_

        Act on a job instance and install the appropriate information
        into it

        """
        msg = "Object %s does not implement Generator.__call__(wmbsJob)" % (
            self.__class__.__name__,)
        raise NotImplementedError(msg)
