#!/usr/bin/env python
"""
_Transitions_

Controls what state transitions are allowed.
"""

from future.utils import viewvalues


class Transitions(dict):
    """
    All allowed state transitions in the JSM.
    """
    def __init__(self):
        self.setdefault('none', ['new'])
        self.setdefault('new', ['created', 'createfailed', 'killed'])
        self.setdefault('created', ['executing', 'submitfailed', 'createfailed', 'killed'])
        self.setdefault('executing', ['complete', 'jobfailed', 'killed'])
        self.setdefault('complete', ['jobfailed', 'success'])
        self.setdefault('createfailed', ['createcooloff', 'retrydone', 'killed'])
        self.setdefault('submitfailed', ['submitcooloff', 'retrydone', 'killed'])
        self.setdefault('jobfailed', ['jobcooloff', 'retrydone', 'killed'])
        self.setdefault('createcooloff', ['created', 'createpaused', 'retrydone', 'killed'])
        self.setdefault('submitcooloff', ['created', 'submitpaused', 'retrydone', 'killed'])
        self.setdefault('jobcooloff', ['created', 'jobpaused', 'retrydone', 'killed'])
        self.setdefault('success', ['cleanout'])
        self.setdefault('retrydone', ['exhausted', 'killed'])
        self.setdefault('exhausted', ['cleanout'])
        self.setdefault('killed', ['cleanout', 'killed'])
        self.setdefault('jobpaused', ['created', 'retrydone', 'killed'])
        self.setdefault('createpaused', ['created', 'retrydone', 'killed'])
        self.setdefault('submitpaused', ['created', 'retrydone', 'killed'])

    def states(self):
        """
        _states_

        Return a list of all known states, derive it in case we add new final
        states other than cleanout.
        """
        knownstates = set(self.keys())
        for possiblestates in viewvalues(self):
            for i in possiblestates:
                knownstates.add(i)
        return list(knownstates)
