#!/usr/bin/env python
"""
_Transitions_

Controls what state transitions are allowed.
"""

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
        self.setdefault('createfailed', ['createcooloff', 'exhausted', 'killed'])
        self.setdefault('submitfailed', ['submitcooloff', 'exhausted', 'killed'])
        self.setdefault('jobfailed', ['jobcooloff', 'exhausted', 'killed'])
        self.setdefault('createcooloff', ['created', 'killed', 'createpaused'])
        self.setdefault('submitcooloff', ['created', 'killed', 'submitpaused'])
        self.setdefault('jobcooloff', ['created', 'jobpaused', 'killed'])
        self.setdefault('success', ['cleanout'])
        self.setdefault('exhausted', ['cleanout'])
        self.setdefault('killed', ['cleanout', 'killed'])
        self.setdefault('jobpaused', ['created', 'killed'])
        self.setdefault('createpaused', ['created', 'killed'])
        self.setdefault('submitpaused', ['created', 'killed'])

    def states(self):
        """
        _states_

        Return a list of all known states, derive it in case we add new final
        states other than cleanout.
        """
        knownstates = set(self.keys())
        for possiblestates in self.values():
            for i in possiblestates:
                knownstates.add(i)
        return list(knownstates)
