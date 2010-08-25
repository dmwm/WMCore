'''
Created on Jul 15, 2009

@author: meloam
'''
from sets import Set
class Transitions(dict):
    """
    All allowed state transitions in the JSM.
    """
    def __init__(self):
        self.setdefault('none', ['new'])
        self.setdefault('new', ['created', 'createfailed'])
        self.setdefault('created', ['executing', 'submitfailed'])
        self.setdefault('executing', ['complete', 'jobfailed'])
        self.setdefault('complete', ['jobfailed', 'success'])
        self.setdefault('createfailed', ['createcooloff', 'exhausted'])
        self.setdefault('submitfailed', ['submitcooloff', 'exhausted'])
        self.setdefault('jobfailed', ['jobcooloff', 'exhausted'])
        self.setdefault('createcooloff', ['new'])
        self.setdefault('submitcooloff', ['created'])
        self.setdefault('jobcooloff', ['created'])
        self.setdefault('success', ['cleanout'])
        self.setdefault('exhausted', ['cleanout'])


    def states(self):
        """
        Return a list of all known states, derive it in case we add new final
        states other than cleanout.
        """
        knownstates = Set(self.keys())
        for possiblestates in self.values():
            for i in possiblestates:
                knownstates.add(i)
        return list(knownstates)


