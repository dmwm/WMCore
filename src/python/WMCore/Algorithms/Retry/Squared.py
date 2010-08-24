"""
A squaring retry algorithm.
"""
from WMCore.Algorithms.Retry.Basic import Basic
from math import pow
from time import sleep

class Squared(Basic):
    def __init__(self, timeout = 1, max = 10, unit = 1):
        Basic.__init__(self, timeout, max, unit)
        self.name = 'Squared'
        
    def post(self):
        """
        Wait for timeout * count^2
        """
        sleep(self.unit * self.timeout * pow(self.count, 2))