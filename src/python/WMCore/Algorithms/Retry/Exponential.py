"""
An exponential retry.
"""
from WMCore.Algorithms.Retry.Basic import Basic
from math import pow
from time import sleep

class Exponential(Basic):
    def __init__(self, timeout = 1, max = 10, unit = 1):
        Basic.__init__(self, timeout, max, unit)
        self.name = 'Exponential'
        
    def post(self):
        """
        Wait for timeout ^ count
        """
        sleep(pow((self.unit * self.timeout), self.count))