"""
A linear retry.
"""

from WMCore.Algorithms.Retry.Basic import Basic
from time import sleep

class Linear(Basic):
    def __init__(self, timeout = 1, max = 10, unit = 1):
        Basic.__init__(self, timeout, max, unit)
        self.name = 'Linear'
        
    def post(self):
        """
        Wait for timeout * count
        """
        sleep(self.unit * self.timeout * self.count)