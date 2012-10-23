
from WMCore.DataStructs.WMObject import WMObject

import logging

class SplitterFactory(WMObject):
    """
    A splitter factory is called with a subscription. From that subscription it
    returns an instance of the splitting algorithm as a JobFactory. This is then
    called with a job type to return sets of jobs.
    """

    def __init__(self, package='WMCore.JobSplitting'):
        # package is the package the splitter will be loaded from
        self.package = package

    def __call__(self, subscription=None,
                 package='WMCore.DataStructs',
                 generators=[],
                 limit = 0):
        # package is the package output of the splitter will be loaded from
        """
        Instantiate an Subscription.split_algo and
        run that algorithm on subscription
        """
        algorithm = subscription['split_algo']

        module = "%s.%s" % (self.package, algorithm)
        module = __import__(module, globals(), locals(), [algorithm])#, -1)
        splitter = getattr(module, algorithm.split('.')[-1])
        return splitter(package=package,
                        subscription=subscription,
                        generators=generators,
                        limit = limit)
