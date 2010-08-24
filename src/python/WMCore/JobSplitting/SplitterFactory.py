from WMCore.DataStructs.WMObject import WMObject
class SplitterFactory(WMObject):
    """
    A splitter factory is called with a subscription. From that subscription it
    returns an instance of the splitting algorithm as a JobFactory. This is then 
    called with a job type to return sets of jobs. 
    """

    def __init__(self, package='WMCore.JobSplitting'):
        self.package = package
        
    def __call__(self, subscription=None, package='WMCore.DataStructs'):
        """
        Instantiate an Subscription.split_algo and run that algorithm on subscription
        """
        algorithm = subscription.split_algo
        #TODO: Import the algorithm
        module = "%s.%s" % (self.package, algorithm)
        module = __import__(module, globals(), locals(), [algorithm])#, -1)
        splitter = getattr(module, algorithm.split('.')[-1])
        return splitter(package=package, subscription=subscription)