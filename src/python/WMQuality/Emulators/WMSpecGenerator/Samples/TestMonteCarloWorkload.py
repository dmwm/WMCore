from WMCore.WMSpec.StdSpecs.MonteCarlo import MonteCarloWorkloadFactory

def getMCArgs():
    mcArgs = MonteCarloWorkloadFactory.getTestArguments()
    mcArgs.update({
                   "CouchURL": None,
                   "CouchDBName": None,
                   "ConfigCacheDoc" : None
                   })
    mcArgs.pop('ConfigCacheDoc')

    return mcArgs


class TestMonteCarloFactory(MonteCarloWorkloadFactory):
    """Override bits that talk to cmsssw"""
    def __call__(self, workflowName, args):
        workload = MonteCarloWorkloadFactory.__call__(self, workflowName, args)
        delattr(workload.taskIterator().next().steps().data.application.configuration,
                'configCacheUrl')
        return workload

    def determineOutputModules(self, *args, **kwargs):
        "Don't talk to couch"
        return {}

def monteCarloWorkload(workloadName, arguments):
    """
    _monteCarloWorkload_

    Instantiate the MonteCarloWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myMonteCarloFactory = TestMonteCarloFactory()
    return myMonteCarloFactory(workloadName, arguments)
