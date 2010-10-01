import WMCore.RequestManager.DataStructs.Request
import WMCore.WMSpec.WMWorkload as WMWorkload
from WMCore.RequestManager.RequestMaker.WMWorkloadCache import WMWorkloadCache

if __name__ == '__main__':
    cache = WMWorkloadCache('/tmp')
    requestName = 'RunToTheHills'
    workload = WMWorkload.WMWorkload(requestName)
    workload.request.section_('schema')
    workload.request.schema.RequestType = 'ReReco'
    workload.request.schema.RequestName = requestName
    workload.owner.Group = 'Offline'
    workload.owner.Requestor = 'Eddie'
    cache.checkIn(workload)
    pfn = cache.getPfn(workload)
    print pfn
    newWorkloadHelper = WMWorkload.WMWorkloadHelper()
    newWorkloadHelper.load(pfn)
    newSection = newWorkloadHelper.data.request
    #assert(newWorkloadHelper.data.request.schema.RequestName == requestName)
    assert(newWorkloadHelper.data.owner.Requestor == 'Eddie')
    cache.remove(workload)

