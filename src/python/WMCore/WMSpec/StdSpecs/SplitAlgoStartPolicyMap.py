'''
Map between split algorithm and start policy
'''
#TODO not sure what to do with LumiBased (can't get information from DBS by block or dataset)
# Anzar will add api for Lumibased
SplitAlgoToStartPolicy = {"FileBased" : "NumberOfFiles",
                          "EventBased" : "NumberOfEvents"}

SplitAlgoToArgMap = {"FileBased" : "files_per_job",
                     "EventBased" : "events_per_job"}

#if the spliting Algo is not known (other than above)
#TODO: this shouldn't be needed when Lumibased is supported,
# remove default value.
DefaultSliceType = "NumberOfFiles"
DefaultSliceSize = 1

def getSliceType(splitAlgo):
    """
    convert split algorithm to slice type
    """
    if SplitAlgoToStartPolicy.has_key(splitAlgo):
        return SplitAlgoToStartPolicy[splitAlgo]
    else:
        #TODO: there shouldn't be else condition
        # All top level split algo should be known.
        # currently Lumibase info is not supported in DBS
        # when it gets supported remove else block. 
        return DefaultSliceType
        
def getSliceSize(splitAlgo, splitArgs):
    """
    get slice size according to the split algorithm
    """
    
    if SplitAlgoToStartPolicy.has_key(splitAlgo):
        return splitArgs[SplitAlgoToArgMap[splitAlgo]]
    else:
        #TODO: there shouldn't be else condition
        # All top level split algo should be known.
        # currently Lumibase info is not supported in DBS
        # when it gets supported remove else block. 
        return DefaultSliceSize
    
