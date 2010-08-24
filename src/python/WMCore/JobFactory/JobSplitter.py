#!/usr/bin/env python


from WMCore.JobFactory.JobDefinition import JobDefinition


def splitByFiles(self, files, filesPerJob=1, force=False):
    """
    _splitByFiles_

    Require all files be located at same set of sites

    """
    result = []

    currentJob = JobDefinition()
    currentJob['SENames'] = files[0].locations
    
    counter = 0
    
    for file in files:
        currentJob['Files'].append(file)
        counter += 1
        if counter == filesPerJob:
            currentJob['Files'].sort()
            result.append(currentJob)
            currentJob = JobDefinition()
            currentJob['SENames'] = file.locations
            counter = 0

    if force and counter > 0:
        #  //
        # // remainder - only take if processing closed
        #//
        currentJob['Files'].sort()
        result.append(currentJob)
        
    return result
        

def splitByEvents(self, files, eventsPerJob, force=False):
    """
    _splitByEvents_

    Require all files be located at same set of sites
    
    This enforces at least 1 job per file i think - check this
        TODO: is this what we want - what if eventsPerJob > files.events
    
    """
    result = []

    carryOver = 0
    currentJob = JobDefinition()
    currentJob['SENames'] = files[0].locations
    currentJob['MaxEvents'] = eventsPerJob
    currentJob['SkipEvents'] = 0
    
    for file in files:
        #  //
        # // Take into account offset.
        #//
        startEvent = eventsPerJob - carryOver

        #  //Edge Effect: 
        # // if start event is 0, we need to add this file
        #//  otherwise it will be picked up automatically
        if startEvent != 0:
            currentJob['Files'].append(files[0])            

        #  //
        # // Keep creating job defs while accumulator is within
        #//  file event range
        accumulator = startEvent
        while accumulator < file.events: #eventsInFile:
            currentJob['Files'].sort()
            result.append(currentJob)
            currentJob = JobDefinition()
            currentJob['SENames'] = file.locations
            currentJob['MaxEvents'] = eventsPerJob
            currentJob['Files'].append(file)
            currentJob['SkipEvents'] = accumulator
            
            accumulator += eventsPerJob

        #  //
        # // if there was a shortfall in the last job
        #//  pass it on to the next job
        accumulator -= eventsPerJob
        carryOver = file.events - accumulator
        
    #  //
    # // remainder
    #//
    if force:
        currentJob['Files'].sort()
        result.append(currentJob)
    return result

                    
