class JobGenerator(Object):
    """
    _JobGenerator_
    given the information from blockName 
    generate job and job group 
    """
    
    def __init__(self, wmSpec, files):
        """
        """
    def __call__(self, wmSpec, files, lastfileFlag):
        """
        create the job and job group using algorithm from spec
        and insert into the wmbs
        
        get the top task subscription
        """
        # for non production job
        if files != None:
            wmbsFileset = wmSpec.getTopLevelFileSet()
            # add files to the fileset
            wmbsFileset.addFile(files)
            if lastFileFlag:
                # close the fileset
                pass
        wmbsSubscription = wmSpec.getTopLevelTaskSubscription()
        # add files to the fileset
        splitAlgos = wmSpec.getTopLevelJobSplittingParameter()
        wmbsJobFactory = self.splitterFactory(package = "WMCore.WMBS",
                                              subscription = wmbsSubscription)

        wmbsJobGroups = wmbsJobFactory(**splitAlgos)