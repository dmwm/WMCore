import pickle
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMComponent.RetryManager.Modifier.BaseModifier import BaseModifier
import pickle


class MemoryModifier(BaseModifier):

    def __init__(self):
        BaseModifier.__init__(self)

    def getWorkload():
        """
        _getWorkload_

        
        """
        # Creates copy of original sandbox, unzips it, and retrieves the path to the uncompressed sandbox
        sandboxPath=self.getJobSandbox()

        pklPath=sandboxPath+'WMSandbox/WMWorkload.pkl'

        configHandle = open(pklPath, "rb")
        workload = pickle.load(configHandle)
        configHandle.close()

        return workload

    def setWorkload(self, workload):
        """
        _setWorkload_

        
        """
        pklPath=self.sandboxPath+'WMSandbox/WMWorkload.pkl'

        #Pkl the modified object
        with open(pklPath, 'wb') as pf:
            pickle.dump(worker, pf)
        
        self.updateSandbox()

    ### German ###
    def changeSandbox(self, job, newMemory):
        """
        _changeSandbox_

        Modifies the parameter maxPSS in the sandbox. This is a change that applies for all jobs in that workflow that remain to be submitted 
        """

        # figure how to get the path
        sandboxPath = ""

        workload = self.getWorkload(sandboxPath)
        workHelper = WMWorkloadHelper(workload)

        for task in workHelper.getAllTasks():
            task.setMaxPSS(newMemory)

        self.setWorkload(workload)

        return

    def changeJobPkl(new_memory, pkl_file):
        """
        Modifies the pkl_file job.pkl by changing the estimatedMemoryUsage to a new_memory value

        """
        with open(pkl_file, 'rb') as file:
            data = pickle.load(file)
        #print(data['estimatedMemoryUsage'])
        data['estimatedMemoryUsage'] = new_memory
        with open('job.pkl', 'wb') as file:
            pickle.dump(data, file)

    def checkNewJobPkl(pkl_file):
        with open(pkl_file, 'rb') as file:
            data = pickle.load(file)
        print (data['estimatedMemoryUsage'])

    def changeMemory(self, newMemory):
        """
        The "main" function in charge of modifying the memory before a retry. 
        It needs to modify the job.pkl file and the workflow sandbox
        It gets the cachedir from the database. There it has the sandbox and the job.pkl file accessible
        """

        loadAction = RetryManagerPoller.daoFactory(classname="Jobs.LoadFromID") # RetryManagerPoller has no attribute daoFactory. I am certainly accessing it wrong. How do I access it?
        print(loadAction) 

        # I think loadAction is a dictionary whose keys are columns from the database. What is the exact name of the cachedir column?
        cacheDir = loadAction[cachedir]
        
        #Finds job.pkl file
        pkl_file = '{}/job.pkl'.format(cacheDir) 
        self.changeJobPkl(newMemory, pkl_file)
        self.changeSandbox()