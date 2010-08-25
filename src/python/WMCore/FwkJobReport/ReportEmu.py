'''
Created on Dec 28, 2009

@author: evansde
'''
#import uuid
from WMCore.Services.UUID       import makeUUID 
from WMCore.FwkJobReport.Report import Report
from WMCore.DataStructs.File    import File
from WMCore.DataStructs.File    import Run


def addRunToFile(fileSection, run, *lumis):
    """
    _addRunToFile_
    
    Add run/lumi information to the file section provided
    
    """
    runS = str(run)
    section = fileSection.runs
    runSect = getattr(section, runS, None)
    if runSect == None:
        section.section_(runS)
        runSect = getattr(section, runS)
    if not hasattr(runSect, "lumiSections"):
        runSect.lumiSections = []
    runSect.lumiSections.extend(lumis)
    return
    
def addContributingInput(outFile, lfn, pfn):
    """
    _addContributingInput_
    
    Util to add an input file to an output file. outFile is the ConfigSection describing the file
    
    """
    section = outFile.inputs
    counter = section.fileCount
    counterValue = counter + 1
    stub = "file%s" % counterValue
    
    section.section_(stub)
    data = getattr(section, stub)
    
    data.LFN = lfn
    data.PFN = pfn
        
    section.fileCount += 1
    return
    
    

class ReportEmu(object):
    '''
    _ReportEmu_
    
    Job Report Emulator that creates a Report given a WMTask/WMStep and a Job instance.
    '''


    def __init__(self, **options):
        '''
        Constructor
        
        Options contain the settings for producing the report instance from the provided step
        '''
        self.step = options.get("WMStep", None)
        self.job = options.get("Job", None)
        
        
        
        
    def __call__(self):
        report = Report(self.step.name())
        
        report.id = self.job['id']
        report.task = self.job['task']
        report.workload = None
        
        
        
        report.addInputSource("PoolSauce")
        inpFiles = []
        runs = []
        
        for ifile in self.job['input_files']:
            ifilerep = report.addInputFile("PoolSource", LFN = ifile['lfn'], PFN =  "file:%s" % ifile['lfn'], TotalEvents = ifile['events'])
            inpFiles.append( (ifilerep.LFN, ifilerep.PFN) )
            for run in ifile['runs']:
                runs.append(run)
                addRunToFile(ifilerep, run.run, *run.lumis)
            
        
        for omod in self.step.listOutputModules():
            omodRef = self.step.getOutputModule(omod)
            report.addOutputModule(omod)
            #guid = str(uuid.uuid4())
            guid = str(makeUUID())
            basename = "%s.root" % guid
            outFile = File(lfn = "%s/%s" % (omodRef.lfnBase, basename ),
                           size = 100, events = 10, merged = False)
            outFile.setLocation(se = 'bad.cern.ch')
            outFile.addRun(Run(1, *[45]))
            outFile['dataset'] = {'name': '/Primary/Processed/Tier', 'ApplicationVersion' : '101', "ApplicationName" : 'JustSomeName'}
            repOutFile = report.addOutputFile(omod, outFile)
            #for ifile in inpFiles:
            #    addContributingInput(repOutFile, ifile[0], ifile[1])
            for run in runs:
                addRunToFile(repOutFile, run.run, *run.lumis)
    

        return report
        
