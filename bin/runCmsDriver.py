#!/usr/bin/env python

import sys
import os
import pickle
import tempfile
#from ProdCommon.MCPayloads.WorkflowTools import createPSetHash
from WMCore.RequestManager.RequestMaker.ConfigUpload import uploadConfigText

# first argument is CMSSW installation.  Add it to path
sys.path.reverse()
sys.path.append(sys.argv[1]+'/python')
sys.path.reverse()

# argument is a pickled file of the options.
# Get overwritten with a proces object
f = open(sys.argv[2], mode='r+')
pick = f.read()
kwargs = pickle.loads(pick)

import Configuration.PyReleaseValidation.ConfigBuilder as ConfigBuilder
options = ConfigBuilder.defaultOptions
options.name = kwargs['name']
options.isData = (kwargs['dataMC'] == "data")
options.isMC = (kwargs['dataMC'] == "MC")
options.beamspot = None
options.scenario = kwargs['scenario']
options.evt_type = kwargs['gencfi']
options.conditions = kwargs['conditions'].split(',')[0]
options.globaltag = kwargs['conditions'].split(',')[1]
options.eventcontent = kwargs['eventcontent']
options.number = kwargs['number']
options.filein = kwargs['filein']
options.filetype = "EDM"
options.outfile_name = kwargs['outfile_name']

# Expert options, not implemented
options.customisation_file = ''
options.datatier = ''
options.dirin = ''
options.dirout= ''
options.filtername = ''
options.no_output_flag = False
options.oneoutput = False
options.prefix = ''
options.relval = ''
options.dump_dsetname_flag = False
options.python_filename = ''
options.secondfilein = ''
options.writeraw = False
options.no_exec_flag = False
options.arguments = ''
# make a list of all boxes turned on.
allLevels = ["GEN", "SIM", "DIGI", "L1", "DIGI2RAW", "RAW2DIGI", "RECO",
                "POSTRECO", "ALCA", "DQM"]
checked = [checkbox for checkbox in allLevels if kwargs.has_key(checkbox) and kwargs[checkbox] != None]
if checked == []:
   sys.exit("No configuration objects checked")
options.step = ','.join(checked).replace('"', '')
configBuilder = ConfigBuilder.ConfigBuilder(options, with_output = True, with_input = True)
configBuilder.prepare()

of = open(sys.argv[3], 'w')
of.write(configBuilder.pythonCfgCode)
of.close()

