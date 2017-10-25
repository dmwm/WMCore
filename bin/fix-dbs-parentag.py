#!/usr/bin/env python
from __future__ import print_function, division
import json
from pprint import pprint
from collections import defaultdict
from dbs.apis.dbsClient import DbsApi

endpoint = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')

def listOfParentDatasets(dataset):
    parents = dbs.listDatasetParents(dataset=dataset)
    parentDatasets = []
    for parent in parents:
        parentDatasets.append(parent['parent_dataset'])
    return parentDatasets

def listBlocksWithNoParents(dataset):
    allBlocks = dbs.listBlocks(dataset=dataset)
    blockNames = []
    for block in allBlocks:
        blockNames.append(block['block_name'])
    parentBlocks = dbs.listBlockParents(block_name=blockNames)

    cblock = set()
    for pblock in parentBlocks:
        cblock.add(pblock['this_block_name'])

    noParentBlocks = set(blockNames) - cblock
    return noParentBlocks

def listFilesWithNoParents(blockName):
    allFiles = dbs.listFiles(block_name=blockName)
    parentFiles = dbs.listFileParents(block_name=blockName)

    allFileNames = set()
    for fInfo in allFiles:
        allFileNames.add(fInfo['logical_file_name'])

    cfile = set()
    for pFile in parentFiles:
        cfile.add(pFile['logical_file_name'])

    noParentFiles = allFileNames - cfile
    return noParentFiles

def getParentFilesFromDataset(parentDataset, lfn):
    fInfo = dbs.listFileLumis(logical_file_name=lfn)
    pLFNsByBlocks = defaultdict(set)
    for f in fInfo:
        pFiles = dbs.listFiles(dataset=parentDataset, run_num=f['run_num'], lumi_list=f['lumi_section_num'])
        for fi in pFiles:
            blocks = dbs.listBlocks(logical_file_name=fi['logical_file_name'])
            for bl in blocks:
                pLFNsByBlocks[bl['block_name']].add(fi['logical_file_name'])
    return pLFNsByBlocks

def getParentfilesMissingParents(dataset):

    print("=== searching problem dataset %s ===\n" % dataset)

    pDatasets = listOfParentDatasets(dataset)
    # print("parent datasets %s\n" % pDatasets)
    if not pDatasets:
        print("No parents dataset found for %s\n" % dataset)
        return

    # DatasetReport = {'child_dataset': dataset, 'parent_datasets': pDatasets, 'mssing_parent_blocks': []}
    DatasetReport = {'CDS': dataset, 'PDS': pDatasets, 'MISSING': []}

    blocks = listBlocksWithNoParents(dataset)
    countChildFiles = 0
    countParentFiles = 0
    countParentBlocks = 0

    for blockName in blocks:
        # BlockLevel = {'child_block': blockName, "parent_blocks": set(), 'lfn_parentage': []}
        BlockLevel = {'CBK': blockName, "PBK": [], 'PT': []}
        noParentFiles = listFilesWithNoParents(blockName)
        # print("%s block has %s files with no parents\n\n" % (blockName, len(noParentFiles)))
        countChildFiles += len(noParentFiles)
        parentBlocks = set()
        for lfn in noParentFiles:
            # print("child file : %s\n" % lfn)
            parentlfns = []  # combined parent with different datasts
            for parentDataset in pDatasets:
                # print("found parents %s dataset" % parentDataset)
                plfns = getParentFilesFromDataset(parentDataset, lfn)
                # pprint("block, files %s" % dict(plfns))
                for pblock, pfiles in plfns.items():
                    # BlockLevel['parent_blocks'].append(pblock)
                    parentBlocks.add(pblock)
                    parentlfns.extend(list(pfiles))
                countParentFiles += len(parentlfns)
                # BlockLevel['lfn_parentage'].append({'child_lfn': lfn, 'parent_lfns': parentlfns})
            BlockLevel['PT'].append({'CLFN': lfn, 'PLFN': parentlfns})
        BlockLevel['PBK'] = list(parentBlocks)
        countParentBlocks += len(BlockLevel['PBK'])
        # DatasetReport['missing_parent_blocks'].append(BlockLevel)
        DatasetReport['MISSING'].append(BlockLevel)
            # print()
    print("%s blocks with no parent block found, %s parent bocks, %s child files, %s parent_files\n" % (len(blocks), countParentBlocks, countChildFiles, countParentFiles))
    return DatasetReport

    print("====================================\n\n")

if __name__ == '__main__':

    datasetLists = [{'dataset': '/TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/PhaseIFall16DR-FlatPU28to62HcalNZSRAW_90X_upgrade2017_realistic_v6_C1-v2/AODSIM'},
                    {'dataset': '/SingleNeutrino/RunIISummer17DRStdmix-NZSFlatPU28to62_92X_upgrade2017_realistic_v10_ext1-v1/AODSIM'},
                    {'dataset': '/SingleNeutrino/RunIISummer17DRStdmix-NZSFlatPU28to62_92X_upgrade2017_realistic_v10_ext1-v1/GEN-SIM-RAW'},
                    {'dataset': '/SingleMuon/Run2016H-03Feb2017_ver2-v1/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016G-03Feb2017-v1/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016F-03Feb2017-v1/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016E-03Feb2017-v1/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016D-03Feb2017-v1/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016C-03Feb2017-v1/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016B-03Feb2017_ver2-v2/MINIAOD'},
                    {'dataset': '/SingleMuon/Run2016B-03Feb2017_ver1-v1/MINIAOD'},
                    ]

    # datasetLists = dbs.listDatasets(min_cdate=1492717811, max_cdate=1498717900)
    # print(len(datasetLists))
    missingParents = []
    for dataset in datasetLists:
        missingParents.append(getParentfilesMissingParents(dataset['dataset']))
    with open("./missing_parents.json", "w") as f:
        json.dump(missingParents, f, indent=4)
    with open("./missing_parents.py", "w") as f:
        f.write("MD = %s" % missingParents)


# ## report on this script
#
# cmst1@vocms0193:/data/srv/wmagent/current/apps/wmagent/bin $ $manage execute-agent fix-dbs-parent
# Executing fix-dbs-parent  ...
# === searching problem dataset /TT_TuneCUETP8M2T4_13TeV-powheg-pythia8/PhaseIFall16DR-FlatPU28to62HcalNZSRAW_90X_upgrade2017_realistic_v6_C1-v2/AODSIM ===
#
# 1 blocks with no parent block found, 2 parent bocks, 2 child files, 12 parent_files
#
# === searching problem dataset /SingleNeutrino/RunIISummer17DRStdmix-NZSFlatPU28to62_92X_upgrade2017_realistic_v10_ext1-v1/AODSIM ===
#
# 3 blocks with no parent block found, 4 parent bocks, 8 child files, 13 parent_files
#
# === searching problem dataset /SingleNeutrino/RunIISummer17DRStdmix-NZSFlatPU28to62_92X_upgrade2017_realistic_v10_ext1-v1/GEN-SIM-RAW ===
#
# 2 blocks with no parent block found, 4 parent bocks, 7 child files, 9 parent_files
#
# === searching problem dataset /SingleMuon/Run2016H-03Feb2017_ver2-v1/MINIAOD ===
#
# 0 blocks with no parent block found, 0 parent bocks, 0 child files, 0 parent_files
#
# === searching problem dataset /SingleMuon/Run2016G-03Feb2017-v1/MINIAOD ===
#
#
# 3 blocks with no parent block found, 98 parent bocks, 27 child files, 364 parent_files
#
# === searching problem dataset /SingleMuon/Run2016F-03Feb2017-v1/MINIAOD ===
#
# 1 blocks with no parent block found, 19 parent bocks, 7 child files, 144 parent_files
#
# === searching problem dataset /SingleMuon/Run2016E-03Feb2017-v1/MINIAOD ===
#
# 0 blocks with no parent block found, 0 parent bocks, 0 child files, 0 parent_files
#
# === searching problem dataset /SingleMuon/Run2016D-03Feb2017-v1/MINIAOD ===
#
# 1 blocks with no parent block found, 13 parent bocks, 2 child files, 42 parent_files
#
# === searching problem dataset /SingleMuon/Run2016C-03Feb2017-v1/MINIAOD ===
#
# 0 blocks with no parent block found, 0 parent bocks, 0 child files, 0 parent_files
#
# === searching problem dataset /SingleMuon/Run2016B-03Feb2017_ver2-v2/MINIAOD ===
#
# 1 blocks with no parent block found, 20 parent bocks, 6 child files, 108 parent_files
#
# === searching problem dataset /SingleMuon/Run2016B-03Feb2017_ver1-v1/MINIAOD ===
#
# 1 blocks with no parent block found, 6 parent bocks, 1 child files, 14 parent_files
