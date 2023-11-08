"""
This module contains example of CMSSW metrics and corresponding helper
function to convert real metrics values to their data-types
"""

CMSSW_METRICS = {
    "SystemMemory": {
        "Active(file)": 4004848,
        "KernelStack": 13760,
        "VmallocUsed": 89264,
        "Hugepagesize": 2048,
        "Buffers": 81144,
        "DirectMap4k": 145248,
        "Inactive(file)": 3464500,
        "PageTables": 54704,
        "Shmem": 101300,
        "SReclaimable": 4005812,
        "Slab": 4393020,
        "SUnreclaim": 387208,
        "HardwareCorrupted": 0,
        "VmallocTotal": -1,
        "Committed_AS": 10468632,
        "MemFree": 8409300,
        "HugePages_Total": 0,
        "AnonHugePages": 14336,
        "Inactive": 5438044,
        "Inactive(anon)": 1973544,
        "Cached": 7490532,
        "MemAvailable": 19492112,
        "SwapFree": 28181320,
        "HugePages_Free": 0,
        "Bounce": 0,
        "CmaFree": 0,
        "WritebackTmp": 0,
        "Active(anon)": 7233764,
        "Mapped": 555512,
        "CommitLimit": 45402068,
        "Percpu": 4224,
        "AnonPages": 9069956,
        "Active": 11238612,
        "MemTotal": 29986740,
        "Mlocked": 0,
        "VmallocChunk": -224904,
        "NFS_Unstable": 0,
        "SwapTotal": 30408700,
        "DirectMap2M": 5408768,
        "Unevictable": 0,
        "CmaTotal": 0,
        "HugePages_Surp": 0,
        "HugePages_Rsvd": 0,
        "DirectMap1G": 25165824,
        "Dirty": 233852,
        "SwapCached": 87876,
        "Writeback": 0
        },
    "SystemCPU": {
        "CPUModels": "Intel Core Processor (Broadwell, IBRS)",
        "averageCoreSpeed": 2194.898,
        "totalCPUs": 16,
        "cpusetCount": 16
        },
    "ProcessingSummary": {
        "NumberEvents": 3,
        "NumberBeginRunCalls": 1,
        "NumberBeginLumiCalls": 1
        },
    "XrdSiteStatistics": {
        "readv-numOperations": [
            {
                "site": "roma1.infn.it",
                "value": 2
                },
            {
                "site": "T2_IT_BARI",
                "value": 5
                },
            {
                "site": "recas.ba.infn.it",
                "value": 15
                },
            {
                "site": "cern.ch",
                "value": 1
                }
            ],
        "read-totalMegabytes": [
            {
                "site": "roma1.infn.it",
                "value": 0.455857
                },
            {
                "site": "T2_IT_BARI",
                "value": 1.36202
                },
            {
                "site": "recas.ba.infn.it",
                "value": 1.58517
                },
            {
                "site": "cern.ch",
                "value": 0
                }
            ],
        "readv-totalMegabytes": [
            {
                "site": "roma1.infn.it",
                "value": 17.8692
                },
            {
                "site": "T2_IT_BARI",
                "value": 6.18529
                },
            {
                "site": "recas.ba.infn.it",
                "value": 71.6814
                },
            {
                "site": "cern.ch",
                "value": 0.251508
                }
            ],
        "read-totalMsecs": [
            {
                "site": "roma1.infn.it",
                "value": 1820.49
                },
            {
                "site": "T2_IT_BARI",
                "value": 4115.96
                },
            {
                "site": "recas.ba.infn.it",
                "value": 2925.22
                },
            {
                "site": "cern.ch",
                "value": 0
                }
            ],
        "read-numOperations": [
            {
                "site": "roma1.infn.it",
                "value": 72
                },
            {
                "site": "T2_IT_BARI",
                "value": 99
                },
            {
                "site": "recas.ba.infn.it",
                "value": 101
                },
            {
                "site": "cern.ch",
                "value": 0
                }
            ],
        "readv-totalMsecs": [
            {
                "site": "roma1.infn.it",
                "value": 5100.97
                },
            {
                "site": "T2_IT_BARI",
                "value": 1064.16
                },
            {
                "site": "recas.ba.infn.it",
                "value": 4575.52
                },
            {
                "site": "cern.ch",
                "value": 48.8356
                }
            ],
        "readv-numChunks": [
            {
                "site": "roma1.infn.it",
                "value": 108
                },
            {
                "site": "T2_IT_BARI",
                "value": 29
                },
            {
                "site": "recas.ba.infn.it",
                "value": 359
                },
            {
                "site": "cern.ch",
                "value": 13
                }
            ]
        },
    "Timing": {
        "MinEventTime": 10.3773,
        "NumberOfThreads": 2,
        "EventThroughput": 0.0379954,
        "NumberOfStreams": 2,
        "TotalEventSetupTime": 16.1899,
        "TotalLoopTime": 78.9568,
        "MaxEventTime": 46.6407,
        "TotalInitTime": 73.9733,
        "TotalJobChildrenCPU": 0.174912,
        "TotalJobTime": 153.001,
        "TotalNonModuleTime": 32.1743,
        "TotalInitCPU": 47.1554,
        "AvgEventTime": 34.1318,
        "TotalJobCPU": 155.483,
        "TotalLoopCPU": 108.262
        },
    "ApplicationMemory": {
        "SecondLargestRssEvent-f-RSS": 3319.17,
        "SecondLargestVsizeEventT2-f-RSS": 3284.8,
        "HEAP_MAPPED_SIZE_BYTES": 0,
        "LargestVsizeEventT1-c-EVENT": 102,
        "SecondLargestVsizeEventT2-a-COUNT": 2,
        "SecondLargestVsizeEventT2-e-DELTV": 0,
        "LargestVsizeIncreaseEvent-e-DELTV": 0,
        "HEAP_UNUSED_BYTES": 0,
        "HEAP_USED_BYTES": 0,
        "HEAP_MAPPED_N_CHUNKS": 0,
        "PeakValueRss": 3331.98,
        "ThirdLargestRssEvent-a-COUNT": 2,
        "LargestVsizeEventT1-f-RSS": 3331.98,
        "ThirdLargestVsizeEventT3-c-EVENT": 101,
        "ThirdLargestVsizeEventT3-e-DELTV": 0,
        "SecondLargestRssEvent-e-DELTV": 0,
        "LargestVsizeEventT1-a-COUNT": 1,
        "ThirdLargestRssEvent-b-RUN": 1,
        "HEAP_ARENA_N_UNUSED_CHUNKS": 1,
        "SecondLargestRssEvent-a-COUNT": 3,
        "AverageGrowthRateRss": 669.988,
        "ThirdLargestVsizeEventT3-d-VSIZE": 5453.68,
        "AverageGrowthRateVsize": 895.158,
        "LargestVsizeEventT1-e-DELTV": 0,
        "LargestVsizeEventT1-d-VSIZE": 5453.68,
        "LargestRssEvent-d-VSIZE": 5453.68,
        "PeakValueVsize": 5453.68,
        "LargestRssEvent-a-COUNT": 1,
        "SecondLargestVsizeEventT2-d-VSIZE": 5453.68,
        "LargestVsizeIncreaseEvent-f-RSS": 3331.98,
        "ThirdLargestRssEvent-c-EVENT": 104,
        "LargestVsizeEventT1-b-RUN": 1,
        "LargestVsizeIncreaseEvent-a-COUNT": 1,
        "LargestRssEvent-b-RUN": 1,
        "ThirdLargestRssEvent-f-RSS": 3284.8,
        "ThirdLargestVsizeEventT3-a-COUNT": 3,
        "LargestVsizeIncreaseEvent-d-VSIZE": 5453.68,
        "SecondLargestRssEvent-b-RUN": 1,
        "ThirdLargestVsizeEventT3-f-RSS": 3319.17,
        "ThirdLargestRssEvent-d-VSIZE": 5453.68,
        "LargestRssEvent-c-EVENT": 102,
        "HEAP_TOP_FREE_BYTES": 0,
        "HEAP_ARENA_SIZE_BYTES": 0,
        "LargestRssEvent-f-RSS": 3331.98,
        "SecondLargestVsizeEventT2-c-EVENT": 104,
        "LargestRssEvent-e-DELTV": 0,
        "LargestVsizeIncreaseEvent-b-RUN": 1,
        "SecondLargestVsizeEventT2-b-RUN": 1,
        "ThirdLargestVsizeEventT3-b-RUN": 1,
        "SecondLargestRssEvent-d-VSIZE": 5453.68,
        "ThirdLargestRssEvent-e-DELTV": 0,
        "LargestVsizeIncreaseEvent-c-EVENT": 102,
        "SecondLargestRssEvent-c-EVENT": 101
        },
    "StorageStatistics": {
        "Timing-tstoragefile-stat-maxMsecs": 0.072352,
        "Parameter-untracked-bool-enabled": True,
        "Timing-tstoragefile-flush-minMsecs": 0.646707,
        "Timing-root-close-minMsecs": 0.10421,
        "Timing-tstoragefile-flush-totalMegabytes": 0,
        "Timing-file-readv-maxMsecs": 0,
        "Timing-file-close-numSuccessfulOperations": 1,
        "Timing-file-read-numOperations": 4680,
        "Timing-file-read-totalMsecs": 61.8403,
        "Timing-tstoragefile-writeViaCache-numOperations": 7307,
        "Timing-file-flush-numOperations": 3,
        "Timing-file-prefetch-numOperations": 0,
        "Timing-file-close-totalMsecs": 0.190476,
        "Timing-tstoragefile-readAsync-minMsecs": 0,
        "Timing-root-open-numOperations": 3,
        "Timing-root-position-totalMegabytes": 0,
        "Timing-tstoragefile-readViaCache-minMsecs": 0,
        "Timing-file-construct-totalMsecs": 0.00055,
        "Timing-root-stagein-totalMsecs": 4.10695,
        "Timing-tstoragefile-flush-numSuccessfulOperations": 3,
        "Timing-tstoragefile-write-minMsecs": 0.006056,
        "Timing-tstoragefile-readAsync-totalMsecs": 0,
        "Timing-file-write-totalMegabytes": 29.0666,
        "Timing-file-open-minMsecs": 1.05184,
        "Timing-root-open-numSuccessfulOperations": 3,
        "Timing-root-construct-numSuccessfulOperations": 3,
        "Timing-file-read-minMsecs": 0.001137,
        "Timing-file-close-maxMsecs": 0.190476,
        "Timing-tstoragefile-readActual-numSuccessfulOperations": 4968,
        "Timing-file-write-numSuccessfulOperations": 7307,
        "Timing-root-prefetch-minMsecs": 0,
        "Timing-root-stagein-numSuccessfulOperations": 1,
        "Timing-root-readv-numOperations": 231,
        "Timing-root-close-totalMsecs": 0.741968,
        "Timing-tstoragefile-construct-numSuccessfulOperations": 5,
        "Timing-tstoragefile-stat-totalMsecs": 2.83543,
        "Timing-file-flush-maxMsecs": 1809.22,
        "Timing-file-open-totalMegabytes": 0,
        "Timing-tstoragefile-readAsync-numOperations": 30,
        "Timing-root-position-numSuccessfulOperations": 93,
        "Timing-tstoragefile-write-numOperations": 7307,
        "Timing-root-read-numOperations": 57,
        "Timing-file-writev-totalMsecs": 0,
        "Timing-tstoragefile-readAsync-totalMegabytes": 0,
        "Timing-file-writev-maxMsecs": 0,
        "Timing-root-writev-totalMegabytes": 0,
        "Timing-tstoragefile-writeActual-totalMegabytes": 29.0666,
        "Timing-file-position-totalMsecs": 17.9375,
        "Timing-tstoragefile-readViaCache-maxMsecs": 0,
        "Timing-file-open-totalMsecs": 3.28384,
        "Timing-root-writev-numSuccessfulOperations": 0,
        "Timing-tstoragefile-writeViaCache-totalMsecs": 0,
        "Timing-file-prefetch-numSuccessfulOperations": 0,
        "Timing-tstoragefile-readViaCache-totalMegabytes": 0,
        "Timing-file-prefetch-totalMsecs": 0,
        "Timing-file-flush-numSuccessfulOperations": 3,
        "Timing-tstoragefile-stat-minMsecs": 0.00071,
        "Timing-file-position-totalMegabytes": 0,
        "Timing-root-prefetch-totalMegabytes": 0,
        "Timing-root-position-numOperations": 93,
        "Timing-tstoragefile-close-numOperations": 4,
        "Timing-file-position-minMsecs": 0.000751,
        "Timing-root-read-totalMsecs": 2011.12,
        "Timing-tstoragefile-construct-totalMegabytes": 0,
        "Parameter-untracked-bool-prefetching": False,
        "Timing-file-open-numOperations": 2,
        "Timing-tstoragefile-writeViaCache-numSuccessfulOperations": 0,
        "Timing-tstoragefile-writeActual-numSuccessfulOperations": 7307,
        "Timing-root-readv-minMsecs": 15.7082,
        "Timing-file-construct-totalMegabytes": 0,
        "Timing-root-position-minMsecs": "4.4e-05",
        "Timing-root-readv-maxMsecs": 4168.77,
        "Timing-tstoragefile-seek-totalMegabytes": 0,
        "Timing-root-close-numOperations": 3,
        "Timing-root-close-numSuccessfulOperations": 3,
        "Timing-root-readv-numSuccessfulOperations": 231,
        "Timing-file-flush-minMsecs": 0.645059,
        "Timing-tstoragefile-seek-minMsecs": 0.000217,
        "Timing-root-read-maxMsecs": 258.502,
        "Timing-file-write-maxMsecs": 12.2384,
        "Timing-root-write-numSuccessfulOperations": 0,
        "Timing-tstoragefile-readAsync-numSuccessfulOperations": 0,
        "Timing-root-read-minMsecs": 15.753,
        "Timing-file-readv-numSuccessfulOperations": 0,
        "Timing-file-readv-minMsecs": 0,
        "Timing-file-prefetch-maxMsecs": 0,
        "Timing-root-construct-totalMegabytes": 0,
        "Timing-tstoragefile-construct-numOperations": 5,
        "Timing-file-read-numSuccessfulOperations": 4680,
        "Timing-root-write-totalMsecs": 0,
        "Timing-file-readv-numOperations": 0,
        "Timing-root-prefetch-numSuccessfulOperations": 0,
        "Timing-file-prefetch-totalMegabytes": 0,
        "Timing-file-readv-totalMegabytes": 0,
        "Timing-root-readv-totalMegabytes": 96.7375,
        "Timing-tstoragefile-close-totalMegabytes": 0,
        "Timing-file-writev-minMsecs": 0,
        "Timing-file-construct-minMsecs": 0.000259,
        "Timing-tstoragefile-flush-numOperations": 3,
        "Timing-tstoragefile-readActual-minMsecs": 0.001295,
        "Timing-file-write-minMsecs": 0.005698,
        "ROOT-tfile-write-totalMegabytes": 0,
        "Timing-root-open-maxMsecs": 4379.9,
        "Timing-tstoragefile-readActual-numOperations": 4968,
        "Timing-tstoragefile-flush-maxMsecs": 1809.23,
        "Timing-root-close-maxMsecs": 0.402898,
        "Timing-root-open-totalMegabytes": 0,
        "Timing-tstoragefile-read-totalMegabytes": 8.08306,
        "Timing-root-position-totalMsecs": 0.032065,
        "Timing-file-open-numSuccessfulOperations": 2,
        "Timing-root-write-numOperations": 0,
        "Timing-tstoragefile-read-numOperations": 4737,
        "Timing-root-open-totalMsecs": 9945.23,
        "ROOT-tfile-read-totalMegabytes": 5.55608,
        "Parameter-untracked-string-cacheHint": "auto-detect",
        "Timing-root-write-maxMsecs": 0,
        "Timing-file-writev-numOperations": 0,
        "Timing-root-writev-numOperations": 0,
        "Timing-file-writev-numSuccessfulOperations": 0,
        "Timing-file-close-totalMegabytes": 0,
        "Timing-tstoragefile-write-totalMegabytes": 29.0666,
        "Timing-tstoragefile-close-maxMsecs": 0.40434,
        "Timing-file-flush-totalMegabytes": 0,
        "Timing-tstoragefile-readActual-totalMegabytes": 104.821,
        "Timing-file-open-maxMsecs": 2.23201,
        "Timing-tstoragefile-close-minMsecs": 0.152179,
        "Timing-tstoragefile-read-minMsecs": 0.001442,
        "Timing-tstoragefile-stat-numOperations": 697,
        "Timing-root-construct-totalMsecs": 0.000643,
        "Timing-root-writev-minMsecs": 0,
        "Timing-tstoragefile-read-numSuccessfulOperations": 4737,
        "Timing-file-position-maxMsecs": 0.065989,
        "Timing-tstoragefile-close-totalMsecs": 0.992356,
        "Parameter-untracked-bool-stats": True,
        "Timing-file-writev-totalMegabytes": 0,
        "Timing-root-read-totalMegabytes": 2.65289,
        "Timing-tstoragefile-write-totalMsecs": 224.665,
        "Timing-file-flush-totalMsecs": 1847.35,
        "Timing-tstoragefile-close-numSuccessfulOperations": 4,
        "Timing-tstoragefile-seek-maxMsecs": 0.05572,
        "Timing-root-prefetch-numOperations": 30,
        "Timing-root-open-minMsecs": 2228.61,
        "Timing-root-readv-totalMsecs": 16602.5,
        "Timing-tstoragefile-writeActual-numOperations": 7307,
        "Timing-file-read-maxMsecs": 15.5135,
        "Timing-root-stagein-totalMegabytes": 0,
        "Timing-root-close-totalMegabytes": 0,
        "Timing-tstoragefile-read-maxMsecs": 258.507,
        "Timing-root-write-minMsecs": 0,
        "Timing-tstoragefile-construct-maxMsecs": 4586.16,
        "Parameter-untracked-string-readHint": "auto-detect",
        "Timing-root-stagein-maxMsecs": 4.10695,
        "Timing-tstoragefile-writeActual-maxMsecs": 12.2421,
        "Timing-file-write-totalMsecs": 220.75,
        "Timing-file-readv-totalMsecs": 0,
        "Timing-tstoragefile-readAsync-maxMsecs": 0,
        "Timing-tstoragefile-seek-numOperations": 12044,
        "Timing-tstoragefile-writeViaCache-minMsecs": 0,
        "Timing-tstoragefile-write-numSuccessfulOperations": 7307,
        "Timing-tstoragefile-seek-numSuccessfulOperations": 12044,
        "Timing-tstoragefile-readViaCache-totalMsecs": 0,
        "Timing-tstoragefile-write-maxMsecs": 12.2428,
        "Timing-file-read-totalMegabytes": 5.43017,
        "Timing-tstoragefile-stat-numSuccessfulOperations": 697,
        "Timing-root-construct-maxMsecs": 0.000261,
        "Timing-tstoragefile-construct-minMsecs": 36.3145,
        "Timing-root-writev-maxMsecs": 0,
        "Timing-tstoragefile-readActual-totalMsecs": 18677.2,
        "Timing-tstoragefile-construct-totalMsecs": 11419,
        "Timing-root-writev-totalMsecs": 0,
        "Timing-file-construct-maxMsecs": 0.000291,
        "Timing-file-prefetch-minMsecs": 0,
        "Timing-root-stagein-numOperations": 1,
        "Timing-file-close-minMsecs": 0.190476,
        "Timing-file-construct-numOperations": 2,
        "Timing-file-construct-numSuccessfulOperations": 2,
        "Timing-root-stagein-minMsecs": 4.10695,
        "Timing-root-write-totalMegabytes": 0,
        "Timing-root-read-numSuccessfulOperations": 57,
        "Timing-tstoragefile-writeActual-minMsecs": 0.005877,
        "Timing-tstoragefile-readViaCache-numOperations": 3,
        "Timing-tstoragefile-readActual-maxMsecs": 4168.78,
        "Timing-file-close-numOperations": 1,
        "Timing-file-position-numOperations": 14759,
        "Timing-tstoragefile-read-totalMsecs": 2075,
        "Timing-root-position-maxMsecs": 0.001203,
        "Timing-root-construct-minMsecs": 0.000142,
        "Timing-tstoragefile-readViaCache-numSuccessfulOperations": 0,
        "Timing-file-position-numSuccessfulOperations": 14759,
        "Timing-tstoragefile-writeViaCache-maxMsecs": 0,
        "Timing-tstoragefile-writeViaCache-totalMegabytes": 0,
        "Timing-file-write-numOperations": 7307,
        "Timing-tstoragefile-flush-totalMsecs": 1847.36,
        "Timing-root-construct-numOperations": 3,
        "Timing-tstoragefile-seek-totalMsecs": 18.3395,
        "Timing-tstoragefile-stat-totalMegabytes": 0,
        "Timing-root-prefetch-totalMsecs": 0,
        "Timing-root-prefetch-maxMsecs": 0,
        "Timing-tstoragefile-writeActual-totalMsecs": 222.926
        }
    }


def CMSSWMetrics():
    """
    Helper function to convert CMSSW_METRICS dict values to their data-types
    :return: dictionary

    Output dictionary will be in form
    {
        "SystemMemory": {
            "Active(file)": <class 'int'>.
            "KernelStack": <class 'int'>,
            ...
        },
        ...
    }
    """
    rdict = {}
    for key, mdict in CMSSW_METRICS.items():
        if key == 'XrdSiteStatistics':
            rdict[key] = XrdMetrics(mdict)
            continue
        ndict = {}
        for mkey, mval in mdict.items():
            if isinstance(mval, (int, float, str, bool)):
                ndict[mkey] = type(mval)
        rdict[key] = ndict
    return rdict


def XrdMetrics(mdict):
    """
    Convert given XrdSiteStatistics dict values to their data-types
    :param mdict: XrdSiteStatistics dict
    :return: dictionary

    The output dictionary will be in a form of
    {
        "XrdSiteStatistics": {
            "readv-numOperations": [
              {
                "site": <class 'str'>,
                "value": <class 'int'>
              },
              ...
        }
        ...
    }
    """
    rdict = {}
    for key, values in mdict.items():
        entry = values[0]
        ndict = {}
        for mkey, mval in entry.items():
            if isinstance(mval, (int, float, str, bool)):
                ndict[mkey] = type(mval)
        rdict[key] = [ndict]
    return rdict
