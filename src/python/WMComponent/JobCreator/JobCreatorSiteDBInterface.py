#!/usr/bin/env python

"""
A clumsy interface to siteDB for jobCreator

"""
__all__ = []




import logging
import threading

from WMCore.Services.SiteDB.SiteDB import SiteDBJSON


class JobCreatorSiteDBInterface(SiteDBJSON):


    def getAllCMSNametoCE(self, file='result.json', clearCache = False):
        """
        I wrote this because the SEtoCMSname doesn't work the way I want, and I want
        the whole dictionary back.
        """
        

        result = ''
        if clearCache:
            self.clearCache(file)
        try:
            f = self.refreshCache(file, "CMSNametoCE?name")
            result = f.read()
            f.close()
        except IOError:
            raise RuntimeError("URL not available: %s" % callname )
        # When SiteDB sends proper json, we can use simplejson
        #return json.loads(result)
        return self.parser.dictParser(result)

    def getPledgedSlots(self, name):
        """
        Given a CMS site name, this should return the number of pledged slots

        """

        siteID = self.getSiteID(name)
        slots = 0

        if siteID:
            siteInfo = self.getJSON(callname = "Pledge", clearCache=True, site = siteID)['0']
            slots = siteInfo['job_slots - #']

        return slots
        


    def getSiteID(self, name):
        """
        When passed a CMS site name, this should return the siteID from getSiteDict()

        """

        siteDict = self.getSiteDict()

        if not name in siteDict.keys():
            logging.error("Asked for a site name %s that was not in siteDB" %(name))
            return 0

        return siteDict[name]
            



    def getSiteDict(self):
        """
        This returns the actual site->ID dictionary

        """


        siteDict = {"T0_CH_CERN"             : "40",
                    "T1_CH_CERN"             : "20",
                    "T1_DE_FZK"              : "22",
                    "T1_ES_PIC"              : "35",
                    "T1_FR_CCIN2P3"          : "8",
                    "T1_IT_CNAF"             : "32",
                    "T1_TW_ASGC"             : "19",
                    "T1_UK_RAL"              : "17",
                    "T1_US_FNAL"             : "18",
                    "T2_AT_Vienna"           : "481",
                    "T2_BE_IIHE"             : "1681",
                    "T2_BE_UCL"              : "1682",
                    "T2_BR_SPRACE"           : "38",
                    "T2_BR_UERJ"             : "49",
                    "T2_CH_CAF"              : "1241",
                    "T2_CH_CSCS"             : "16",
                    "T2_CN_Beijing"          : "5",
                    "T2_DE_DESY"             : "48",
                    "T2_DE_RWTH"             : "27",
                    "T2_EE_Estonia"          : "11",
                    "T2_ES_CIEMAT"           : "50",
                    "T2_ES_IFCA"             : "12",
                    "T2_FI_HIP"              : "622",
                    "T2_FR_CCIN2P3"          : "581",
                    "T2_FR_GRIF_IRFU"        : "26",
                    "T2_FR_GRIF_LLR"         : "26",
                    "T2_FR_IPHC"             : "541",
                    "T2_HU_Budapest"         : "2",
                    "T2_IN_TIFR"             : "681",
                    "T2_IT_Bari"             : "42",
                    "T2_IT_Legnaro"          : "37",
                    "T2_IT_Pisa"             : "13",
                    "T2_IT_Rome"             : "33",
                    "T2_KR_KNU"              : "30",
                    "T2_PK_NCP"              : "1782",
                    "T2_PL_Warsaw"           : "7",
                    "T2_PT_LIP_Coimbra"      : "621",
                    "T2_PT_LIP_Lisbon"       : "465",
                    "T2_RU_IHEP"             : "1722",
                    "T2_RU_INR"              : "1742",
                    "T2_RU_ITEP"             : "1745",
                    "T2_RU_JINR"             : "1743",
                    "T2_RU_PNPI"             : "1723",
                    "T2_RU_RRC_KI"           : "1744",
                    "T2_RU_SINP"             : "1741",
                    "T2_TR_METU"             : "781",
                    "T2_TW_Taiwan"           : "1",
                    "T2_UA_KIPT"             : "1421",
                    "T2_UK_London_Brunel"    : "15",
                    "T2_UK_London_IC"        : "15",
                    "T2_UK_SGrid_Bristol"    : "29",
                    "T2_UK_SGrid_RALPP"      : "28",
                    "T2_US_Caltech"          : "41",
                    "T2_US_Florida"          : "31",
                    "T2_US_MIT"              : "3",
                    "T2_US_Nebraska"         : "4",
                    "T2_US_Purdue"           : "43",
                    "T2_US_UCSD"             : "23",
                    "T2_US_Wisconsin"        : "44",
                    "T3_CH_PSI"              : "1541",
                    "T3_CN_PKU"              : "801",
                    "T3_CO_Uniandes"         : "1381",
                    "T3_DE_Karlsruhe"        : "9",
                    "T3_ES_Oviedo"           : "1104",
                    "T3_FR_IPNL"             : "683",
                    "T3_GR_Demokritos"       : "546",
                    "T3_GR_IASA"             : "761",
                    "T3_GR_Ioannina"         : "1461",
                    "T3_IT_Firenze"          : "1823",
                    "T3_IT_Napoli"           : "10",
                    "T3_IT_Padova"           : "1841",
                    "T3_IT_Perugia"          : "547",
                    "T3_IT_Trieste"          : "682",
                    "T3_TW_NCU"              : "1501",
                    "T3_UK_London_QMUL"      : "15",
                    "T3_UK_London_RHUL"      : "15",
                    "T3_UK_London_UCL"       : "15",
                    "T3_UK_SGrid_Oxford"     : "1521",
                    "T3_UK_ScotGrid_ECDF"    : "15",
                    "T3_UK_ScotGrid_GLA"     : "1801",
                    "T3_US_Colorado"         : "1401",
                    "T3_US_Cornell"          : "1103",
                    "T3_US_FIT"              : "1881",
                    "T3_US_FNALLPC"          : "1282",
                    "T3_US_FNALXEN"          : "1961",
                    "T3_US_FSU"              : "1922",
                    "T3_US_JHU"              : "1861",
                    "T3_US_Kansas"           : "1041",
                    "T3_US_Minnesota"        : "39",
                    "T3_US_Omaha"            : "1901",
                    "T3_US_Princeton"        : "24",
                    "T3_US_Princeton_ICSE"   : "1781",
                    "T3_US_Rice"             : "1561",
                    "T3_US_Rutgers"          : "1368",
                    "T3_US_TTU"              : "45",
                    "T3_US_UCLA"             : "701",
                    "T3_US_UCR"              : "301",
                    "T3_US_UIowa"            : "14",
                    "T3_US_UMD"              : "1342",
                    "T3_US_UTENN"            : "1701",
                    "T3_US_Vanderbilt"       : "261"  }


        return siteDict
