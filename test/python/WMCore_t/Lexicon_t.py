#!/usr/bin/python
"""
_Lexicon_t_

General test of Lexicon

"""

import unittest
import os
import json

from WMCore.WMBase import getTestBase
from WMCore.Lexicon import *
from WMCore.Lexicon import _gpuInternalParameters


class LexiconTest(unittest.TestCase):
    def testDBSUser(self):

        u1 = '/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118'
        u2 = '/C=IT/O=INFN/OU=Personal Certificate/L=Bari/CN=antonio pierro'
        u3 = '/DC=ch/DC=cern/OU=computers/CN=vocms39.cern.ch'
        u4 = '/C=BE/O=BEGRID/OU=IIHE(VUB)/OU=DNTK/CN=Maes Joris'
        u5 = 'cmsprod@vocms19.cern.ch'
        u6 = 'sfoulkes'
        u7 = '/DC=org/DC=doegrids/OU=People/CN=Si Xie 523253'
        u8 = '/C=IT/O=INFN/OU=Personal Certificate/L=Sns/CN=Federico Calzolari'
        u9 = '/O=GermanGrid/OU=RWTH/CN=Maarten Thomas'
        u10 = 'cmsdataops@cmssrv44.fnal.gov'
        u11 = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=ceballos/CN=488892/CN=Guillelmo Gomez Ceballos'
        for test_u in [u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11]:
            assert DBSUser(test_u), 'valid search not validated'

        u10 = 'Fred Bloggs'
        u11 = 'cmsprod@vocms19#'
        u12 = 'cms-xen@drop/'
        u13 = 'C=IT/O=INFN/OU=Personal Certificate/L=Sns/CN=Federico Calzolari'
        for test_u in [u10, u11]:
            self.assertRaises(AssertionError, DBSUser, test_u)

    def testSearchDataset(self):
        ds1 = '/Higgs/blah-v2/RECO'
        ds2 = '/Higgs*/blah-v2/RECO'
        ds3 = '/*/blah-v2/RECO'
        ds4 = '/Higgs/blah*/RECO'
        ds5 = '/Higgs/blah-v2/*'
        ds6 = '/*/*/RECO'
        ds7 = '/Higgs/*/*'
        ds8 = '/*/blah-v2/*'
        ds9 = '/QCD_EMenriched_Pt30to80/Summer08_IDEAL_V11_redigi_v2/GEN-SIM-RAW'
        ds10 = '/*'
        ds11 = '/*QCD'
        ds12 = '/QCD*/Summer*'
        ds13 = '/QCD_EMenriched_Pt30to80/Summer08_IDEAL_V11_redigi_v2/GEN-SIM-*'
        ds14 = '/QCD_EMenriched_Pt30to80/Summer08_IDEAL_V11_redigi_v2/*-SIM-*'

        for test_ds in [ds1, ds2, ds3, ds4, ds5, ds6, ds9, ds10, ds11, ds12, ds13, ds14]:
            assert searchdataset(test_ds), 'valid search not validated'

        ds1 = '*/blah-v2/RECO'
        ds2 = '*/blah-v2/*'
        ds3 = '*/*//RECO'
        ds4 = '/*/*/*/*'
        ds5 = '*'
        ds6 = '/Higgs/ /RECO'
        ds7 = '/Higgs/%/RECO'
        ds8 = 'Higgs'
        ds9 = 'RECO'
        ds10 = 'blah-v2'
        ds11 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        for test_ds in [ds1, ds2, ds3, ds4, ds5, ds6, ds7, ds8, ds9, ds10, ds11]:
            self.assertRaises(AssertionError, searchdataset, test_ds)

    def testSearchBlock(self):
        ds1 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/GEN-SIM-RAW#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds2 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO#*'
        ds3 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/*#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds4 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO#bdd066ce-e8fb-488e-beb1-*'
        ds5 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds6 = '/*Minimum*'
        ds7 = '/*'
        ds8 = '/*/BeamCommissioning09*/*#bdd066ce-e8fb-488e-beb1-2043'
        ds9 = '/*/*/RECO#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds10 = '/*/RECO#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds11 = '/*RECO*/*#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds12 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO*'
        ds13 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO*#*1234-5677-abcdessssssssss-123444'

        for test_ds in [ds1, ds2, ds3, ds4, ds5, ds6, ds7, ds8, ds9, ds10, ds11, ds12, ds13]:
            assert searchblock(test_ds), 'valid search not validated'

        ds1 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO#bdd066ce-e8fb-488e/beb1/20432d96baaa'
        ds2 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECObdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds3 = '/MinimumBias/BeamCommissioning09-PromptReco-v2/RECO/bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds4 = '/RECO#bdd066ce-e8fb-488e-beb1-20432d96baaa'
        ds5 = '/*#bdd066ce-e8fb-488e-beb1-20432d96baaa'

        for test_ds in [ds1, ds2, ds3, ds4, ds5]:
            self.assertRaises(AssertionError, searchblock, test_ds)

    def testPublishdatasetname(self):
        assert publishdatasetname('pog-Summer09-MC_31X_V3_SD_ZeroBias-v1_Full_v0CandProducerPAT'), \
            'valid publishdatasetname not validated'
        # shouldn't contain .
        self.assertRaises(AssertionError, publishdatasetname, 'withdot.pre3_IDEAL_30X_v1')

    def testGoodProcdataset(self):
        # Check that valid procdataset work
        assert procdataset('CMSSW_4_4_0_pre7_g494p02-START44_V2_special_110808-v1'), 'valid procdataset not validated'
        assert procdataset('Summer11-PU_S4_START42_V11-v1'), 'valid procdataset not validated'
        assert procdataset('CMSSW_3_0_0_pre3_IDEAL_30X-v1'), 'valid procdataset not validated'
        assert procdataset('CMSSW_3_0_0_pre3_IDEAL_30X-my_filter-my_string-v1'), 'valid procdataset not validated'
        longStr = 'a' * 99 + "-" + 'b' * 96 + "-v1"
        assert procdataset(longStr), 'valid procdataset %s length fail' % len(longStr)

    def testBadProcdataset(self):
        # Check that invalid Procdataset raise an exception
        self.assertRaises(AssertionError, procdataset, 'Drop Table')
        self.assertRaises(AssertionError, procdataset, 'Alter Table')
        self.assertRaises(AssertionError, procdataset, 'CMSSW_3_0_0_pre3_IDEAL_30X_v1')
        self.assertRaises(AssertionError, procdataset, '')
        self.assertRaises(AssertionError, procdataset, None)
        longStr = 'a' * 100 + "-" + 'b' * 96 + "-v1"
        self.assertRaises(AssertionError, procdataset, longStr)

    def testGoodUserProcDataset(self):
        dsList = ['weinberg-StealthSusy_mm16_RECO_AOD_Z2-689dc471cdaaa10be587d0cc7c95f00f',
                  'tracker-pog-Summer09-MC_31X_V3_SD_ZeroBias-v1_Full_v0CandProducerPAT-6b5c47aa1f79fc09d5b81a20702e3621',
                  'StoreResults-Summer12_DR53X-PU_S10_START53_V7A-v1_TLBSM_53x_v3_bugfix_v1-99bd99199697666ff01397dad5652e9e']
        for ds in dsList:
            self.assertTrue(userprocdataset(ds))
        longStr = 'a' * 99 + '-' + 'b' * 66 + '-' + 'c' * 32
        assert userprocdataset(longStr), 'valid userdataset %s length fail' % len(longStr)

    def testBadUserProcDataset(self):
        dsList = ['weinberg-StealthSusy_mm16_RECO_AOD_Z2-689dc471cdaaa10be587d0cc7c95z00f',
                  'weinberg-StealthSusy_mm16_RECO_AOD_Z2-689dc471cdaaa10be587d0cc7c95z00f1'
                  'tracker-pog-Summer09-MC_31X_V3_SD_ZeroBias-v1_Full_v0#CandProducerPAT-6b5c47aa1f79fc09d5b81a20702e3621']
        for ds in dsList:
            self.assertRaises(AssertionError, userprocdataset, ds)
        # 201 length
        longStr = 'a' * 100 + '-' + 'b' * 66 + '-' + 'c' * 32
        self.assertRaises(AssertionError, userprocdataset, longStr)

    def testGoodPrimdataset(self):
        # Check that valid Primdataset work
        assert primdataset('qqH125-2tau'), 'valid primdataset not validated'
        assert primdataset('RelVal124QCD_pt120_170'), 'valid primdataset not validated'
        assert primdataset('RelVal160pre14SingleMuMinusPt10'), 'valid primdataset not validated'
        longStr = 'a' * 99
        assert primdataset(longStr), 'valid primdataset %s length failed' % len(longStr)

    def testBadPrimdataset(self):
        # Check that invalid Primdataset raise an exception
        self.assertRaises(AssertionError, primdataset, 'Drop Table')
        self.assertRaises(AssertionError, primdataset, 'Alter Table')
        longStr = 'a' * 100
        self.assertRaises(AssertionError, primdataset, longStr)

    def testGoodBlock(self):

        bList = [
            "/ZPrimeToTTJets_M500GeV_W5GeV_TuneZ2star_8TeV-madgraph-tauola/StoreResults-Summer12_DR53X-PU_S10_START53_V7A-v1_TLBSM_53x_v3_bugfix_v1-99bd99199697666ff01397dad5652e9e/USER#620a38a9-29ba-4af4-b650-e2ba07d133f3",
            "/DoubleMu/aburgmei-Run2012A_22Jan2013_v1_RHembedded_trans1_tau121_ptelec1_17elec2_8_v4-f456bdbb960236e5c696adfe9b04eaae/USER#1f1eee22-cdee-0f1b-271b-77a7f559e7dd"]
        for bk in bList:
            assert block(bk), "validation failed"
        longStr = "/" + 'a' * 99 + "/" + 'a' * 199 + "/" + 'A' * 99 + "#" + 'a' * 99
        assert block(longStr), 'valid block %s length failed' % len(longStr)

    def testBadBlock(self):

        bList = ["/ZPrimeToTTJets/StoreResults-Summer12_DR53X-P",
                 "/DoubleMu/aburgme/USER1f1eee22-cdee-0f1b-271b-77a7f559e7dd"]
        for bk in bList:
            self.assertRaises(AssertionError, block, bk)
        longStr = "/" + 'a' * 100 + "/" + 'a' * 200 + "/" + 'a' * 99 + "#" + 'a' * 99
        self.assertRaises(AssertionError, block, longStr)

    def testProcVersion(self):
        """
        _testProcVersion_

        Test whether we can correctly identify
        a good processing version
        """
        self.assertRaises(AssertionError, procversion, 'version')
        self.assertRaises(AssertionError, procversion, 'a280')
        self.assertTrue(procversion('1'))
        self.assertTrue(procversion('88'))
        return

    def testGoodAcqName(self):
        """
        _testGoodAcqName_

        Test some valid AcquisitionEra names
        """
        self.assertTrue(acqname('a22'))
        self.assertTrue(acqname('aForReals'))
        self.assertTrue(acqname('Run1016B'))
        self.assertTrue(acqname('CMSSW_8_0_16_patch1'))
        return

    def testBadAcqName(self):
        """
        _testBadAcqName_

        Test some invalid AcquisitionEra names
        """
        self.assertRaises(AssertionError, acqname, '*Nothing')
        self.assertRaises(AssertionError, acqname, '1version')
        self.assertRaises(AssertionError, acqname, '_version')
        self.assertRaises(AssertionError, acqname, 'AcqEra Spaced')
        self.assertRaises(AssertionError, acqname, '')
        self.assertRaises(AssertionError, acqname, None)
        self.assertRaises(AssertionError, acqname, 'Run2016B-Terrible')
        return

    def testGoodCampaign(self):
        """
        _testGoodCampaign_

        Test some valid Campaign names
        """
        self.assertTrue(campaign(''))
        self.assertTrue(campaign(None))
        self.assertTrue(campaign('a22'))
        self.assertTrue(campaign('aForReals'))
        self.assertTrue(campaign('Run1016B'))
        self.assertTrue(campaign('CMSSW_9_1_0_pre2__UPSG_Tracker_PU200-1492810586'))
        return

    def testBadCampaign(self):
        """
        _testBadCampaign_

        Test some invalid Campaign names
        """
        self.assertRaises(AssertionError, campaign, '*Nothing')
        self.assertRaises(AssertionError, campaign, 'B@dCampaign')
        self.assertRaises(AssertionError, campaign, '\version')
        self.assertRaises(AssertionError, campaign, '/bad-campaign')
        self.assertRaises(AssertionError, campaign, 'spaced campaign')
        self.assertRaises(AssertionError, campaign, 'a_very_very_very__very__very__very__long_long_long_long_invalid_campaign_with_81_chars')
        return

    def testGoodSearchstr(self):
        # Check that valid searchstr work
        assert searchstr('/store/mc/Fall08/BBJets250to500-madgraph/*'), 'valid search string not validated'
        assert searchstr(
            '/QCD_EMenriched_Pt30to80/Summer08_IDEAL_V11_redigi_v2/GEN-SIM-RAW#cfc0a501-e845-4576-af8a-f811165d82d9'), 'valid search string not validated'
        assert searchstr('STREAMER'), 'valid search string not validated'

    def testBadSearchstr(self):
        # Check that invalid searchstr raise an exception
        self.assertRaises(AssertionError, searchstr, 'Drop Table')
        self.assertRaises(AssertionError, searchstr, 'Alter Table')

    def testGoodNamestr(self):
        # Check that valid namestr work
        assert namestr('LUMI-VDM'), 'valid name string not validated'
        assert namestr('PhysVal'), 'valid name string not validated'
        assert namestr('cosmic'), 'valid name string not validated'

    def testBadNamestr(self):
        # Check that invalid namestr raise an exception
        self.assertRaises(AssertionError, namestr, 'insert into Table')
        self.assertRaises(AssertionError, namestr, 'drop database')

    def testGoodSiteTier(self):
        # Check that valid tiers work
        assert sitetier('T0'), 'valid tier not validated'
        assert sitetier('T1'), 'valid tier not validated'
        assert sitetier('T2'), 'valid tier not validated'
        assert sitetier('T3'), 'valid tier not validated'

    def testBadSiteTier(self):
        # Check that invalid tiers raise an exception
        self.assertRaises(AssertionError, sitetier, 'T4')
        self.assertRaises(AssertionError, sitetier, 'D0')

    def testGoodJobranges(self):
        # Check that valid tiers work
        assert jobrange('3'), 'valid job range not validated'
        assert jobrange('78'), 'valid job range not validated'
        assert jobrange('3-9'), 'valid job range not validated'
        assert jobrange('32-97'), 'valid job range not validated'
        assert jobrange('2,3'), 'valid job range not validated'
        assert jobrange('24,39'), 'valid job range not validated'
        assert jobrange('2-91,45,5,64,7,78-91'), 'valid job range not validated'

    def testBadJobranges(self):
        # Check that invalid tiers raise an exception
        self.assertRaises(AssertionError, jobrange, 'I')
        self.assertRaises(AssertionError, jobrange, '1-2i')
        self.assertRaises(AssertionError, jobrange, '1-i2')
        self.assertRaises(AssertionError, jobrange, '1,2-3,3d-5')
        self.assertRaises(AssertionError, jobrange, '-1,2-3,3d-5')
        self.assertRaises(AssertionError, jobrange, '1-2,2,-5')

    def testGoodCMSName(self):
        # Check that full names work
        assert cmsname('T0_CH_CERN'), 'valid CMS name not validated'
        assert cmsname('T2_UK_SGrid_Bristol'), 'valid CMS name not validated'
        assert cmsname('T2_FR_CCIN2P3'), 'valid CMS name not validated'
        assert cmsname('T3_US_FNAL_LPC_Cloud'), 'valid CMS name not validated'

    def testBadCMSName(self):
        # Check that invalid names raise an exception
        self.assertRaises(AssertionError, cmsname, 'T5_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2-UK-SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2_UK_SGrid_Bris-tol')
        self.assertRaises(AssertionError, cmsname, 'D2_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2asjkjhadshjkdashjkasdkjhdas')
        self.assertRaises(AssertionError, cmsname, 'T2_UK')

    def testGoodIdentifier(self):
        for ok in ['__wil.1.am__', '.']:
            assert identifier(ok)

    def testBadIdentifier(self):
        for notok in ['ke$ha', '<begin>']:
            self.assertRaises(AssertionError, identifier, notok)

    def testGoodDataset(self):
        assert dataset('/a/b/C')
        assert dataset('/m000n/RIII-ver/WIDER-THAN-MILES')

    def testBadDataset(self):
        for notok in ['/Sugar/Sugar', '/Oh/honey/honey!', '/You/are/my/candy/GIIIRRL']:
            self.assertRaises(AssertionError, dataset, notok)

    def testVersion(self):
        for ok in ['CMSSW_3_8_0_pre1', 'CMSSW_1_2_0', 'CMSSW_4_0_0_patch11', 'CMSSW_3_10_0_pre9G493']:
            assert cmsswversion(ok)

    def testBadVersion(self):
        for notok in ['ORCA_3_8_0', 'CMSSW_3_5']:
            self.assertRaises(AssertionError, cmsswversion, notok)

    def testGoodCouchUrl(self):
        for ok in ['http://vittoria@antimatter.cern.ch:5984',
                   'https://fbi.fnal.gov:5984',
                   'http://fmulder:trustno1@fbi.fnal.gov:5984',
                   'http://localhost:443',
                   'http://127.0.0.1:1234',
                   'http://0.0.0.0:4321',
                   'http://1.2.3.4:5678',
                   'http://han:solo@1.2.3.4:9876',
                   'http://luke:skywalker@localhost:7654/some_db/some_doc',
                   'https://cmsreqmgr.cern.ch/couchdb/db1/doc',
                   'http://1.2.3.4:5184/somedb/some_dir/doc',
                   'http://iam.anexternal.host:5184/somedb/some_doc/some_doc']:
            assert couchurl(ok)

    def testBadCouchUrl(self):
        for notok in ['agent86@control.fnal.gov:5984', 'http:/localhost:443', 'http://www.myspace.com']:
            self.assertRaises(AssertionError, couchurl, notok)

    def testHNName(self):
        """
        _testHNName_

        Test the HN name checker
        """

        hnName('ewv2')
        hnName('m.cinquilli')
        self.assertRaises(AssertionError, hnName, 'invalid-user')

    def testLFN(self):
        """
        _testLFN_

        Test the LFN checker in several modes, including user LFNs
        """
        lfnA = '/store/mc/Fall10/DYToMuMu_M-20_TuneZ2_7TeV-pythia6/AODSIM/START38_V12-v1/0003/C0F3344F-6EC8-DF11-8ED6-E41F13181020.root'
        lfn(lfnA)
        lfnA = '/store/mc/2008/2/21/FastSim-CSA07Electron-1203615548/0009/B6E531DD-99E1-DC11-9FEC-001617E30D4A.root'
        lfn(lfnA)
        lfnA = '/store/temp/user/ewv/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/temp/user/cinquilli.nocern/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/user/ewv/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/user/cinquilli.nocern/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/temp/group/Exotica/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/group/Exotica/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/temp1/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/temp/lustre1/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/data/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/data/Run2010A/Cosmics/RECO/v4/000/143/316/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/hidata/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        lfn(lfnA)
        lfnA = '/store/hidata/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/hidata/HIRun2011/HIMinBiasUPC/RECO/PromptReco-v1/000/182/591/449805F5-7F1B-E111-AC84-E0CB4E55365D.root'
        lfn(lfnA)
        lfnA = '/store/t0temp/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/unmerged/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/himc/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/t0temp/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/unmerged/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/Run2012B/Cosmics/RAW-RECO/PromptSkim-v1/000/194/912/00000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/results/qcd/QCD_Pt80/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/QCD_Pt80/USER/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/0000/C44630AC-C0C7-DE11-AD4E-0019B9CAC0F8.root'
        lfn(lfnA)
        lfnA = '/store/results/qcd/StoreResults/QCD_Pt_40_2017_14TeV_612_SLHC6_patch1/USER/QCD_Pt_40_2017_14TeV_612_SLHC6_patch1_6be6d116203e430d91d7e1d6d9a88cd7-v1/00000/028DDC2A-63A8-E311-BB40-842B2B5546DE.root'
        lfn(lfnA)
        lfnA = '/store/user/fanzago/RelValZMM/FanzagoTutGrid/f30a6bb13f516198b2814e83414acca1/outfile_10_2_tw4.root'
        lfn(lfnA)
        lfnA = '/store/group/higgs/SDMu9_Zmumu/Zmumu/OctX_HZZ3lepSkim_SDMu9/1eb161a436e69f7af28d18145e4ce909/3lepSkim_SDMu9_1.root'
        lfn(lfnA)
        lfnA = '/store/group/e-gamma_ecal/SDMu9_Zmumu/Zmumu/OctX_HZZ3lepSkim_SDMu9/1eb161a436e69f7af28d18145e4ce909/3lepSkim_SDMu9_1.root'
        lfn(lfnA)
        lfnA = '/store/group/B2G/SDMu9_Zmumu/Zmumu/OctX_HZZ3lepSkim_SDMu9/1eb161a436e69f7af28d18145e4ce909/3lepSkim_SDMu9_1.root'
        lfn(lfnA)
        lfnA = '/store/group/phys_higgs/meridian/HGGProd/GluGluToHToGG_M-125_8TeV-powheg-pythia6-Summer12-START53_V7D-v2/meridian/GluGlu_HToGG_M-125_8TeV-powheg-LHE_v1/GluGluToHToGG_M-125_8TeV-powheg-pythia6-Summer12-START53_V7D-v2/fb576e5b6a5810681def50b608ec31ad/Hadronizer_TuneZ2star_8TeV_Powheg_pythia_tauola_cff_py_GEN_SIM_DIGI_L1_DIGI2RAW_RAW2DIGI_L1Reco_RECO_PU_1_1_ukQ.root'
        lfn(lfnA)
        lfnA = '/store/lhe/7365/TprimeTprimeToTHTH_M-400_TuneZ2star_8TeV-madgraph_50219221.lhe'
        lfn(lfnA)
        lfnA = '/store/lhe/10860/LQToUE_BetaHalf_vector_YM-MLQ300LG0KG0.lhe.xz'
        lfn(lfnA)
        lfnA = '/store/lhe/7365/mysecondary/0001/TprimeTprimeToTHTH_M-400_TuneZ2star_8TeV-madgraph_50219221.lhe'
        lfn(lfnA)
        lfnA = '/store/lhe/10860/mysecondary/0001/LQToUE_BetaHalf_vector_YM-MLQ300LG0KG0.lhe.xz'
        lfn(lfnA)

        # All these cases should fail
        lfnA = '/storeA/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/Temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/Lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_;10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron;-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-rECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1;/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000a/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X;-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/000/000/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/000/Iamralph/000/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)

        # All these cases should fail
        lfnA = '/storeA/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/Temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_;10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron;-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-rECO/vX-1/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1;/1000/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000a/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X;-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)

        lfnA = '/store/temp/user/ewv/Higgs;123/PrivateSample/v1/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/user/ewv/Higgs-123/Private;Sample/v1/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/user/ewv/Higgs-123/PrivateSample/v1;/a_X-2.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/mc/2008/2/FastSim-CSA07Electron-1203615548/B6E531DD-99E1-DC11-9FEC-001617E30D4A.root'
        self.assertRaises(AssertionError, lfn, lfnA)

        lfnA = '/store/Results/qcd/QCD_Pt80/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/QCD_Pt80/USER/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/0000/C44630AC-C0C7-DE11-AD4E-0019B9CAC0F8.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/Results/qcd123/QCD_Pt80/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/QCD_Pt80/USER/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/0000/C44630AC-C0C7-DE11-AD4E-0019B9CAC0F8.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/results/qcd/QCD_Pt80/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/QCD_Pt80/USER/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/0000a/C44630AC-C0C7-DE11-AD4E-0019B9CAC0F8.root'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/lhe/10860/11%11/1111/LQToUE_BetaHalf_vector_YM-MLQ300LG0KG0.lhe.xz'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/lhe/TprimeTprimeToTHTH_M-400_TuneZ2star_8TeV-madgraph_50219221.lhe'
        self.assertRaises(AssertionError, lfn, lfnA)
        return

    def testLFNBase(self):
        """
        _testLFNBase_

        Test the LFN Base
        """

        lfnA = '/store/temp/user/ewv/Higgs-123/PrivateSample/v1'
        lfnBase(lfnA)
        lfnA = '/store/user/ewv/Higgs-123/PrivateSample/v1'
        lfnBase(lfnA)
        lfnA = '/store/temp/group/Exotica/Higgs-123/PrivateSample/v1'
        lfnBase(lfnA)
        lfnA = '/store/user/group/Exotica/PrivateSample/v1'
        lfnBase(lfnA)
        lfnA = '/store/temp1/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        lfnBase(lfnA)
        lfnA = '/store/temp/lustre1/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        lfnBase(lfnA)

        lfnA = '/store/hidata/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        lfnBase(lfnA)
        lfnA = '/store/hidata/Run2010A/Cosmics/RECO/v4'
        lfnBase(lfnA)
        lfnA = '/store/results/qcd/StoreResults/QCD_Pt_40_2017_14TeV_612_SLHC6_patch1/USER/QCD_Pt_40_2017_14TeV_612_SLHC6_patch1_6be6d116203e430d91d7e1d6d9a88cd7-v1'
        lfnBase(lfnA)
        lfnA = '/Store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/Temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/Lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition;_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10;_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-rECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX;-1'
        self.assertRaises(AssertionError, lfn, lfnA)

        lfnA = '/Store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/Temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition;_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10;_100/RAW-RECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-rECO/vX-1'
        self.assertRaises(AssertionError, lfn, lfnA)
        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX;-1'
        self.assertRaises(AssertionError, lfn, lfnA)

        return

    def testUserLFN(self):
        """
        _testUserLFN_

        Test the LFN checker for user LFNs that are not part of a dataset (TFiles, logs)
        """

        lfnA = '/store/temp/user/ewv/CRAB-Out/ewv_crab_EwvAnalysis_46_111109_143552/output/total_0001.json'
        userLfn(lfnA)
        lfnA = '/store/user/ewv/CRAB-Out/ewv_crab_EwvAnalysis_46_111109_143552/output/total_0001.json'
        userLfn(lfnA)
        lfnA = "/store/temp/user/ewv/CRAB-Out/ewv_crab_EwvAnalysis_45_111109_130928/output/histo_0002.root"
        userLfn(lfnA)

        return

    def testUserLFNBase(self):
        """
        _testLFNBase_

        Test the LFN Base checker for user LFNs that are not part of a dataset (TFiles, logs)
        """

        lfnA = '/store/temp/user/ewv/CRAB-Out/ewv_crab_EwvAnalysis_46_111109_143552/output'
        userLfnBase(lfnA)
        lfnA = '/store/user/ewv/CRAB-Out/ewv_crab_EwvAnalysis_46_111109_143552/output'
        userLfnBase(lfnA)

        return

    def testLFNParser(self):
        """
        _testLFNParser_

        Check and make sure that we parse LFNs correctly
        """

        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        result = parseLFN(lfnA)

        self.assertEqual(result['baseLocation'], '/store/temp')
        self.assertEqual(result['acquisitionEra'], 'acquisition_10-A')
        self.assertEqual(result['primaryDataset'], 'MuElectron-10_100')
        self.assertEqual(result['dataTier'], 'RAW-RECO')
        self.assertEqual(result['processingVersion'], 'vX-1')
        self.assertEqual(result['lfnCounter'], '1000')
        self.assertEqual(result['filename'], 'a_X-2.root')

        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1/1000/a_X-2.root'
        result = parseLFN(lfnA)

        self.assertEqual(result['baseLocation'], '/store/temp/lustre')
        self.assertEqual(result['acquisitionEra'], 'acquisition_10-A')
        self.assertEqual(result['primaryDataset'], 'MuElectron-10_100')
        self.assertEqual(result['dataTier'], 'RAW-RECO')
        self.assertEqual(result['processingVersion'], 'vX-1')
        self.assertEqual(result['lfnCounter'], '1000')
        self.assertEqual(result['filename'], 'a_X-2.root')

        lfnA = '/store/temp/user/ewv/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        result = parseLFN(lfnA)

        self.assertEqual(result['baseLocation'], '/store/temp/user')
        self.assertEqual(result['primaryDataset'], 'Higgs-123')
        self.assertEqual(result['secondaryDataset'], 'PrivateSample')
        self.assertEqual(result['processingVersion'], 'v1')
        self.assertEqual(result['filename'], 'a_X-2.root')

        lfnA = '/store/user/ewv/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        result = parseLFN(lfnA)

        self.assertEqual(result['baseLocation'], '/store/user')
        self.assertEqual(result['primaryDataset'], 'Higgs-123')
        self.assertEqual(result['secondaryDataset'], 'PrivateSample')
        self.assertEqual(result['processingVersion'], 'v1')
        self.assertEqual(result['filename'], 'a_X-2.root')

        lfnA = '/store/group/Exotica/Higgs-123/PrivateSample/v1/1000/a_X-2.root'
        result = parseLFN(lfnA)

        self.assertEqual(result['baseLocation'], '/store/group')
        self.assertEqual(result['primaryDataset'], 'Higgs-123')
        self.assertEqual(result['secondaryDataset'], 'PrivateSample')
        self.assertEqual(result['processingVersion'], 'v1')
        self.assertEqual(result['filename'], 'a_X-2.root')

        return

    def testLFNBaseParser(self):
        """
        _testLFNBaseParser_

        Test the parsing for LFN Base
        """

        lfnA = '/store/temp/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        result = parseLFNBase(lfnA)

        self.assertEqual(result['baseLocation'], '/store/temp')
        self.assertEqual(result['acquisitionEra'], 'acquisition_10-A')
        self.assertEqual(result['primaryDataset'], 'MuElectron-10_100')
        self.assertEqual(result['dataTier'], 'RAW-RECO')
        self.assertEqual(result['processingVersion'], 'vX-1')

        lfnA = '/store/temp/lustre/acquisition_10-A/MuElectron-10_100/RAW-RECO/vX-1'
        result = parseLFNBase(lfnA)

        self.assertEqual(result['baseLocation'], '/store/temp/lustre')
        self.assertEqual(result['acquisitionEra'], 'acquisition_10-A')
        self.assertEqual(result['primaryDataset'], 'MuElectron-10_100')
        self.assertEqual(result['dataTier'], 'RAW-RECO')
        self.assertEqual(result['processingVersion'], 'vX-1')

        lfnA = '/store/temp/user/ewv/Higgs-123/PrivateSample/v1'
        result = parseLFNBase(lfnA)

        self.assertEqual(result['baseLocation'], '/store/temp/user')
        self.assertEqual(result['primaryDataset'], 'Higgs-123')
        self.assertEqual(result['secondaryDataset'], 'PrivateSample')
        self.assertEqual(result['processingVersion'], 'v1')

        lfnA = '/store/user/ewv/Higgs-123/PrivateSample/v1'
        result = parseLFNBase(lfnA)

        self.assertEqual(result['baseLocation'], '/store/user')
        self.assertEqual(result['primaryDataset'], 'Higgs-123')
        self.assertEqual(result['secondaryDataset'], 'PrivateSample')
        self.assertEqual(result['processingVersion'], 'v1')

        lfnA = '/store/group/Exotica/Higgs-123/PrivateSample/v1'
        result = parseLFNBase(lfnA)

        self.assertEqual(result['baseLocation'], '/store/group')
        self.assertEqual(result['primaryDataset'], 'Higgs-123')
        self.assertEqual(result['secondaryDataset'], 'PrivateSample')
        self.assertEqual(result['processingVersion'], 'v1')

        return

    def testSanitizeURL(self):
        proto = "http"
        host = "test.com"
        user = "abc"
        passwd = "^cba$"
        port = "9999"
        url = "%s://%s:%s@%s:%s" % (proto, user, passwd, host, port)
        urlDict = sanitizeURL(url)
        self.assertEqual(urlDict['url'], "%s://%s:%s" % (proto, host, port))
        self.assertEqual(urlDict['username'], user)
        self.assertEqual(urlDict['password'], passwd)

        noPassURL = "http://test.com"
        urlDict = sanitizeURL(noPassURL)
        self.assertEqual(urlDict['url'], noPassURL)
        self.assertEqual(urlDict['username'], None)
        self.assertEqual(urlDict['password'], None)

    def testReplaceToSantizeURL(self):
        """
        Checks that a URI containing a username password has that information
        removed when an error occurs.
        """
        dbUrl = "mysql://DBUSER:DBSPASSWORD@localhost:80808/WMAgentDB"
        errorMsg = "DB failure: %s" % (dbUrl)
        dbUrl2 = "mysql://localhost:80808/WMAgentDB"
        sanitizedError = "DB failure: %s" % (dbUrl2)
        self.assertEqual(replaceToSantizeURL(errorMsg), sanitizedError)

        dbUrl = "MySql://DBUSER:DBSPASSWORD@localhost:80808/WMAgentDB"
        errorMsg = "DB failure: %s" % (dbUrl)
        dbUrl2 = "MySql://localhost:80808/WMAgentDB"
        sanitizedError = "DB failure: %s" % (dbUrl2)
        self.assertEqual(replaceToSantizeURL(errorMsg), sanitizedError)

        dbUrl = "mysql://localhost:80808/WMAgentDB"
        errorMsg = "DB failure: %s" % (dbUrl)
        dbUrl2 = "mysql://localhost:80808/WMAgentDB"
        sanitizedError = "DB failure: %s" % (dbUrl2)
        self.assertEqual(replaceToSantizeURL(errorMsg), sanitizedError)

        dbUrl = "http://DBUSER:DBSPASSWORD@localhost:80808/WMAgentDB"
        errorMsg = "DB failure: %s" % (dbUrl)
        dbUrl2 = "http://localhost:80808/WMAgentDB"
        sanitizedError = "DB failure: %s" % (dbUrl2)
        self.assertEqual(replaceToSantizeURL(errorMsg), sanitizedError)

        dbUrl = "HtTp://DBUSER:DBSPASSWORD@localhost:80808/WMAgentDB"
        errorMsg = "DB failure: %s" % (dbUrl)
        dbUrl2 = "HtTp://localhost:80808/WMAgentDB"
        sanitizedError = "DB failure: %s" % (dbUrl2)
        self.assertEqual(replaceToSantizeURL(errorMsg), sanitizedError)

        dbUrl = "http://localhost:80808/WMAgentDB"
        errorMsg = "DB failure: %s" % (dbUrl)
        dbUrl2 = "http://localhost:80808/WMAgentDB"
        sanitizedError = "DB failure: %s" % (dbUrl2)
        self.assertEqual(replaceToSantizeURL(errorMsg), sanitizedError)

    def testSplitCouchServiceURL(self):

        urlSplit = splitCouchServiceURL("https://cmsweb-dev.cern.ch:8888/workqueue")
        self.assertEqual("https://cmsweb-dev.cern.ch:8888", urlSplit[0])
        self.assertEqual("workqueue", urlSplit[1])

        urlSplit = splitCouchServiceURL("https://cmsweb-dev.cern.ch/couchdb/workqueue")
        self.assertEqual("https://cmsweb-dev.cern.ch/couchdb", urlSplit[0])
        self.assertEqual("workqueue", urlSplit[1])

        return

    def testGlobalTag(self):
        """
        Test and check with some global tags.

        """

        gTag = 'START_V2::ALL'
        globalTag(gTag)
        gTag = 'START_V2;;ALL'
        self.assertRaises(AssertionError, globalTag, gTag)

        return

    def testUrlValidation(self):
        """
        Test the validateUrl function for some use case of DBS 3
        """
        # Good http(s) urls
        for url in ['https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader',
                    'https://mydbs.mydomain.de:8443',
                    'http://localhost',
                    'http://192.168.1.1',
                    'https://192.168.1.1:443',
                    'http://[2001:0db8:85a3:08d3:1319:8a2e:0370:7344]/',
                    'http://2001:0db8:85a3:08d3:1319:8a2e:0370:7344/',
                    'http://[2001:0db8:85a3:08d3:1319:8a2e:0370:7344]:8443/']:
            validateUrl(url)

        # Bad http(s) urls
        for url in ['ftp://dangerous.download.this',
                    'https:/notvalid.url',
                    'http://-NiceTry',
                    'https://NiceTry; DROP Table;--',
                    'http://123123104122',
                    'http://.www.google.com',
                    'http://[2001:0db8:85a3:08d3:1319:8a2z:0370:7344]/', ]:
            self.assertRaises(AssertionError, validateUrl, url)

    def testPrimaryDatasetType(self):
        self.assertRaises(AssertionError, primaryDatasetType, "MC")
        self.assertTrue(primaryDatasetType("mc"), "mc should be allowed")
        self.assertTrue(primaryDatasetType("data"), "data should be allowed")
        self.assertTrue(primaryDatasetType("cosmic"), "data should be allowed")
        self.assertTrue(primaryDatasetType("test"), "test should be allowed")

    def testPhysicsGroup(self):
        """
        _testPhysicsGroup_

        Test a few use cases for PhysicsGroup
        """
        self.assertRaises(AssertionError, physicsgroup, '')
        self.assertRaises(AssertionError, physicsgroup, 'A-30-length-Str_that_is_not_allowed')

        self.assertTrue(physicsgroup('1'))
        self.assertTrue(physicsgroup('A'))
        self.assertTrue(physicsgroup('_'))
        self.assertTrue(physicsgroup('-'))
        self.assertTrue(physicsgroup('FacOps'))
        self.assertTrue(physicsgroup('Heavy-Ions'))
        self.assertTrue(physicsgroup('Heavy_Ions'))
        self.assertTrue(physicsgroup('Tracker-POG'))
        self.assertTrue(physicsgroup('Trigger'))
        return

    def testGetStringsBetween(self):
        start = '<a n="MachineAttrGLIDEIN_CMSSite0"><s>'
        end = '</s></a>'
        source = '<a n="MachineAttrGLIDEIN_CMSSite0"><s>T2_US_Florida</s></a>'
        result = getStringsBetween(start, end, source)
        self.assertEqual(result, 'T2_US_Florida')

    def testGetIterMatchObjectOnRegex(self):
        logPath = os.path.join(getTestBase(), "WMCore_t/test_empty.log")
        for mo in getIterMatchObjectOnRegexp(logPath, WMEXCEPTION_REGEXP):
            pass

        count = 0
        ecount = 0
        logPath = os.path.join(getTestBase(), "WMCore_t/test_condor.log")
        for mo in getIterMatchObjectOnRegexp(logPath, WMEXCEPTION_REGEXP):
            errMsg = mo.group("WMException")
            if errMsg:
                count += 1
            error = mo.group("ERROR")
            if error:
                ecount += 1
        self.assertEqual(count, 4)
        self.assertEqual(ecount, 1)

        rcount = 0
        scount = 0
        for mo in getIterMatchObjectOnRegexp(logPath, CONDOR_LOG_FILTER_REGEXP):
            if mo.group("Reason"):
                reason = mo.group("Reason")
                rcount += 1
            if mo.group("Site"):
                site = mo.group("Site")
                scount += 1

        self.assertEqual(rcount, 1)
        self.assertEqual(scount, 2)
        self.assertEqual(site, 'T1_US_FNAL')
        self.assertEqual(reason, 'via condor_rm (by user cmst1)')

    def testTaskStepName(self):
        """
        Test some task and step names
        """
        self.assertTrue(taskStepName("T"))
        self.assertTrue(taskStepName("TaskName-Test_Num1"))
        self.assertTrue(taskStepName("this_is-a-step-name"))
        self.assertTrue(taskStepName("Test123-456_789"))
        self.assertTrue(taskStepName("t" * 50))

        # now bad names
        self.assertRaises(AssertionError, taskStepName, "")
        self.assertRaises(AssertionError, taskStepName, "1Task")
        self.assertRaises(AssertionError, taskStepName, "-Task_testName")
        self.assertRaises(AssertionError, taskStepName, "_Task_testName")
        self.assertRaises(AssertionError, taskStepName, "Task@testName")
        self.assertRaises(AssertionError, taskStepName, "t" * 51)

    def testGpuParameters(self):
        """
        Test the 'GPUParams' spec parameter via 'gpuParameters' function
        """
        # unsupported values or incomplete information
        self.assertRaises(AssertionError, gpuParameters, "")
        self.assertRaises(AssertionError, gpuParameters, {})
        self.assertRaises(AssertionError, gpuParameters, json.dumps(""))
        self.assertRaises(AssertionError, gpuParameters, json.dumps([]))
        self.assertRaises(AssertionError, gpuParameters, json.dumps(1))
        self.assertRaises(AssertionError, gpuParameters, json.dumps({}))
        # one mandatory missing
        data = {"GPUMemoryMB": 123, "CUDARuntime": "11.2"}
        self.assertRaises(AssertionError, gpuParameters, json.dumps(data))
        # one unknown argument missing
        data = {"GPUMemoryMB": 123, "CUDARuntime": "11.2", "CUDACapabilities": ["11.2"], "BAD_KEY": 123}
        self.assertRaises(AssertionError, gpuParameters, json.dumps(data))
        # only optional arguments, regardless of their values
        data = {"GPUName": "NVidia 970M GTX", "CUDADriverVersion": "11.2", "CUDARuntimeVersion": 123}
        self.assertRaises(AssertionError, gpuParameters, json.dumps(data))

        # valid and well supported cases
        self.assertTrue(gpuParameters(json.dumps(None)))
        # all mandatory arguments
        data = {"GPUMemoryMB": 123, "CUDARuntime": "11.2", "CUDACapabilities": ["11.2"]}
        self.assertTrue(gpuParameters(json.dumps(data)))
        # all mandatory arguments plus one optional argument
        data = {"GPUMemoryMB": 123, "CUDARuntime": "11.2", "CUDACapabilities": ["11.2"],
                "GPUName": "Nvidia"}
        self.assertTrue(gpuParameters(json.dumps(data)))
        # all mandatory arguments plus two optional argument
        data = {"GPUMemoryMB": 123, "CUDARuntime": "11.2", "CUDACapabilities": ["11.2"],
                "GPUName": "Nvidia", "CUDADriverVersion": "11.2"}
        self.assertTrue(gpuParameters(json.dumps(data)))
        # all mandatory and all optional arguments
        data = {"GPUMemoryMB": 123, "CUDARuntime": "11.2", "CUDACapabilities": ["11.2"],
                "GPUName": "Nvidia", "CUDADriverVersion": "11.2", "CUDARuntimeVersion": "11.2.03"}
        self.assertTrue(gpuParameters(json.dumps(data)))

    def testGpuInternalParameters(self):
        """
        Test the inner key/value pairs of 'GPUParams' spec parameter,
        via '_gpuInternalParameters' private function
        """
        # test "GPUMemoryMB" validation
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": "123"})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": [123]})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": None})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 100.5})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 0})
        # now pass "GPUMemoryMB", but fail "CUDACapabilities"
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 1, "CUDACapabilities": 123})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 1, "CUDACapabilities": "123"})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 1, "CUDACapabilities": None})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 1, "CUDACapabilities": []})
        self.assertRaises(AssertionError, _gpuInternalParameters, {"GPUMemoryMB": 1, "CUDACapabilities": ["1.2", 1.2]})
        # now pass "GPUMemoryMB" and "CUDACapabilities", but fail "CUDARuntime"
        self.assertRaises(AssertionError, _gpuInternalParameters,
                          {"GPUMemoryMB": 1, "CUDACapabilities": ["1.2"], "CUDARuntime": 123})
        self.assertRaises(AssertionError, _gpuInternalParameters,
                          {"GPUMemoryMB": 1, "CUDACapabilities": ["1.2"], "CUDARuntime": [123]})
        self.assertRaises(AssertionError, _gpuInternalParameters,
                          {"GPUMemoryMB": 1, "CUDACapabilities": ["1.2"], "CUDARuntime": None})
        self.assertRaises(AssertionError, _gpuInternalParameters,
                          {"GPUMemoryMB": 1, "CUDACapabilities": ["1.2"], "CUDARuntime": "123"})
        # now test valid values for all 3 mandatory arguments: GPUMemoryMB, CUDACapabilities, CUDARuntime
        self.assertTrue(_gpuInternalParameters({"GPUMemoryMB": 1, "CUDACapabilities": ["1.2"], "CUDARuntime": "2.3"}))
        self.assertTrue(_gpuInternalParameters({"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2", "2.3", "2.3.4"],
                                                "CUDARuntime": "2.3"}))
        self.assertTrue(_gpuInternalParameters({"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2", "2.3", "2.3.4"],
                                                "CUDARuntime": "2.3.4"}))

        goodParams = {"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2"], "CUDARuntime": "2.3.4"}
        # test "GPUName" validation
        goodParams.update({"GPUName": 123})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"GPUName": None})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"GPUName": []})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"GPUName": 123})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"GPUName": 101*"a"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        # now pass "GPUName", and test "CUDADriverVersion" parameter
        goodParams = {"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2"], "CUDARuntime": "2.3.4",
                      "GPUName": "123"}
        goodParams.update({"CUDADriverVersion": 123})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDADriverVersion": None})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDADriverVersion": []})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDADriverVersion": "123"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDADriverVersion": "1.2.3.4"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDADriverVersion": 101*"1"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        # now pass "GPUName" and "CUDADriverVersion", and test "CUDARuntimeVersion" parameter
        goodParams = {"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2"], "CUDARuntime": "2.3.4",
                      "GPUName": "123", "CUDADriverVersion": "1.2.3"}
        goodParams.update({"CUDARuntimeVersion": 123})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDARuntimeVersion": None})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDARuntimeVersion": []})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDARuntimeVersion": "123"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDARuntimeVersion": "1.2.3.4"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)
        goodParams.update({"CUDARuntimeVersion": 101*"1"})
        self.assertRaises(AssertionError, _gpuInternalParameters, goodParams)

        # finally, test good values for mandatory + optional parameters
        # now test valid values for all 3 mandatory arguments: GPUMemoryMB, CUDACapabilities, CUDARuntime
        self.assertTrue(_gpuInternalParameters({"GPUMemoryMB": 1, "CUDACapabilities": ["1.2"],
                                                "CUDARuntime": "2.3", "GPUName": "123",
                                                "CUDADriverVersion": "1.2.3", "CUDARuntimeVersion": "1.2.03"}))
        self.assertTrue(_gpuInternalParameters({"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2", "2.3", "2.3.4"],
                                                "CUDARuntime": "2.3"}))
        self.assertTrue(_gpuInternalParameters({"GPUMemoryMB": 12345, "CUDACapabilities": ["1.2", "2.3", "2.3.4"],
                                                "CUDARuntime": "2.3.4"}))

    def testSubRequestType(self):
        """
        Test some task and step names
        """
        # valid values
        for cand in ["MC", "ReDigi", "Pilot", "RelVal", "HIRelVal", "ReReco", ""]:
            self.assertTrue(subRequestType(cand))

        # now invalid ones
        self.assertRaises(AssertionError, subRequestType, None)
        self.assertRaises(AssertionError, subRequestType, "test")
        self.assertRaises(AssertionError, subRequestType, ["blah"])
        self.assertRaises(AssertionError, subRequestType, 1)


if __name__ == "__main__":
    unittest.main()
