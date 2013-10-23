#!/usr/bin/python
"""
_Lexicon_t_

General test of Lexicon

"""

import logging
import unittest

from WMCore.Lexicon import *

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
        for test_u in [u1, u2, u3, u4, u5, u6, u7,u8,u9, u10,u11]:
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
        ds10 ='/*'
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

    def testGoodProcdataset(self):
        # Check that valid procdataset work
        assert procdataset('CMSSW_4_4_0_pre7_g494p02-START44_V2_special_110808-v1'), 'valid procdataset not validated'
        assert procdataset('Summer11-PU_S4_START42_V11-v1'), 'valid procdataset not validated'
        assert procdataset('CMSSW_3_0_0_pre3_IDEAL_30X-v1'), 'valid procdataset not validated'
        assert procdataset('CMSSW_3_0_0_pre3_IDEAL_30X-my_filter-my_string-v1'), 'valid procdataset not validated'

    def testBadProcataset(self):
        # Check that invalid Procataset raise an exception
        self.assertRaises(AssertionError, procdataset, 'Drop Table')
        self.assertRaises(AssertionError, procdataset, 'Alter Table')
        self.assertRaises(AssertionError, procdataset, 'CMSSW_3_0_0_pre3_IDEAL_30X_v1')

    def testGoodPrimdataset(self):
        # Check that valid Primdataset work
        assert primdataset('qqH125-2tau'), 'valid primdataset not validated'
        assert primdataset('RelVal124QCD_pt120_170'), 'valid primdataset not validated'
        assert primdataset('RelVal160pre14SingleMuMinusPt10'), 'valid primdataset not validated'

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

    def testAcqName(self):
        """
        _testAcqName_

        Test the acquisitionEra name verification
        """
        self.assertRaises(AssertionError, acqname, '*Nothing')
        self.assertRaises(AssertionError, acqname, '1version')
        self.assertTrue(acqname('a22'))
        self.assertTrue(acqname('aForReals'))
        return

    def testBadPrimdataset(self):
        # Check that invalid Primdataset raise an exception
        self.assertRaises(AssertionError, primdataset, 'Drop Table')
        self.assertRaises(AssertionError, primdataset, 'Alter Table')


    def testGoodSearchstr(self):
        # Check that valid searchstr work
        assert searchstr('/store/mc/Fall08/BBJets250to500-madgraph/*'), 'valid search string not validated'
        assert searchstr('/QCD_EMenriched_Pt30to80/Summer08_IDEAL_V11_redigi_v2/GEN-SIM-RAW#cfc0a501-e845-4576-af8a-f811165d82d9'), 'valid search string not validated'
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

    def testPartialCMSName(self):
        # Check that partial names work
        for i in ['T%', 'T2','T2_', 'T2_UK', 'T2_UK_', 'T2_UK_SGrid', 'T2_UK_SGrid_']:
            assert cmsname(i), 'partial CMS name (%s) not validated' % i

    def testBadCMSName(self):
        # Check that invalid names raise an exception
        self.assertRaises(AssertionError, cmsname, 'T5_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2-UK-SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2_UK_SGrid_Bris-tol')
        self.assertRaises(AssertionError, cmsname, 'D2_UK_SGrid_Bristol')
        self.assertRaises(AssertionError, cmsname, 'T2asjkjhadshjkdashjkasdkjhdas')
        #self.assertRaises(AssertionError, cmsname, 'T2_UK')

    def testGoodIdentifier(self):
        for ok in ['__wil.1.am__', '.']:
            assert identifier(ok)

    def testBadIdentifier(self):
        for notok in ['ke$ha', '<begin>']:
            self.assertRaises(AssertionError, identifier, notok)

    def testGoodDataset(self):
        assert dataset('/a/b/c')
        assert dataset('/m000n/RIII-ver/wider_than_1.0_miles')

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
                   'http://iam.anexternal.host:5184/somedb/some_doc/some_doc' ]:
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
        lfnA = '/store/himc/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)        
        lfnA = '/store/backfill/1/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/t0temp/data/Run2010A/Cosmics/RECO/v4/000/143/316/0000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/backfill/1/Run2012B/Cosmics/RAW-RECO/PromptSkim-v1/000/194/912/00000/F65F4AFE-14AC-DF11-B3BE-00215E21F32E.root'
        lfn(lfnA)
        lfnA = '/store/results/qcd/QCD_Pt80/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/QCD_Pt80/USER/StoreResults-Summer09-MC_31X_V3_7TeV-Jet30U-JetAODSkim-0a98be42532eba1f0545cc9b086ec3c3/0000/C44630AC-C0C7-DE11-AD4E-0019B9CAC0F8.root'
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

        lfnA = '/store/temp/user/ewv/Higgs-123/PrivateSample/USER/v1/a_X-2.root'
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
        port  = "9999"
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
        #Good http(s) urls
        for url in ['https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                    'https://mydbs.mydomain.de:8443',
                    'http://localhost',
                    'http://192.168.1.1',
                    'https://192.168.1.1:443',
                    'http://[2001:0db8:85a3:08d3:1319:8a2e:0370:7344]/',
                    'http://2001:0db8:85a3:08d3:1319:8a2e:0370:7344/',
                    'http://[2001:0db8:85a3:08d3:1319:8a2e:0370:7344]:8443/']:
            validateUrl(url)

        #Bad http(s) urls
        for url in ['ftp://dangerous.download.this',
                    'https:/notvalid.url',
                    'http://-NiceTry',
                    'https://NiceTry; DROP Table;--',
                    'http://123123104122',
                    'http://.www.google.com',
                    'http://[2001:0db8:85a3:08d3:1319:8a2z:0370:7344]/',]:
            self.assertRaises(AssertionError, validateUrl, url)

if __name__ == "__main__":
    unittest.main()
