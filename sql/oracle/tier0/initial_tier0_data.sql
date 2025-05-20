-- Initial data for Tier0 schema
-- This file contains the initial data that needs to be inserted into the Tier0 tables

-- Run States
INSERT INTO run_status (id, name) VALUES (1, 'Active')
/

INSERT INTO run_status (id, name) VALUES (2, 'CloseOutRepack')
/

INSERT INTO run_status (id, name) VALUES (3, 'CloseOutRepackMerge')
/

INSERT INTO run_status (id, name) VALUES (4, 'CloseOutPromptReco')
/

INSERT INTO run_status (id, name) VALUES (5, 'CloseOutRecoMerge')
/

INSERT INTO run_status (id, name) VALUES (6, 'CloseOutAlcaSkim')
/

INSERT INTO run_status (id, name) VALUES (7, 'CloseOutAlcaSkimMerge')
/

INSERT INTO run_status (id, name) VALUES (8, 'CloseOutExport')
/

INSERT INTO run_status (id, name) VALUES (9, 'CloseOutT1Skimming')
/

INSERT INTO run_status (id, name) VALUES (10, 'Complete')
/

-- Processing Styles
INSERT INTO processing_style (id, name) VALUES (1, 'Bulk')
/

INSERT INTO processing_style (id, name) VALUES (2, 'Express')
/

INSERT INTO processing_style (id, name) VALUES (3, 'Register')
/

INSERT INTO processing_style (id, name) VALUES (4, 'Convert')
/

INSERT INTO processing_style (id, name) VALUES (5, 'RegisterAndConvert')
/

INSERT INTO processing_style (id, name) VALUES (6, 'Ignore')
/

-- Event Scenarios
INSERT INTO event_scenario (id, name) VALUES (1, 'pp')
/

INSERT INTO event_scenario (id, name) VALUES (2, 'cosmics')
/

INSERT INTO event_scenario (id, name) VALUES (3, 'hcalnzs')
/

INSERT INTO event_scenario (id, name) VALUES (4, 'HeavyIons')
/

INSERT INTO event_scenario (id, name) VALUES (5, 'AlCaTestEnable')
/

INSERT INTO event_scenario (id, name) VALUES (6, 'AlCaP0')
/

INSERT INTO event_scenario (id, name) VALUES (7, 'AlCaPhiSymEcal')
/

INSERT INTO event_scenario (id, name) VALUES (8, 'AlCaLumiPixels')
/

INSERT INTO event_scenario (id, name) VALUES (9, 'DataScouting')
/

INSERT INTO event_scenario (id, name) VALUES (10, 'ppRun2')
/

INSERT INTO event_scenario (id, name) VALUES (11, 'cosmicsRun2')
/

INSERT INTO event_scenario (id, name) VALUES (12, 'hcalnzsRun2')
/

INSERT INTO event_scenario (id, name) VALUES (13, 'ppRun2B0T')
/

INSERT INTO event_scenario (id, name) VALUES (14, 'AlCa')
/

INSERT INTO event_scenario (id, name) VALUES (15, 'ppRun2at50ns')
/

INSERT INTO event_scenario (id, name) VALUES (16, 'HeavyIonsRun2')
/

INSERT INTO event_scenario (id, name) VALUES (17, 'ppEra_Run2_25ns')
/

INSERT INTO event_scenario (id, name) VALUES (18, 'cosmicsEra_Run2_25ns')
/

INSERT INTO event_scenario (id, name) VALUES (19, 'hcalnzsEra_Run2_25ns')
/

INSERT INTO event_scenario (id, name) VALUES (20, 'ppEra_Run2_2016')
/

INSERT INTO event_scenario (id, name) VALUES (21, 'cosmicsEra_Run2_2016')
/

INSERT INTO event_scenario (id, name) VALUES (22, 'hcalnzsEra_Run2_2016')
/

INSERT INTO event_scenario (id, name) VALUES (23, 'ppEra_Run2_2016_trackingLowPU')
/

INSERT INTO event_scenario (id, name) VALUES (24, 'ppEra_Run2_2016_pA')
/

INSERT INTO event_scenario (id, name) VALUES (25, 'ppEra_Run2_2017')
/

INSERT INTO event_scenario (id, name) VALUES (26, 'cosmicsEra_Run2_2017')
/

INSERT INTO event_scenario (id, name) VALUES (27, 'hcalnzsEra_Run2_2017')
/

INSERT INTO event_scenario (id, name) VALUES (28, 'ppEra_Run2_2017_trackingOnly')
/

INSERT INTO event_scenario (id, name) VALUES (29, 'ppEra_Run2_2017_ppRef')
/

INSERT INTO event_scenario (id, name) VALUES (30, 'cosmicsEra_Run2_2018')
/

INSERT INTO event_scenario (id, name) VALUES (31, 'hcalnzsEra_Run2_2018')
/

INSERT INTO event_scenario (id, name) VALUES (32, 'ppEra_Run2_2018')
/

INSERT INTO event_scenario (id, name) VALUES (33, 'trackingOnlyEra_Run2_2018')
/

INSERT INTO event_scenario (id, name) VALUES (34, 'ppEra_Run2_2018_pp_on_AA')
/

INSERT INTO event_scenario (id, name) VALUES (35, 'hcalnzsEra_Run2_2018_pp_on_AA')
/

INSERT INTO event_scenario (id, name) VALUES (36, 'trackingOnlyEra_Run2_2018_pp_on_AA')
/

INSERT INTO event_scenario (id, name) VALUES (37, 'ppEra_Run3')
/

INSERT INTO event_scenario (id, name) VALUES (38, 'cosmicsEra_Run3')
/

INSERT INTO event_scenario (id, name) VALUES (39, 'hcalnzsEra_Run3')
/

INSERT INTO event_scenario (id, name) VALUES (40, 'trackingOnlyEra_Run3')
/

INSERT INTO event_scenario (id, name) VALUES (41, 'AlCaLumiPixels_Run3')
/

INSERT INTO event_scenario (id, name) VALUES (42, 'AlCaPhiSymEcal_Nano')
/

INSERT INTO event_scenario (id, name) VALUES (43, 'AlCaPPS_Run3')
/

INSERT INTO event_scenario (id, name) VALUES (44, 'ppEra_Run3_pp_on_PbPb')
/

INSERT INTO event_scenario (id, name) VALUES (45, 'trackingOnlyEra_Run3_pp_on_PbPb')
/

INSERT INTO event_scenario (id, name) VALUES (46, 'hcalnzsEra_Run3_pp_on_PbPb')
/

INSERT INTO event_scenario (id, name) VALUES (47, 'ppEra_Run3_pp_on_PbPb_approxSiStripClusters')
/

INSERT INTO event_scenario (id, name) VALUES (48, 'ppEra_Run3_2023')
/

INSERT INTO event_scenario (id, name) VALUES (49, 'ppEra_Run3_pp_on_PbPb_2023')
/

INSERT INTO event_scenario (id, name) VALUES (50, 'ppEra_Run3_pp_on_PbPb_approxSiStripClusters_2023')
/

INSERT INTO event_scenario (id, name) VALUES (51, 'ppEra_Run3_2023_repacked')
/

INSERT INTO event_scenario (id, name) VALUES (52, 'hltScoutingEra_Run3_2024')
/

INSERT INTO event_scenario (id, name) VALUES (53, 'ppEra_Run3_2024_UPC')
/

INSERT INTO event_scenario (id, name) VALUES (54, 'ppEra_Run3_pp_on_PbPb_2024')
/

INSERT INTO event_scenario (id, name) VALUES (55, 'ppEra_Run3_pp_on_PbPb_approxSiStripClusters_2024')
/

INSERT INTO event_scenario (id, name) VALUES (56, 'ppEra_Run3_2024_ppRef')
/

INSERT INTO event_scenario (id, name) VALUES (57, 'ppEra_Run3_2024')
/

INSERT INTO event_scenario (id, name) VALUES (58, 'ppEra_Run3_2025')
/ 