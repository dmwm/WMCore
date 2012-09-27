import FWCore.ParameterSet.Config as cms

process = cms.Process("SIM")

process.source = cms.Source("EmptySource")
process.MEtoEDMConverter = cms.EDProducer("MEtoEDMConverter",
    deleteAfterCopy = cms.untracked.bool(True),
    Verbosity = cms.untracked.int32(0),
    Frequency = cms.untracked.int32(50),
    Name = cms.untracked.string('MEtoEDMConverter'),
    MEPathToSave = cms.untracked.string('')
)


process.VtxSmeared = cms.EDProducer("BetafuncEvtVtxGenerator",
    Phi = cms.double(0.0),
    Y0 = cms.double(0.3929),
    BetaStar = cms.double(150.0),
    Emittance = cms.double(6.7e-08),
    SigmaZ = cms.double(5.22),
    TimeOffset = cms.double(0.0),
    Alpha = cms.double(0.0),
    X0 = cms.double(0.244),
    Z0 = cms.double(0.4145),
    src = cms.InputTag("generator")
)


process.ak3HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('AntiKt'),
    rParam = cms.double(0.3)
)


process.ak4HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('AntiKt'),
    rParam = cms.double(0.4)
)


process.ak5GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('AntiKt'),
    rParam = cms.double(0.5)
)


process.ak5GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('AntiKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ak5GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('AntiKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ak5HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('AntiKt'),
    rParam = cms.double(0.5)
)


process.ak7GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('AntiKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJets"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ak7GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('AntiKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ak7GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('AntiKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ak7HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('AntiKt'),
    rParam = cms.double(0.7)
)


process.ca4GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('CambridgeAachen'),
    rParam = cms.double(0.4)
)


process.ca4GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('CambridgeAachen'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.4),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ca4GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('CambridgeAachen'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.4),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ca6GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('CambridgeAachen'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.6),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJets"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ca6GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('CambridgeAachen'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.6),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.ca6GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('CambridgeAachen'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.6),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.g4SimHits = cms.EDProducer("OscarProducer",
    SteppingAction = cms.PSet(
        MaxTrackTime = cms.double(500.0),
        MaxTrackTimes = cms.vdouble(2000.0, 0.0, 0.0),
        MaxTimeNames = cms.vstring('ZDCRegion',
            'QuadRegion',
            'InterimRegion'),
        EkinThresholds = cms.vdouble(),
        Verbosity = cms.untracked.int32(0),
        KillBeamPipe = cms.bool(True),
        EkinNames = cms.vstring(),
        EkinParticles = cms.vstring(),
        CriticalDensity = cms.double(1e-15),
        CriticalEnergyForVacuum = cms.double(2.0)
    ),
    G4StackManagerVerbosity = cms.untracked.int32(0),
    OverrideUserStackingAction = cms.bool(True),
    G4TrackingManagerVerbosity = cms.untracked.int32(0),
    EventAction = cms.PSet(
        debug = cms.untracked.bool(False),
        StopFile = cms.string('StopRun'),
        CollapsePrimaryVertices = cms.bool(False)
    ),
    CaloTrkProcessing = cms.PSet(
        EminTrack = cms.double(0.01),
        PutHistory = cms.bool(False),
        TestBeam = cms.bool(False)
    ),
    HFGflash = cms.PSet(
        BField = cms.untracked.double(3.8),
        WatcherOn = cms.untracked.bool(True),
        FillHisto = cms.untracked.bool(True)
    ),
    StoreRndmSeeds = cms.bool(False),
    PltSD = cms.PSet(
        EnergyThresholdForHistoryInGeV = cms.double(0.05),
        EnergyThresholdForPersistencyInGeV = cms.double(0.2)
    ),
    StorePhysicsTables = cms.bool(False),
    UseMagneticField = cms.bool(True),
    CastorShowerLibrary = cms.PSet(
        BranchEvt = cms.untracked.string('hadShowerLibInfo.'),
        Verbosity = cms.untracked.bool(False),
        BranchEM = cms.untracked.string('emParticles.'),
        BranchHAD = cms.untracked.string('hadParticles.'),
        FileName = cms.FileInPath('SimG4CMS/Forward/data/castorShowerLibrary_QFBE.root')
    ),
    Generator = cms.PSet(
        EtaCutForHector = cms.double(8.2),
        MaxPhiCut = cms.double(3.14159265359),
        RDecLenCut = cms.double(2.9),
        MinPhiCut = cms.double(-3.14159265359),
        Verbosity = cms.untracked.int32(0),
        ApplyPCuts = cms.bool(True),
        MinEtaCut = cms.double(-5.5),
        MinPCut = cms.double(0.04),
        ApplyEtaCuts = cms.bool(True),
        MaxEtaCut = cms.double(5.5),
        MaxPCut = cms.double(99999.0),
        HepMCProductLabel = cms.string('generator'),
        ApplyPhiCuts = cms.bool(False)
    ),
    HcalTB02SD = cms.PSet(
        UseBirkLaw = cms.untracked.bool(False),
        BirkC1 = cms.untracked.double(0.013),
        BirkC3 = cms.untracked.double(1.75),
        BirkC2 = cms.untracked.double(0.0568)
    ),
    MagneticField = cms.PSet(
        UseLocalMagFieldManager = cms.bool(False),
        Verbosity = cms.untracked.bool(False),
        ConfGlobalMFM = cms.PSet(
            Volume = cms.string('OCMS'),
            OCMS = cms.PSet(
                Stepper = cms.string('G4ClassicalRK4'),
                Type = cms.string('CMSIMField'),
                G4ClassicalRK4 = cms.PSet(
                    MaximumEpsilonStep = cms.untracked.double(0.01),
                    DeltaIntersection = cms.double(0.0001),
                    MaximumLoopCounts = cms.untracked.double(1000.0),
                    DeltaChord = cms.double(0.001),
                    DeltaIntersectionAndOneStep = cms.untracked.double(-1.0),
                    MinStep = cms.double(0.1),
                    DeltaOneStep = cms.double(0.001),
                    MinimumEpsilonStep = cms.untracked.double(1e-05)
                )
            )
        ),
        delta = cms.double(1.0)
    ),
    ECalSD = cms.PSet(
        IgnoreTrackID = cms.bool(False),
        XtalMat = cms.untracked.string('E_PbWO4'),
        SlopeLightYield = cms.double(0.02),
        StoreRadLength = cms.untracked.bool(False),
        BirkL3Parametrization = cms.bool(True),
        BirkC2 = cms.double(0.0),
        NullNumbering = cms.untracked.bool(False),
        BirkCut = cms.double(0.1),
        BirkC1 = cms.double(0.03333),
        BirkC3 = cms.double(1.0),
        StoreSecondary = cms.bool(False),
        TimeSliceUnit = cms.int32(1),
        TestBeam = cms.untracked.bool(False),
        UseBirkLaw = cms.bool(True),
        BirkSlope = cms.double(0.253694)
    ),
    CheckOverlap = cms.untracked.bool(False),
    NonBeamEvent = cms.bool(False),
    BscSD = cms.PSet(
        Verbosity = cms.untracked.int32(0)
    ),
    TotemSD = cms.PSet(
        Verbosity = cms.untracked.int32(0)
    ),
    HCalSD = cms.PSet(
        IgnoreTrackID = cms.bool(False),
        UseHF = cms.untracked.bool(True),
        TestNumberingScheme = cms.bool(False),
        UsePMTHits = cms.bool(False),
        UseParametrize = cms.bool(True),
        ForTBH2 = cms.untracked.bool(False),
        UseFibreBundleHits = cms.bool(False),
        WtFile = cms.untracked.string('None'),
        BirkC2 = cms.double(0.142),
        EminHitHF = cms.double(0.0),
        EminHitHE = cms.double(0.0),
        UseLayerWt = cms.untracked.bool(False),
        BetaThreshold = cms.double(0.7),
        UseShowerLibrary = cms.bool(False),
        BirkC3 = cms.double(1.75),
        EminHitHB = cms.double(0.0),
        TimeSliceUnit = cms.int32(1),
        EminHitHO = cms.double(0.0),
        BirkC1 = cms.double(0.0052),
        UseBirkLaw = cms.bool(True)
    ),
    HFShowerLibrary = cms.PSet(
        BranchPost = cms.untracked.string('_R.obj'),
        BranchEvt = cms.untracked.string('HFShowerLibraryEventInfos_hfshowerlib_HFShowerLibraryEventInfo'),
        TreeHadID = cms.string('hadParticles'),
        Verbosity = cms.untracked.bool(False),
        BackProbability = cms.double(0.2),
        FileName = cms.FileInPath('SimG4CMS/Calo/data/hfshowerlibrary_lhep_140_edm.root'),
        TreeEMID = cms.string('emParticles'),
        BranchPre = cms.untracked.string('HFShowerPhotons_hfshowerlib_')
    ),
    CaloSD = cms.PSet(
        NeutronThreshold = cms.double(30.0),
        ProtonThreshold = cms.double(30.0),
        IonThreshold = cms.double(30.0),
        EminHits = cms.vdouble(0.015, 0.01, 0.0, 0.0, 0.0),
        UseResponseTables = cms.vint32(0, 0, 0, 0, 0),
        SuppressHeavy = cms.bool(False),
        DetailedTiming = cms.untracked.bool(False),
        Verbosity = cms.untracked.int32(0),
        CheckHits = cms.untracked.int32(25),
        TmaxHits = cms.vdouble(500.0, 500.0, 500.0, 500.0, 2000.0),
        BeamPosition = cms.double(0.0),
        TmaxHit = cms.double(1000.0),
        CorrectTOFBeam = cms.bool(False),
        UseMap = cms.untracked.bool(False),
        EminTrack = cms.double(1.0),
        EminHitsDepth = cms.vdouble(0.0, 0.0, 0.0, 0.0, 0.0),
        HCNames = cms.vstring('EcalHitsEB',
            'EcalHitsEE',
            'EcalHitsES',
            'HcalHits',
            'ZDCHITS')
    ),
    TrackerSD = cms.PSet(
        ZeroEnergyLoss = cms.bool(False),
        PrintHits = cms.bool(False),
        ElectronicSigmaInNanoSeconds = cms.double(12.06),
        NeverAccumulate = cms.bool(False),
        EnergyThresholdForPersistencyInGeV = cms.double(0.2),
        EnergyThresholdForHistoryInGeV = cms.double(0.05)
    ),
    HcalTB06BeamSD = cms.PSet(
        UseBirkLaw = cms.bool(False),
        BirkC1 = cms.double(0.013),
        BirkC3 = cms.double(1.75),
        BirkC2 = cms.double(0.0568)
    ),
    Watchers = cms.VPSet(),
    ZdcShowerLibrary = cms.PSet(
        Verbosity = cms.untracked.int32(0)
    ),
    EcalTBH4BeamSD = cms.PSet(
        UseBirkLaw = cms.bool(False),
        BirkC1 = cms.double(0.013),
        BirkC3 = cms.double(1.75),
        BirkC2 = cms.double(0.0568)
    ),
    RunAction = cms.PSet(
        StopFile = cms.string('StopRun')
    ),
    RestorePhysicsTables = cms.bool(False),
    ZdcSD = cms.PSet(
        UseShowerHits = cms.bool(False),
        Verbosity = cms.int32(0),
        ZdcHitEnergyCut = cms.double(10.0),
        UseShowerLibrary = cms.bool(True),
        FiberDirection = cms.double(45.0)
    ),
    HFShowerConicalBundle = cms.PSet(
        RefIndex = cms.double(1.459),
        Lambda1 = cms.double(280.0),
        Gain = cms.double(0.33),
        Aperture = cms.double(0.33),
        CheckSurvive = cms.bool(False),
        FactorBundle = cms.double(1.0),
        Lambda2 = cms.double(700.0),
        ApertureTrapped = cms.double(0.22)
    ),
    FP420SD = cms.PSet(
        Verbosity = cms.untracked.int32(2)
    ),
    HFShowerStraightBundle = cms.PSet(
        RefIndex = cms.double(1.459),
        Lambda1 = cms.double(280.0),
        Gain = cms.double(0.33),
        Aperture = cms.double(0.33),
        CheckSurvive = cms.bool(False),
        FactorBundle = cms.double(1.0),
        Lambda2 = cms.double(700.0),
        ApertureTrapped = cms.double(0.22)
    ),
    RestoreRndmSeeds = cms.bool(False),
    Physics = cms.PSet(
        FlagCHIPS = cms.untracked.bool(False),
        MonopoleTransport = cms.untracked.bool(True),
        HadPhysics = cms.untracked.bool(True),
        SRType = cms.bool(True),
        TrackingCut = cms.bool(True),
        FlagGlauber = cms.untracked.bool(False),
        FlagHP = cms.untracked.bool(False),
        FlagFTF = cms.untracked.bool(False),
        Region = cms.string(' '),
        FlagBERT = cms.untracked.bool(False),
        EMPhysics = cms.untracked.bool(True),
        Verbosity = cms.untracked.int32(0),
        MonopoleMultiScatter = cms.untracked.bool(False),
        MonopoleCharge = cms.untracked.int32(1),
        DefaultCutValue = cms.double(1.0),
        GFlash = cms.PSet(
            GflashHistogram = cms.bool(False),
            GflashEMShowerModel = cms.bool(False),
            GflashHadronPhysics = cms.string('QGSP_BERT_EMV'),
            GflashHadronShowerModel = cms.bool(False)
        ),
        CutsPerRegion = cms.bool(True),
        G4BremsstrahlungThreshold = cms.double(0.5),
        type = cms.string('SimG4Core/Physics/QGSP_FTFP_BERT_EML'),
        MonopoleDeltaRay = cms.untracked.bool(True),
        DummyEMPhysics = cms.bool(False)
    ),
    G4Commands = cms.vstring(),
    StackingAction = cms.PSet(
        NeutronThreshold = cms.double(30.0),
        ProtonThreshold = cms.double(30.0),
        IonThreshold = cms.double(30.0),
        MaxTrackTime = cms.double(500.0),
        MaxTrackTimes = cms.vdouble(2000.0, 0.0, 0.0),
        MaxTimeNames = cms.vstring('ZDCRegion',
            'QuadRegion',
            'InterimRegion'),
        SaveFirstLevelSecondary = cms.untracked.bool(False),
        KillDeltaRay = cms.bool(False),
        SavePrimaryDecayProductsAndConversionsInTracker = cms.untracked.bool(True),
        SavePrimaryDecayProductsAndConversionsInCalo = cms.untracked.bool(False),
        SavePrimaryDecayProductsAndConversionsInMuon = cms.untracked.bool(False),
        TrackNeutrino = cms.bool(False),
        KillHeavy = cms.bool(False)
    ),
    CastorSD = cms.PSet(
        useShowerLibrary = cms.bool(True),
        Verbosity = cms.untracked.int32(0),
        minEnergyInGeVforUsingSLibrary = cms.double(1.0)
    ),
    HFShower = cms.PSet(
        ApplyFiducialCut = cms.bool(False),
        ProbMax = cms.double(1.0),
        EminLibrary = cms.double(0.0),
        LambdaMean = cms.double(350.0),
        RefIndex = cms.double(1.459),
        CFibre = cms.double(0.5),
        OnlyLong = cms.bool(True),
        UseShowerLibrary = cms.bool(False),
        Gain = cms.double(0.33),
        ParametrizeLast = cms.untracked.bool(False),
        Aperture = cms.double(0.33),
        PEPerGeV = cms.double(0.31),
        CheckSurvive = cms.bool(False),
        TrackEM = cms.bool(False),
        UseHFGflash = cms.bool(True),
        Lambda1 = cms.double(280.0),
        Lambda2 = cms.double(700.0),
        ApertureTrapped = cms.double(0.22)
    ),
    MuonSD = cms.PSet(
        PrintHits = cms.bool(False),
        EnergyThresholdForPersistency = cms.double(1.0),
        AllMuonsPersistent = cms.bool(True)
    ),
    G4EventManagerVerbosity = cms.untracked.int32(0),
    CaloResponse = cms.PSet(
        ResponseScale = cms.double(1.0),
        UseResponseTable = cms.bool(True),
        ResponseFile = cms.FileInPath('SimG4CMS/Calo/data/responsTBpim50.dat')
    ),
    PhysicsTablesDirectory = cms.string('PhysicsTables'),
    HFShowerPMT = cms.PSet(
        RefIndex = cms.double(1.52),
        Gain = cms.double(0.33),
        Aperture = cms.double(0.99),
        CheckSurvive = cms.bool(False),
        PEPerGeVPMT = cms.double(1.0),
        Lambda1 = cms.double(280.0),
        Lambda2 = cms.double(700.0),
        ApertureTrapped = cms.double(0.22)
    ),
    TrackingAction = cms.PSet(
        DetailedTiming = cms.untracked.bool(False)
    )
)


process.genCandidatesForMET = cms.EDProducer("InputGenJetsParticleSelector",
    src = cms.InputTag("genParticles"),
    ignoreParticleIDs = cms.vuint32(1000022, 1000012, 1000014, 1000016, 2000012,
        2000014, 2000016, 1000039, 5100039, 4000012,
        4000014, 4000016, 9900012, 9900014, 9900016,
        39, 12, 13, 14, 16),
    partonicFinalState = cms.bool(False),
    excludeResonances = cms.bool(False),
    excludeFromResonancePids = cms.vuint32(),
    tausAsJets = cms.bool(False)
)


process.genFilterEfficiencyProducer = cms.EDProducer("GenFilterEfficiencyProducer",
    filterPath = cms.string('generation_step')
)


process.genMetCalo = cms.EDProducer("METProducer",
    src = cms.InputTag("genCandidatesForMET"),
    METType = cms.string('GenMET'),
    usePt = cms.untracked.bool(True),
    alias = cms.string('GenMETCalo'),
    onlyFiducialParticles = cms.bool(True),
    globalThreshold = cms.double(0.0),
    InputType = cms.string('CandidateCollection')
)


process.genMetCaloAndNonPrompt = cms.EDProducer("METProducer",
    src = cms.InputTag("genParticlesForJets"),
    METType = cms.string('GenMET'),
    alias = cms.string('GenMETCaloAndNonPrompt'),
    onlyFiducialParticles = cms.bool(True),
    globalThreshold = cms.double(0.0),
    InputType = cms.string('CandidateCollection')
)


process.genMetIC5GenJets = cms.EDProducer("METProducer",
    src = cms.InputTag("iterativeCone5GenJets"),
    METType = cms.string('MET'),
    alias = cms.string('GenMETIC5'),
    noHF = cms.bool(False),
    globalThreshold = cms.double(0.0),
    InputType = cms.string('CandidateCollection')
)


process.genMetTrue = cms.EDProducer("METProducer",
    src = cms.InputTag("genParticlesForMETAllVisible"),
    METType = cms.string('GenMET'),
    usePt = cms.untracked.bool(True),
    alias = cms.string('GenMETAllVisible'),
    onlyFiducialParticles = cms.bool(False),
    globalThreshold = cms.double(0.0),
    InputType = cms.string('CandidateCollection')
)


process.genParticleCandidates = cms.EDProducer("FastGenParticleCandidateProducer",
    saveBarCodes = cms.untracked.bool(False),
    src = cms.InputTag("generator"),
    abortOnUnknownPDGCode = cms.untracked.bool(False)
)


process.genParticles = cms.EDProducer("GenParticleProducer",
    saveBarCodes = cms.untracked.bool(True),
    src = cms.InputTag("generator"),
    abortOnUnknownPDGCode = cms.untracked.bool(False)
)


process.genParticlesForJets = cms.EDProducer("InputGenJetsParticleSelector",
    src = cms.InputTag("genParticles"),
    ignoreParticleIDs = cms.vuint32(1000022, 1000012, 1000014, 1000016, 2000012,
        2000014, 2000016, 1000039, 5100039, 4000012,
        4000014, 4000016, 9900012, 9900014, 9900016,
        39),
    partonicFinalState = cms.bool(False),
    excludeResonances = cms.bool(True),
    excludeFromResonancePids = cms.vuint32(12, 13, 14, 16),
    tausAsJets = cms.bool(False)
)


process.genParticlesForJetsNoMuNoNu = cms.EDProducer("InputGenJetsParticleSelector",
    src = cms.InputTag("genParticles"),
    ignoreParticleIDs = cms.vuint32(1000022, 1000012, 1000014, 1000016, 2000012,
        2000014, 2000016, 1000039, 5100039, 4000012,
        4000014, 4000016, 9900012, 9900014, 9900016,
        39, 12, 13, 14, 16),
    partonicFinalState = cms.bool(False),
    excludeResonances = cms.bool(True),
    excludeFromResonancePids = cms.vuint32(12, 13, 14, 16),
    tausAsJets = cms.bool(False)
)


process.genParticlesForJetsNoNu = cms.EDProducer("InputGenJetsParticleSelector",
    src = cms.InputTag("genParticles"),
    ignoreParticleIDs = cms.vuint32(1000022, 1000012, 1000014, 1000016, 2000012,
        2000014, 2000016, 1000039, 5100039, 4000012,
        4000014, 4000016, 9900012, 9900014, 9900016,
        39, 12, 14, 16),
    partonicFinalState = cms.bool(False),
    excludeResonances = cms.bool(True),
    excludeFromResonancePids = cms.vuint32(12, 13, 14, 16),
    tausAsJets = cms.bool(False)
)


process.genParticlesForMETAllVisible = cms.EDProducer("InputGenJetsParticleSelector",
    src = cms.InputTag("genParticles"),
    ignoreParticleIDs = cms.vuint32(1000022, 1000012, 1000014, 1000016, 2000012,
        2000014, 2000016, 1000039, 5100039, 4000012,
        4000014, 4000016, 9900012, 9900014, 9900016,
        39, 12, 14, 16),
    partonicFinalState = cms.bool(False),
    excludeResonances = cms.bool(False),
    excludeFromResonancePids = cms.vuint32(),
    tausAsJets = cms.bool(False)
)


process.gk5GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('GeneralizedKt'),
    rParam = cms.double(0.5)
)


process.gk5GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('GeneralizedKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.gk5GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('GeneralizedKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.gk7GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('GeneralizedKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJets"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.gk7GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('GeneralizedKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.gk7GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('GeneralizedKt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.hiGenParticles = cms.EDProducer("GenParticleProducer",
    saveBarCodes = cms.untracked.bool(True),
    doSubEvent = cms.untracked.bool(True),
    useCrossingFrame = cms.untracked.bool(False),
    srcVector = cms.vstring('generator')
)


process.hiGenParticlesForJets = cms.EDProducer("InputGenJetsParticleSelector",
    src = cms.InputTag("hiGenParticles"),
    ignoreParticleIDs = cms.vuint32(1000022, 1000012, 1000014, 1000016, 2000012,
        2000014, 2000016, 1000039, 5100039, 4000012,
        4000014, 4000016, 9900012, 9900014, 9900016,
        39),
    partonicFinalState = cms.bool(False),
    excludeResonances = cms.bool(True),
    excludeFromResonancePids = cms.vuint32(12, 13, 14, 16),
    tausAsJets = cms.bool(False)
)


process.iterativeCone3HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('IterativeCone'),
    rParam = cms.double(0.3)
)


process.iterativeCone4HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('IterativeCone'),
    rParam = cms.double(0.4)
)


process.iterativeCone5GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('IterativeCone'),
    rParam = cms.double(0.5)
)


process.iterativeCone5GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('IterativeCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.iterativeCone5GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('IterativeCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.iterativeCone5HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('IterativeCone'),
    rParam = cms.double(0.5)
)


process.iterativeCone7HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('IterativeCone'),
    rParam = cms.double(0.7)
)


process.kt3HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('Kt'),
    rParam = cms.double(0.3)
)


process.kt4GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('Kt'),
    rParam = cms.double(0.4)
)


process.kt4GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('Kt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.4),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.kt4GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('Kt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.4),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.kt4HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('Kt'),
    rParam = cms.double(0.4)
)


process.kt6GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('Kt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.6),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJets"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.kt6GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('Kt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.6),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.kt6GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('Kt'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.6),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.kt6HiGenJets = cms.EDProducer("SubEventGenJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("hiGenParticlesForJets"),
    doAreaFastjet = cms.bool(True),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('Kt'),
    rParam = cms.double(0.6)
)


process.matchVtx = cms.EDProducer("MixEvtVtxGenerator",
    heavyIonLabel = cms.InputTag("generator"),
    signalLabel = cms.InputTag("hiSignal")
)


process.mix = cms.EDProducer("MixingModule",
    mixProdStep1 = cms.bool(False),
    mixProdStep2 = cms.bool(False),
    maxBunch = cms.int32(3),
    useCurrentProcessOnly = cms.bool(False),
    minBunch = cms.int32(-5),
    bunchspace = cms.int32(25),
    playback = cms.untracked.bool(False),
    LabelPlayback = cms.string(''),
    mixObjects = cms.PSet(
        mixCH = cms.PSet(
            input = cms.VInputTag(cms.InputTag("g4SimHits","CaloHitsTk"), cms.InputTag("g4SimHits","CastorBU"), cms.InputTag("g4SimHits","CastorFI"), cms.InputTag("g4SimHits","CastorPL"), cms.InputTag("g4SimHits","CastorTU"),
                cms.InputTag("g4SimHits","EcalHitsEB"), cms.InputTag("g4SimHits","EcalHitsEE"), cms.InputTag("g4SimHits","EcalHitsES"), cms.InputTag("g4SimHits","EcalTBH4BeamHits"), cms.InputTag("g4SimHits","HcalHits"),
                cms.InputTag("g4SimHits","HcalTB06BeamHits"), cms.InputTag("g4SimHits","ZDCHITS")),
            type = cms.string('PCaloHit'),
            subdets = cms.vstring('CaloHitsTk',
                'CastorBU',
                'CastorFI',
                'CastorPL',
                'CastorTU',
                'EcalHitsEB',
                'EcalHitsEE',
                'EcalHitsES',
                'EcalTBH4BeamHits',
                'HcalHits',
                'HcalTB06BeamHits',
                'ZDCHITS')
        ),
        mixHepMC = cms.PSet(
            input = cms.VInputTag(cms.InputTag("generator")),
            type = cms.string('HepMCProduct')
        ),
        mixVertices = cms.PSet(
            input = cms.VInputTag(cms.InputTag("g4SimHits")),
            type = cms.string('SimVertex')
        ),
        mixSH = cms.PSet(
            input = cms.VInputTag(cms.InputTag("g4SimHits","BSCHits"), cms.InputTag("g4SimHits","FP420SI"), cms.InputTag("g4SimHits","MuonCSCHits"), cms.InputTag("g4SimHits","MuonDTHits"), cms.InputTag("g4SimHits","MuonRPCHits"),
                cms.InputTag("g4SimHits","TotemHitsRP"), cms.InputTag("g4SimHits","TotemHitsT1"), cms.InputTag("g4SimHits","TotemHitsT2Gem"), cms.InputTag("g4SimHits","TrackerHitsPixelBarrelHighTof"), cms.InputTag("g4SimHits","TrackerHitsPixelBarrelLowTof"),
                cms.InputTag("g4SimHits","TrackerHitsPixelEndcapHighTof"), cms.InputTag("g4SimHits","TrackerHitsPixelEndcapLowTof"), cms.InputTag("g4SimHits","TrackerHitsTECHighTof"), cms.InputTag("g4SimHits","TrackerHitsTECLowTof"), cms.InputTag("g4SimHits","TrackerHitsTIBHighTof"),
                cms.InputTag("g4SimHits","TrackerHitsTIBLowTof"), cms.InputTag("g4SimHits","TrackerHitsTIDHighTof"), cms.InputTag("g4SimHits","TrackerHitsTIDLowTof"), cms.InputTag("g4SimHits","TrackerHitsTOBHighTof"), cms.InputTag("g4SimHits","TrackerHitsTOBLowTof")),
            type = cms.string('PSimHit'),
            subdets = cms.vstring('BSCHits',
                'FP420SI',
                'MuonCSCHits',
                'MuonDTHits',
                'MuonRPCHits',
                'TotemHitsRP',
                'TotemHitsT1',
                'TotemHitsT2Gem',
                'TrackerHitsPixelBarrelHighTof',
                'TrackerHitsPixelBarrelLowTof',
                'TrackerHitsPixelEndcapHighTof',
                'TrackerHitsPixelEndcapLowTof',
                'TrackerHitsTECHighTof',
                'TrackerHitsTECLowTof',
                'TrackerHitsTIBHighTof',
                'TrackerHitsTIBLowTof',
                'TrackerHitsTIDHighTof',
                'TrackerHitsTIDLowTof',
                'TrackerHitsTOBHighTof',
                'TrackerHitsTOBLowTof')
        ),
        mixTracks = cms.PSet(
            input = cms.VInputTag(cms.InputTag("g4SimHits")),
            type = cms.string('SimTrack')
        )
    )
)


process.randomEngineStateProducer = cms.EDProducer("RandomEngineStateProducer")


process.sisCone5GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    GhostArea = cms.double(0.01),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0),
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999),
    jetAlgorithm = cms.string('SISCone'),
    rParam = cms.double(0.5)
)


process.sisCone5GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('SISCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.sisCone5GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('SISCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.5),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.sisCone7GenJets = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('SISCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJets"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.sisCone7GenJetsNoMuNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('SISCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoMuNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.sisCone7GenJetsNoNu = cms.EDProducer("FastjetJetProducer",
    Active_Area_Repeats = cms.int32(5),
    doAreaFastjet = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    jetType = cms.string('GenJet'),
    doRhoFastjet = cms.bool(False),
    jetAlgorithm = cms.string('SISCone'),
    nSigmaPU = cms.double(1.0),
    GhostArea = cms.double(0.01),
    Rho_EtaMax = cms.double(4.5),
    maxBadEcalCells = cms.uint32(9999999),
    doPVCorrection = cms.bool(False),
    maxRecoveredHcalCells = cms.uint32(9999999),
    rParam = cms.double(0.7),
    maxProblematicHcalCells = cms.uint32(9999999),
    src = cms.InputTag("genParticlesForJetsNoNu"),
    inputEtMin = cms.double(0.0),
    srcPVs = cms.InputTag(""),
    jetPtMin = cms.double(3.0),
    radiusPU = cms.double(0.5),
    maxProblematicEcalCells = cms.uint32(9999999),
    doPUOffsetCorr = cms.bool(False),
    inputEMin = cms.double(0.0)
)


process.MuFromBd = cms.EDFilter("PythiaFilter",
    MaxEta = cms.untracked.double(2.5),
    Status = cms.untracked.int32(1),
    MinEta = cms.untracked.double(-2.5),
    MotherID = cms.untracked.int32(511),
    ParticleID = cms.untracked.int32(13)
)


process.generator = cms.EDFilter("Pythia6GeneratorFilter",
    ExternalDecays = cms.PSet(
        EvtGen = cms.untracked.PSet(
            use_default_decay = cms.untracked.bool(False),
            operates_on_particles = cms.vint32(0),
            particle_property_file = cms.FileInPath('GeneratorInterface/ExternalDecays/data/evt.pdl'),
            user_decay_file = cms.FileInPath('GeneratorInterface/ExternalDecays/data/Bd_mumu.dec'),
            list_forced_decays = cms.vstring('MyB0',
                'Myanti-B0'),
            decay_table = cms.FileInPath('GeneratorInterface/ExternalDecays/data/DECAY_NOLONGLIFE.DEC')
        ),
        parameterSets = cms.vstring('EvtGen')
    ),
    pythiaPylistVerbosity = cms.untracked.int32(0),
    filterEfficiency = cms.untracked.double(4e-05),
    pythiaHepMCVerbosity = cms.untracked.bool(False),
    comEnergy = cms.double(7000.0),
    crossSection = cms.untracked.double(71260000000.0),
    maxEventsToPrint = cms.untracked.int32(0),
    PythiaParameters = cms.PSet(
        bbbarSettings = cms.vstring('MSEL = 1'),
        pythiaUESettings = cms.vstring('MSTU(21)=1     ! Check on possible errors during program execution',
            'MSTJ(22)=2     ! Decay those unstable particles',
            'PARJ(71)=10 .  ! for which ctau  10 mm',
            'MSTP(33)=0     ! no K factors in hard cross sections',
            'MSTP(2)=1      ! which order running alphaS',
            'MSTP(51)=10042 ! structure function chosen (external PDF CTEQ6L1)',
            'MSTP(52)=2     ! work with LHAPDF',
            'PARP(82)=1.832 ! pt cutoff for multiparton interactions',
            'PARP(89)=1800. ! sqrts for which PARP82 is set',
            'PARP(90)=0.275 ! Multiple interactions: rescaling power',
            'MSTP(95)=6     ! CR (color reconnection parameters)',
            'PARP(77)=1.016 ! CR',
            'PARP(78)=0.538 ! CR',
            'PARP(80)=0.1   ! Prob. colored parton from BBR',
            'PARP(83)=0.356 ! Multiple interactions: matter distribution parameter',
            'PARP(84)=0.651 ! Multiple interactions: matter distribution parameter',
            'PARP(62)=1.025 ! ISR cutoff',
            'MSTP(91)=1     ! Gaussian primordial kT',
            'PARP(93)=10.0  ! primordial kT-max',
            'MSTP(81)=21    ! multiple parton interactions 1 is Pythia default',
            'MSTP(82)=4     ! Defines the multi-parton model'),
        parameterSets = cms.vstring('pythiaUESettings',
            'bbbarSettings')
    )
)


process.mumugenfilter = cms.EDFilter("MCParticlePairFilter",
    MaxEta = cms.untracked.vdouble(2.5, 2.5),
    Status = cms.untracked.vint32(1, 1),
    MinEta = cms.untracked.vdouble(-2.5, -2.5),
    ParticleID1 = cms.untracked.vint32(-13, 13),
    ParticleID2 = cms.untracked.vint32(-13, 13)
)


process.MEtoMEComparitor = cms.EDAnalyzer("MEtoMEComparitor",
    Diffgoodness = cms.double(0.1),
    processRef = cms.string('HLT'),
    dirDepth = cms.uint32(1),
    KSgoodness = cms.double(0.9),
    runInstance = cms.string('MEtoEDMConverterRun'),
    MEtoEDMLabel = cms.string('MEtoEDMConverter'),
    autoProcess = cms.bool(False),
    processNew = cms.string('RERECO'),
    OverAllgoodness = cms.double(0.9),
    lumiInstance = cms.string('MEtoEDMConverterLumi')
)


process.RAWSIMoutput = cms.OutputModule("PoolOutputModule",
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'drop *',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep *_g4SimHits_*_*',
        'keep edmHepMCProduct_source_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*',
        'keep EBSrFlagsSorted_simEcalDigis_*_*',
        'keep EESrFlagsSorted_simEcalDigis_*_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep PileupSummaryInfos_*_*_*',
        'keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*',
        'keep recoGenJets_*_*_*',
        'keep *_genParticle_*_*',
        'keep recoGenMETs_*_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep *_randomEngineStateProducer_*_*',
        'keep *_logErrorHarvester_*_*'),
    fileName = cms.untracked.string('step1.root'),
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('generation_step')
    ),
    dataset = cms.untracked.PSet(
        filterName = cms.untracked.string(''),
        dataTier = cms.untracked.string('GEN-SIM')
    )
)


process.genMETParticles = cms.Sequence(process.genCandidatesForMET+process.genParticlesForMETAllVisible)


process.recoAllGenJetsNoMuNoNu = cms.Sequence(process.sisCone5GenJetsNoMuNoNu+process.sisCone7GenJetsNoMuNoNu+process.kt4GenJetsNoMuNoNu+process.kt6GenJetsNoMuNoNu+process.iterativeCone5GenJetsNoMuNoNu+process.ak5GenJetsNoMuNoNu+process.ak7GenJetsNoMuNoNu+process.gk5GenJetsNoMuNoNu+process.gk7GenJetsNoMuNoNu+process.ca4GenJetsNoMuNoNu+process.ca6GenJetsNoMuNoNu)


process.recoAllGenJetsNoNu = cms.Sequence(process.sisCone5GenJetsNoNu+process.sisCone7GenJetsNoNu+process.kt4GenJetsNoNu+process.kt6GenJetsNoNu+process.iterativeCone5GenJetsNoNu+process.ak5GenJetsNoNu+process.ak7GenJetsNoNu+process.gk5GenJetsNoNu+process.gk7GenJetsNoNu+process.ca4GenJetsNoNu+process.ca6GenJetsNoNu)


process.ProductionFilterSequence = cms.Sequence(process.generator+process.MuFromBd+process.mumugenfilter)


process.hiRecoGenJets = cms.Sequence(process.iterativeCone5HiGenJets+process.iterativeCone7HiGenJets+process.ak5HiGenJets+process.ak7HiGenJets+process.kt4HiGenJets+process.kt6HiGenJets)


process.genFilterSummary = cms.Sequence(process.genFilterEfficiencyProducer)


process.recoAllGenJets = cms.Sequence(process.sisCone5GenJets+process.sisCone7GenJets+process.kt4GenJets+process.kt6GenJets+process.iterativeCone5GenJets+process.ak5GenJets+process.ak7GenJets+process.gk5GenJets+process.gk7GenJets+process.ca4GenJets+process.ca6GenJets)


process.GeneInfo = cms.Sequence(process.genParticles)


process.endOfProcess = cms.Sequence(process.MEtoEDMConverter)


process.genJetParticles = cms.Sequence(process.genParticlesForJets)


process.VertexSmearing = cms.Sequence(cms.SequencePlaceholder("VtxSmeared"))


process.endOfProcess_withComparison = cms.Sequence(process.MEtoEDMConverter+process.MEtoMEComparitor)


process.hiGenJets = cms.Sequence(process.hiGenParticlesForJets+process.hiRecoGenJets)


process.recoGenMET = cms.Sequence(process.genMetCalo+process.genMetCaloAndNonPrompt+process.genMetTrue+process.genMetIC5GenJets)


process.psim = cms.Sequence(cms.SequencePlaceholder("randomEngineStateProducer")+process.g4SimHits)


process.pgen_hi = cms.Sequence(cms.SequencePlaceholder("randomEngineStateProducer")+process.VertexSmearing+process.hiGenParticles+process.hiGenJets)


process.recoGenJets = cms.Sequence(process.kt4GenJets+process.kt6GenJets+process.iterativeCone5GenJets+process.ak5GenJets+process.ak7GenJets)


process.pgen_genonly = cms.Sequence(cms.SequencePlaceholder("randomEngineStateProducer"))


process.pgen_himix = cms.Sequence(cms.SequencePlaceholder("randomEngineStateProducer")+process.matchVtx+process.hiGenParticles+process.hiGenJets)


process.genJetMET = cms.Sequence(process.genJetParticles+process.recoGenJets+process.genMETParticles+process.recoGenMET)


process.pgen = cms.Sequence(cms.SequencePlaceholder("randomEngineStateProducer")+process.VertexSmearing+process.GeneInfo+process.genJetMET)


process.generation_step = cms.Path(process.ProductionFilterSequence+process.pgen)


process.simulation_step = cms.Path(process.ProductionFilterSequence+process.psim)


process.genfiltersummary_step = cms.EndPath(process.genFilterSummary)


process.endjob_step = cms.EndPath(process.endOfProcess)


process.RAWSIMoutput_step = cms.EndPath(process.RAWSIMoutput)


process.DQMStore = cms.Service("DQMStore")


process.MessageLogger = cms.Service("MessageLogger",
    suppressInfo = cms.untracked.vstring(),
    debugs = cms.untracked.PSet(
        placeholder = cms.untracked.bool(True)
    ),
    suppressDebug = cms.untracked.vstring(),
    cout = cms.untracked.PSet(
        placeholder = cms.untracked.bool(True)
    ),
    cerr_stats = cms.untracked.PSet(
        threshold = cms.untracked.string('WARNING'),
        output = cms.untracked.string('cerr'),
        optionalPSet = cms.untracked.bool(True)
    ),
    warnings = cms.untracked.PSet(
        placeholder = cms.untracked.bool(True)
    ),
    default = cms.untracked.PSet(

    ),
    statistics = cms.untracked.vstring('cerr_stats'),
    cerr = cms.untracked.PSet(
        INFO = cms.untracked.PSet(
            limit = cms.untracked.int32(0)
        ),
        noTimeStamps = cms.untracked.bool(False),
        FwkReport = cms.untracked.PSet(
            reportEvery = cms.untracked.int32(1),
            optionalPSet = cms.untracked.bool(True),
            limit = cms.untracked.int32(10000000)
        ),
        default = cms.untracked.PSet(
            limit = cms.untracked.int32(10000000)
        ),
        Root_NoDictionary = cms.untracked.PSet(
            optionalPSet = cms.untracked.bool(True),
            limit = cms.untracked.int32(0)
        ),
        threshold = cms.untracked.string('INFO'),
        FwkJob = cms.untracked.PSet(
            optionalPSet = cms.untracked.bool(True),
            limit = cms.untracked.int32(0)
        ),
        FwkSummary = cms.untracked.PSet(
            reportEvery = cms.untracked.int32(1),
            optionalPSet = cms.untracked.bool(True),
            limit = cms.untracked.int32(10000000)
        ),
        optionalPSet = cms.untracked.bool(True)
    ),
    FrameworkJobReport = cms.untracked.PSet(
        default = cms.untracked.PSet(
            limit = cms.untracked.int32(0)
        ),
        optionalPSet = cms.untracked.bool(True),
        FwkJob = cms.untracked.PSet(
            optionalPSet = cms.untracked.bool(True),
            limit = cms.untracked.int32(10000000)
        )
    ),
    suppressWarning = cms.untracked.vstring(),
    errors = cms.untracked.PSet(
        placeholder = cms.untracked.bool(True)
    ),
    destinations = cms.untracked.vstring('warnings',
        'errors',
        'infos',
        'debugs',
        'cout',
        'cerr'),
    debugModules = cms.untracked.vstring(),
    infos = cms.untracked.PSet(
        optionalPSet = cms.untracked.bool(True),
        Root_NoDictionary = cms.untracked.PSet(
            optionalPSet = cms.untracked.bool(True),
            limit = cms.untracked.int32(0)
        ),
        placeholder = cms.untracked.bool(True)
    ),
    categories = cms.untracked.vstring('FwkJob',
        'FwkReport',
        'FwkSummary',
        'Root_NoDictionary'),
    fwkJobReports = cms.untracked.vstring('FrameworkJobReport')
)


process.RandomNumberGeneratorService = cms.Service("RandomNumberGeneratorService",
    hiSignalG4SimHits = cms.PSet(
        initialSeed = cms.untracked.uint32(11),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    simCastorDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(12345678),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    generator = cms.PSet(
        initialSeed = cms.untracked.uint32(123456789),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    simMuonRPCDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(1234567),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    hiSignal = cms.PSet(
        initialSeed = cms.untracked.uint32(123456789),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    simEcalUnsuppressedDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(1234567),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    saveFileName = cms.untracked.string(''),
    simSiStripDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(1234567),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    mix = cms.PSet(
        initialSeed = cms.untracked.uint32(12345),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    simHcalUnsuppressedDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(11223344),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    LHCTransport = cms.PSet(
        initialSeed = cms.untracked.uint32(87654321),
        engineName = cms.untracked.string('TRandom3')
    ),
    simMuonCSCDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(11223344),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    mixData = cms.PSet(
        initialSeed = cms.untracked.uint32(12345),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    VtxSmeared = cms.PSet(
        initialSeed = cms.untracked.uint32(98765432),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    g4SimHits = cms.PSet(
        initialSeed = cms.untracked.uint32(11),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    simMuonDTDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(1234567),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    simSiPixelDigis = cms.PSet(
        initialSeed = cms.untracked.uint32(1234567),
        engineName = cms.untracked.string('HepJamesRandom')
    ),
    hiSignalLHCTransport = cms.PSet(
        initialSeed = cms.untracked.uint32(88776655),
        engineName = cms.untracked.string('TRandom3')
    )
)


process.CSCGeometryESModule = cms.ESProducer("CSCGeometryESModule",
    appendToDataLabel = cms.string(''),
    useDDD = cms.bool(False),
    debugV = cms.untracked.bool(False),
    useGangedStripsInME1a = cms.bool(True),
    alignmentsLabel = cms.string(''),
    useOnlyWiresInME1a = cms.bool(False),
    useRealWireGeometry = cms.bool(True),
    useCentreTIOffsets = cms.bool(False),
    applyAlignment = cms.bool(True)
)


process.CaloGeometryBuilder = cms.ESProducer("CaloGeometryBuilder",
    SelectedCalos = cms.vstring('HCAL',
        'ZDC',
        'CASTOR',
        'EcalBarrel',
        'EcalEndcap',
        'EcalPreshower',
        'TOWER')
)


process.CaloTopologyBuilder = cms.ESProducer("CaloTopologyBuilder")


process.CaloTowerGeometryFromDBEP = cms.ESProducer("CaloTowerGeometryFromDBEP",
    applyAlignment = cms.bool(False)
)


process.CastorGeometryFromDBEP = cms.ESProducer("CastorGeometryFromDBEP",
    applyAlignment = cms.bool(False)
)


process.DTGeometryESModule = cms.ESProducer("DTGeometryESModule",
    appendToDataLabel = cms.string(''),
    fromDDD = cms.bool(False),
    applyAlignment = cms.bool(True),
    alignmentsLabel = cms.string('')
)


process.EcalBarrelGeometryFromDBEP = cms.ESProducer("EcalBarrelGeometryFromDBEP",
    applyAlignment = cms.bool(True)
)


process.EcalElectronicsMappingBuilder = cms.ESProducer("EcalElectronicsMappingBuilder")


process.EcalEndcapGeometryFromDBEP = cms.ESProducer("EcalEndcapGeometryFromDBEP",
    applyAlignment = cms.bool(True)
)


process.EcalLaserCorrectionService = cms.ESProducer("EcalLaserCorrectionService")


process.EcalPreshowerGeometryFromDBEP = cms.ESProducer("EcalPreshowerGeometryFromDBEP",
    applyAlignment = cms.bool(True)
)


process.EcalTrigTowerConstituentsMapBuilder = cms.ESProducer("EcalTrigTowerConstituentsMapBuilder",
    MapFile = cms.untracked.string('Geometry/EcalMapping/data/EndCap_TTMap.txt')
)


process.GlobalTrackingGeometryESProducer = cms.ESProducer("GlobalTrackingGeometryESProducer")


process.HcalAlignmentEP = cms.ESProducer("HcalAlignmentEP")


process.HcalGeometryFromDBEP = cms.ESProducer("HcalGeometryFromDBEP",
    applyAlignment = cms.bool(False)
)


process.HcalTopologyIdealEP = cms.ESProducer("HcalTopologyIdealEP")


process.MuonDetLayerGeometryESProducer = cms.ESProducer("MuonDetLayerGeometryESProducer")


process.MuonNumberingInitialization = cms.ESProducer("MuonNumberingInitialization")


process.ParametrizedMagneticFieldProducer = cms.ESProducer("ParametrizedMagneticFieldProducer",
    version = cms.string('OAE_1103l_071212'),
    parameters = cms.PSet(
        BValue = cms.string('3_8T')
    ),
    label = cms.untracked.string('parametrizedField')
)


process.RPCGeometryESModule = cms.ESProducer("RPCGeometryESModule",
    useDDD = cms.untracked.bool(False),
    compatibiltyWith11 = cms.untracked.bool(True)
)


process.SiStripRecHitMatcherESProducer = cms.ESProducer("SiStripRecHitMatcherESProducer",
    ComponentName = cms.string('StandardMatcher'),
    NSigmaInside = cms.double(3.0)
)


process.StripCPEfromTrackAngleESProducer = cms.ESProducer("StripCPEESProducer",
    ComponentName = cms.string('StripCPEfromTrackAngle')
)


process.TrackerDigiGeometryESModule = cms.ESProducer("TrackerDigiGeometryESModule",
    appendToDataLabel = cms.string(''),
    fromDDD = cms.bool(False),
    applyAlignment = cms.bool(True),
    alignmentsLabel = cms.string('')
)


process.TrackerGeometricDetESModule = cms.ESProducer("TrackerGeometricDetESModule",
    fromDDD = cms.bool(False)
)


process.TrackerRecoGeometryESProducer = cms.ESProducer("TrackerRecoGeometryESProducer")


process.VolumeBasedMagneticFieldESProducer = cms.ESProducer("VolumeBasedMagneticFieldESProducer",
    scalingVolumes = cms.vint32(14100, 14200, 17600, 17800, 17900,
        18100, 18300, 18400, 18600, 23100,
        23300, 23400, 23600, 23800, 23900,
        24100, 28600, 28800, 28900, 29100,
        29300, 29400, 29600, 28609, 28809,
        28909, 29109, 29309, 29409, 29609,
        28610, 28810, 28910, 29110, 29310,
        29410, 29610, 28611, 28811, 28911,
        29111, 29311, 29411, 29611),
    scalingFactors = cms.vdouble(1, 1, 0.994, 1.004, 1.004,
        1.005, 1.004, 1.004, 0.994, 0.965,
        0.958, 0.958, 0.953, 0.958, 0.958,
        0.965, 0.918, 0.924, 0.924, 0.906,
        0.924, 0.924, 0.918, 0.991, 0.998,
        0.998, 0.978, 0.998, 0.998, 0.991,
        0.991, 0.998, 0.998, 0.978, 0.998,
        0.998, 0.991, 0.991, 0.998, 0.998,
        0.978, 0.998, 0.998, 0.991),
    overrideMasterSector = cms.bool(False),
    useParametrizedTrackerField = cms.bool(True),
    label = cms.untracked.string(''),
    version = cms.string('grid_1103l_090322_3_8t'),
    debugBuilder = cms.untracked.bool(False),
    paramLabel = cms.string('parametrizedField'),
    cacheLastVolume = cms.untracked.bool(True)
)


process.XMLFromDBSource = cms.ESProducer("XMLIdealGeometryESProducer",
    rootDDName = cms.string('cms:OCMS'),
    label = cms.string('Extended')
)


process.ZdcGeometryFromDBEP = cms.ESProducer("ZdcGeometryFromDBEP",
    applyAlignment = cms.bool(False)
)


process.fakeForIdealAlignment = cms.ESProducer("FakeAlignmentProducer",
    appendToDataLabel = cms.string('fakeForIdeal')
)


process.hcal_db_producer = cms.ESProducer("HcalDbProducer",
    file = cms.untracked.string(''),
    dump = cms.untracked.vstring('')
)


process.idealForDigiCSCGeometry = cms.ESProducer("CSCGeometryESModule",
    appendToDataLabel = cms.string('idealForDigi'),
    useDDD = cms.bool(False),
    debugV = cms.untracked.bool(False),
    useGangedStripsInME1a = cms.bool(True),
    alignmentsLabel = cms.string('fakeForIdeal'),
    useOnlyWiresInME1a = cms.bool(False),
    useRealWireGeometry = cms.bool(True),
    useCentreTIOffsets = cms.bool(False),
    applyAlignment = cms.bool(False)
)


process.idealForDigiDTGeometry = cms.ESProducer("DTGeometryESModule",
    appendToDataLabel = cms.string('idealForDigi'),
    fromDDD = cms.bool(False),
    applyAlignment = cms.bool(False),
    alignmentsLabel = cms.string('fakeForIdeal')
)


process.idealForDigiTrackerGeometry = cms.ESProducer("TrackerDigiGeometryESModule",
    appendToDataLabel = cms.string('idealForDigi'),
    fromDDD = cms.bool(False),
    applyAlignment = cms.bool(False),
    alignmentsLabel = cms.string('fakeForIdeal')
)


process.siStripGainESProducer = cms.ESProducer("SiStripGainESProducer",
    printDebug = cms.untracked.bool(False),
    appendToDataLabel = cms.string(''),
    APVGain = cms.VPSet(cms.PSet(
        Record = cms.string('SiStripApvGainRcd'),
        NormalizationFactor = cms.untracked.double(1.0),
        Label = cms.untracked.string('')
    ),
        cms.PSet(
            Record = cms.string('SiStripApvGain2Rcd'),
            NormalizationFactor = cms.untracked.double(1.0),
            Label = cms.untracked.string('')
        )),
    AutomaticNormalization = cms.bool(False)
)


process.siStripQualityESProducer = cms.ESProducer("SiStripQualityESProducer",
    appendToDataLabel = cms.string(''),
    PrintDebugOutput = cms.bool(False),
    ThresholdForReducedGranularity = cms.double(0.3),
    UseEmptyRunInfo = cms.bool(False),
    ReduceGranularity = cms.bool(False),
    ListOfRecordToMerge = cms.VPSet(cms.PSet(
        record = cms.string('SiStripDetVOffRcd'),
        tag = cms.string('')
    ),
        cms.PSet(
            record = cms.string('SiStripDetCablingRcd'),
            tag = cms.string('')
        ),
        cms.PSet(
            record = cms.string('RunInfoRcd'),
            tag = cms.string('')
        ),
        cms.PSet(
            record = cms.string('SiStripBadChannelRcd'),
            tag = cms.string('')
        ),
        cms.PSet(
            record = cms.string('SiStripBadFiberRcd'),
            tag = cms.string('')
        ),
        cms.PSet(
            record = cms.string('SiStripBadModuleRcd'),
            tag = cms.string('')
        ),
        cms.PSet(
            record = cms.string('SiStripBadStripRcd'),
            tag = cms.string('')
        ))
)


process.sistripconn = cms.ESProducer("SiStripConnectivity")


process.GlobalTag = cms.ESSource("PoolDBESSource",
    DBParameters = cms.PSet(
        authenticationPath = cms.untracked.string('.'),
        enableReadOnlySessionOnUpdateConnection = cms.untracked.bool(False),
        idleConnectionCleanupPeriod = cms.untracked.int32(10),
        messageLevel = cms.untracked.int32(0),
        enablePoolAutomaticCleanUp = cms.untracked.bool(False),
        enableConnectionSharing = cms.untracked.bool(True),
        connectionRetrialTimeOut = cms.untracked.int32(60),
        connectionTimeOut = cms.untracked.int32(60),
        connectionRetrialPeriod = cms.untracked.int32(10)
    ),
    BlobStreamerName = cms.untracked.string('TBufferBlobStreamingService'),
    toGet = cms.VPSet(),
    connect = cms.string('frontier://FrontierProd/CMS_COND_31X_GLOBALTAG'),
    globaltag = cms.string('START311_V2::All')
)


process.HepPDTESSource = cms.ESSource("HepPDTESSource",
    pdtFileName = cms.FileInPath('SimGeneral/HepPDTESSource/data/pythiaparticle.tbl')
)


process.eegeom = cms.ESSource("EmptyESSource",
    iovIsRunNotTime = cms.bool(True),
    recordName = cms.string('EcalMappingRcd'),
    firstValid = cms.vuint32(1)
)


process.es_hardcode = cms.ESSource("HcalHardcodeCalibrations",
    toGet = cms.untracked.vstring('GainWidths')
)


process.magfield = cms.ESSource("XMLIdealGeometryESSource",
    geomXMLFiles = cms.vstring('Geometry/CMSCommonData/data/normal/cmsextent.xml',
        'Geometry/CMSCommonData/data/cms.xml',
        'Geometry/CMSCommonData/data/cmsMagneticField.xml',
        'MagneticField/GeomBuilder/data/MagneticFieldVolumes_1103l.xml',
        'MagneticField/GeomBuilder/data/MagneticFieldParameters_07_2pi.xml',
        'Geometry/CMSCommonData/data/materials.xml'),
    rootNodeName = cms.string('cmsMagneticField:MAGF')
)


process.prefer("magfield")

process.ALCARECOEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'keep edmTriggerResults_*_*_*',
        'keep *_ALCARECOTkAlZMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOTkAlCosmicsInCollisions_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*',
        'keep *_ALCARECOTkAlCosmicsCTF_*_*',
        'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*',
        'keep *_ALCARECOTkAlCosmicsCTF_*_*',
        'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*',
        'keep *_ALCARECOTkAlCosmics*0T_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*',
        'keep *_ALCARECOTkAlCosmics*0T_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*',
        'keep *_ALCARECOTkAlLAST0Producer_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOTkAlMuonIsolated_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOTkAlJpsiMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOTkAlUpsilonMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOTkAlMinBias_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep *_offlineBeamSpot_*_*',
        'keep *_ALCARECOTkAlBeamHalo_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOSiStripCalZeroBias_*_*',
        'keep *_calZeroBiasClusters_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep *_TriggerResults_*_*',
        'keep *_ALCARECOSiStripCalMinBias_*_*',
        'keep *_siStripClusters_*_*',
        'keep *_siPixelClusters_*_*',
        'keep DetIdedmEDCollection_siStripDigis_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep *_ecalPhiSymCorrected_phiSymEcalRecHitsEB_*',
        'keep *_ecalPhiSymCorrected_phiSymEcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep recoGsfElectronCores_*_*_*',
        'keep recoSuperClusters_*_*_*',
        'keep *_electronGsfTracks_*_*',
        'keep  *_gsfElectrons_*_*',
        'keep  *_alCaIsolatedElectrons_*_*',
        'keep recoCaloMETs_met_*_*',
        'keep edmTriggerResults_TriggerResults__*',
        'keep edmHepMCProduct_*_*_*',
        'keep *_ecalPi0Corrected_pi0EcalRecHitsEB_*',
        'keep *_ecalPi0Corrected_pi0EcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_pi0EcalRecHitsES_*',
        'keep *_ecalEtaCorrected_etaEcalRecHitsEB_*',
        'keep *_ecalEtaCorrected_etaEcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_etaEcalRecHitsES_*',
        'keep *_DiJProd_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep *_GammaJetProd_*_*',
        'keep *_IsoProd_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep *_IsoProd_*_*',
        'keep *_gtDigisAlCaMB_*_*',
        'keep HBHERecHitsSorted_hbherecoMB_*_*',
        'keep HORecHitsSorted_horecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMBspecial_*_*',
        'keep HBHERecHitsSorted_hbherecoNoise_*_*',
        'keep HORecHitsSorted_horecoNoise_*_*',
        'keep HFRecHitsSorted_hfrecoNoise_*_*',
        'keep *_hoCalibProducer_*_*',
        'keep HOCalibVariabless_*_*_*',
        'keep *_HcalNoiseProd_*_*',
        'keep edmTriggerResults_*_*_HLT',
        'keep *_ALCARECOMuAlStandAloneCosmics_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlGlobalCosmicsInCollisions_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlGlobalCosmics_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlCalIsolatedMu_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlZMuMu_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlOverlaps_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlBeamHaloOverlaps_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_ALCARECOMuAlBeamHalo_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_muonDTDigis_*_*',
        'keep CSCDetIdCSCWireDigiMuonDigiCollection_*_*_*',
        'keep CSCDetIdCSCStripDigiMuonDigiCollection_*_*_*',
        'keep DTLayerIdDTDigiMuonDigiCollection_*_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep RPCDetIdRPCDigiMuonDigiCollection_*_*_*',
        'keep recoMuons_muonsNoRPC_*_*',
        'keep L1MuRegionalCands_*_RPCb_*',
        'keep L1MuRegionalCands_*_RPCf_*',
        'keep L1MuGMTCands_*_*_*',
        'keep L1MuGMTReadoutCollection_*_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_dt4DSegmentsNoWire_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_dttfDigis_*_*',
        'keep *_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*',
        'drop *_MEtoEDMConverter_*_*')
)

process.AODEventContent = cms.PSet(
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_castorreco_*_*',
        'keep *_reducedHcalRecHits_*_*',
        'keep *_selectDigi_*_*',
        'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*',
        'keep recoSuperClusters_correctedHybridSuperClusters_*_*',
        'keep recoCaloClusters_hybridSuperClusters_*_*',
        'keep recoSuperClusters_hybridSuperClusters_uncleanOnlyHybridBarrelSuperClusters_*',
        'keep recoCaloClusters_multi5x5BasicClusters_multi5x5EndcapBasicClusters_*',
        'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*',
        'keep recoTracks_GsfGlobalElectronTest_*_*',
        'keep recoGsfTracks_electronGsfTracks_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoTracks_rsWithMaterialTracks_*_*',
        'keep recoTracks_beamhaloTracks_*_*',
        'keep recoTracks_regionalCosmicTracks_*_*',
        'keep recoTracks_ctfPixelLess_*_*',
        'keep *_dedxHarmonic2_*_*',
        'keep *_trackExtrapolator_*_*',
        'keep *_kt4CaloJets_*_*',
        'keep *_kt6CaloJets_*_*',
        'keep *_ak5CaloJets_*_*',
        'keep *_ak7CaloJets_*_*',
        'keep *_kt4PFJets_*_*',
        'keep *_kt6PFJets_*_*',
        'keep *_ak5PFJets_*_*',
        'keep *_ak7PFJets_*_*',
        'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_ak5TrackJets_*_*',
        'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*',
        'keep *_caloTowers_*_*',
        'keep *_towerMaker_*_*',
        'keep *_CastorTowerReco_*_*',
        'keep *_ak5JetTracksAssociatorAtVertex_*_*',
        'keep *_ak7JetTracksAssociatorAtVertex_*_*',
        'keep *_kt4JetExtender_*_*',
        'keep *_ak5JetExtender_*_*',
        'keep *_ak7JetExtender_*_*',
        'keep *_ak5JetID_*_*',
        'keep *_ak7JetID_*_*',
        'keep *_kt4JetID_*_*',
        'keep *_kt6JetID_*_*',
        'keep *_ak7BasicJets_*_*',
        'keep *_ak7CastorJetID_*_*',
        'keep recoCaloMETs_met_*_*',
        'keep recoCaloMETs_metNoHF_*_*',
        'keep recoCaloMETs_metHO_*_*',
        'keep recoCaloMETs_corMetGlobalMuons_*_*',
        'keep recoMETs_htMetAK5_*_*',
        'keep recoMETs_htMetAK7_*_*',
        'keep recoMETs_htMetIC5_*_*',
        'keep recoMETs_htMetKT4_*_*',
        'keep recoMETs_htMetKT6_*_*',
        'keep recoMETs_tcMet_*_*',
        'keep recoMETs_tcMetWithPFclusters_*_*',
        'keep recoPFMETs_pfMet_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*',
        'drop recoHcalNoiseRBXs_*_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*',
        'keep *GlobalHaloData_*_*_*',
        'keep *BeamHaloSummary_BeamHaloSummary_*_*',
        'keep recoTracks_standAloneMuons_*_*',
        'keep recoTrackExtras_standAloneMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneMuons_*_*',
        'keep recoTracks_globalMuons_*_*',
        'keep recoTrackExtras_globalMuons_*_*',
        'keep recoTracks_tevMuons_*_*',
        'keep recoTrackExtras_tevMuons_*_*',
        'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*',
        'keep recoMuonTimeExtraedmValueMap_muons_*_*',
        'keep *_muonShowerInformation_*_*',
        'keep recoTracks_cosmicMuons_*_*',
        'keep recoTracks_globalCosmicMuons_*_*',
        'keep recoMuons_muonsFromCosmics_*_*',
        'keep recoTracks_cosmicMuons1Leg_*_*',
        'keep recoTracks_globalCosmicMuons1Leg_*_*',
        'keep recoMuons_muonsFromCosmics1Leg_*_*',
        'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*',
        'keep uintedmValueMap_cosmicsVeto_*_*',
        'keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*',
        'keep *_trackCountingHighEffBJetTags_*_*',
        'keep *_trackCountingHighPurBJetTags_*_*',
        'keep *_jetProbabilityBJetTags_*_*',
        'keep *_jetBProbabilityBJetTags_*_*',
        'keep *_simpleSecondaryVertexBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighEffBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighPurBJetTags_*_*',
        'keep *_combinedSecondaryVertexBJetTags_*_*',
        'keep *_combinedSecondaryVertexMVABJetTags_*_*',
        'keep *_softElectronBJetTags_*_*',
        'keep *_softElectronByIP3dBJetTags_*_*',
        'keep *_softElectronByPtBJetTags_*_*',
        'keep *_softMuonBJetTags_*_*',
        'keep *_softMuonByIP3dBJetTags_*_*',
        'keep *_softMuonByPtBJetTags_*_*',
        'keep *_combinedMVABJetTags_*_*',
        'keep *_ak5PFJetsRecoTauPiZeros_*_*',
        'keep *_hpsPFTauProducer*_*_*',
        'keep *_hpsPFTauDiscrimination*_*_*',
        'keep *_shrinkingConePFTauProducer*_*_*',
        'keep *_shrinkingConePFTauDiscrimination*_*_*',
        'keep *_hpsTancTaus_*_*',
        'keep *_hpsTancTausDiscrimination*_*_*',
        'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_caloRecoTauTagInfoProducer_*_*',
        'keep recoCaloTaus_caloRecoTauProducer*_*_*',
        'keep *_caloRecoTauDiscrimination*_*_*',
        'keep  *_offlinePrimaryVertices_*_*',
        'keep  *_offlinePrimaryVerticesWithBS_*_*',
        'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*',
        'keep  *_nuclearInteractionMaker_*_*',
        'keep *_generalV0Candidates_*_*',
        'keep recoGsfElectronCores_gsfElectronCores_*_*',
        'keep recoGsfElectrons_gsfElectrons_*_*',
        'keep floatedmValueMap_eidRobustLoose_*_*',
        'keep floatedmValueMap_eidRobustTight_*_*',
        'keep floatedmValueMap_eidRobustHighEnergy_*_*',
        'keep floatedmValueMap_eidLoose_*_*',
        'keep floatedmValueMap_eidTight_*_*',
        'keep recoPhotonCores_photonCore_*_*',
        'keep recoPhotons_photons_*_*',
        'keep recoConversions_conversions_*_*',
        'drop *_conversions_uncleanedConversions_*',
        'keep recoConversions_trackerOnlyConversions_*_*',
        'keep recoTracks_ckfOutInTracksFromConversions_*_*',
        'keep recoTracks_ckfInOutTracksFromConversions_*_*',
        'keep *_PhotonIDProd_*_*',
        'keep *_hfRecoEcalCandidate_*_*',
        'keep *_hfEMClusters_*_*',
        'drop CaloTowersSorted_towerMakerPF_*_*',
        'drop *_pfElectronTranslator_*_*',
        'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*',
        'keep recoPFCandidates_particleFlow_*_*',
        'keep recoCaloClusters_pfElectronTranslator_*_*',
        'keep recoPreshowerClusters_pfElectronTranslator_*_*',
        'keep recoSuperClusters_pfElectronTranslator_*_*',
        'keep *_offlineBeamSpot_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*',
        'keep LumiSummary_lumiProducer_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1TriggerScalerss_*_*_*',
        'keep Level1TriggerScalerss_*_*_*',
        'keep LumiScalerss_*_*_*',
        'keep BeamSpotOnlines_*_*_*',
        'keep DcsStatuss_*_*_*',
        'keep *_logErrorHarvester_*_*',
        'keep PileupSummaryInfos_*_*_*')
)

process.AODSIMEventContent = cms.PSet(
    outputCommands = cms.untracked.vstring('drop *',
        'drop *',
        'keep *_castorreco_*_*',
        'keep *_reducedHcalRecHits_*_*',
        'keep *_selectDigi_*_*',
        'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*',
        'keep recoSuperClusters_correctedHybridSuperClusters_*_*',
        'keep recoCaloClusters_hybridSuperClusters_*_*',
        'keep recoSuperClusters_hybridSuperClusters_uncleanOnlyHybridBarrelSuperClusters_*',
        'keep recoCaloClusters_multi5x5BasicClusters_multi5x5EndcapBasicClusters_*',
        'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*',
        'keep recoTracks_GsfGlobalElectronTest_*_*',
        'keep recoGsfTracks_electronGsfTracks_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoTracks_rsWithMaterialTracks_*_*',
        'keep recoTracks_beamhaloTracks_*_*',
        'keep recoTracks_regionalCosmicTracks_*_*',
        'keep recoTracks_ctfPixelLess_*_*',
        'keep *_dedxHarmonic2_*_*',
        'keep *_trackExtrapolator_*_*',
        'keep *_kt4CaloJets_*_*',
        'keep *_kt6CaloJets_*_*',
        'keep *_ak5CaloJets_*_*',
        'keep *_ak7CaloJets_*_*',
        'keep *_kt4PFJets_*_*',
        'keep *_kt6PFJets_*_*',
        'keep *_ak5PFJets_*_*',
        'keep *_ak7PFJets_*_*',
        'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_ak5TrackJets_*_*',
        'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*',
        'keep *_caloTowers_*_*',
        'keep *_towerMaker_*_*',
        'keep *_CastorTowerReco_*_*',
        'keep *_ak5JetTracksAssociatorAtVertex_*_*',
        'keep *_ak7JetTracksAssociatorAtVertex_*_*',
        'keep *_kt4JetExtender_*_*',
        'keep *_ak5JetExtender_*_*',
        'keep *_ak7JetExtender_*_*',
        'keep *_ak5JetID_*_*',
        'keep *_ak7JetID_*_*',
        'keep *_kt4JetID_*_*',
        'keep *_kt6JetID_*_*',
        'keep *_ak7BasicJets_*_*',
        'keep *_ak7CastorJetID_*_*',
        'keep recoCaloMETs_met_*_*',
        'keep recoCaloMETs_metNoHF_*_*',
        'keep recoCaloMETs_metHO_*_*',
        'keep recoCaloMETs_corMetGlobalMuons_*_*',
        'keep recoMETs_htMetAK5_*_*',
        'keep recoMETs_htMetAK7_*_*',
        'keep recoMETs_htMetIC5_*_*',
        'keep recoMETs_htMetKT4_*_*',
        'keep recoMETs_htMetKT6_*_*',
        'keep recoMETs_tcMet_*_*',
        'keep recoMETs_tcMetWithPFclusters_*_*',
        'keep recoPFMETs_pfMet_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*',
        'drop recoHcalNoiseRBXs_*_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*',
        'keep *GlobalHaloData_*_*_*',
        'keep *BeamHaloSummary_BeamHaloSummary_*_*',
        'keep recoTracks_standAloneMuons_*_*',
        'keep recoTrackExtras_standAloneMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneMuons_*_*',
        'keep recoTracks_globalMuons_*_*',
        'keep recoTrackExtras_globalMuons_*_*',
        'keep recoTracks_tevMuons_*_*',
        'keep recoTrackExtras_tevMuons_*_*',
        'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*',
        'keep recoMuonTimeExtraedmValueMap_muons_*_*',
        'keep *_muonShowerInformation_*_*',
        'keep recoTracks_cosmicMuons_*_*',
        'keep recoTracks_globalCosmicMuons_*_*',
        'keep recoMuons_muonsFromCosmics_*_*',
        'keep recoTracks_cosmicMuons1Leg_*_*',
        'keep recoTracks_globalCosmicMuons1Leg_*_*',
        'keep recoMuons_muonsFromCosmics1Leg_*_*',
        'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*',
        'keep uintedmValueMap_cosmicsVeto_*_*',
        'keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*',
        'keep *_trackCountingHighEffBJetTags_*_*',
        'keep *_trackCountingHighPurBJetTags_*_*',
        'keep *_jetProbabilityBJetTags_*_*',
        'keep *_jetBProbabilityBJetTags_*_*',
        'keep *_simpleSecondaryVertexBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighEffBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighPurBJetTags_*_*',
        'keep *_combinedSecondaryVertexBJetTags_*_*',
        'keep *_combinedSecondaryVertexMVABJetTags_*_*',
        'keep *_softElectronBJetTags_*_*',
        'keep *_softElectronByIP3dBJetTags_*_*',
        'keep *_softElectronByPtBJetTags_*_*',
        'keep *_softMuonBJetTags_*_*',
        'keep *_softMuonByIP3dBJetTags_*_*',
        'keep *_softMuonByPtBJetTags_*_*',
        'keep *_combinedMVABJetTags_*_*',
        'keep *_ak5PFJetsRecoTauPiZeros_*_*',
        'keep *_hpsPFTauProducer*_*_*',
        'keep *_hpsPFTauDiscrimination*_*_*',
        'keep *_shrinkingConePFTauProducer*_*_*',
        'keep *_shrinkingConePFTauDiscrimination*_*_*',
        'keep *_hpsTancTaus_*_*',
        'keep *_hpsTancTausDiscrimination*_*_*',
        'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_caloRecoTauTagInfoProducer_*_*',
        'keep recoCaloTaus_caloRecoTauProducer*_*_*',
        'keep *_caloRecoTauDiscrimination*_*_*',
        'keep  *_offlinePrimaryVertices_*_*',
        'keep  *_offlinePrimaryVerticesWithBS_*_*',
        'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*',
        'keep  *_nuclearInteractionMaker_*_*',
        'keep *_generalV0Candidates_*_*',
        'keep recoGsfElectronCores_gsfElectronCores_*_*',
        'keep recoGsfElectrons_gsfElectrons_*_*',
        'keep floatedmValueMap_eidRobustLoose_*_*',
        'keep floatedmValueMap_eidRobustTight_*_*',
        'keep floatedmValueMap_eidRobustHighEnergy_*_*',
        'keep floatedmValueMap_eidLoose_*_*',
        'keep floatedmValueMap_eidTight_*_*',
        'keep recoPhotonCores_photonCore_*_*',
        'keep recoPhotons_photons_*_*',
        'keep recoConversions_conversions_*_*',
        'drop *_conversions_uncleanedConversions_*',
        'keep recoConversions_trackerOnlyConversions_*_*',
        'keep recoTracks_ckfOutInTracksFromConversions_*_*',
        'keep recoTracks_ckfInOutTracksFromConversions_*_*',
        'keep *_PhotonIDProd_*_*',
        'keep *_hfRecoEcalCandidate_*_*',
        'keep *_hfEMClusters_*_*',
        'drop CaloTowersSorted_towerMakerPF_*_*',
        'drop *_pfElectronTranslator_*_*',
        'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*',
        'keep recoPFCandidates_particleFlow_*_*',
        'keep recoCaloClusters_pfElectronTranslator_*_*',
        'keep recoPreshowerClusters_pfElectronTranslator_*_*',
        'keep recoSuperClusters_pfElectronTranslator_*_*',
        'keep *_offlineBeamSpot_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*',
        'keep LumiSummary_lumiProducer_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1TriggerScalerss_*_*_*',
        'keep Level1TriggerScalerss_*_*_*',
        'keep LumiScalerss_*_*_*',
        'keep BeamSpotOnlines_*_*_*',
        'keep DcsStatuss_*_*_*',
        'keep *_logErrorHarvester_*_*',
        'keep PileupSummaryInfos_*_*_*',
        'keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep *_kt4GenJets_*_*',
        'keep *_kt6GenJets_*_*',
        'keep *_ak5GenJets_*_*',
        'keep *_ak7GenJets_*_*',
        'keep *_genParticle_*_*',
        'keep recoGenMETs_*_*_*',
        'keep PileupSummaryInfos_*_*_*')
)

process.AnomalousCellParameters = cms.PSet(
    maxRecoveredHcalCells = cms.uint32(9999999),
    maxBadEcalCells = cms.uint32(9999999),
    maxProblematicEcalCells = cms.uint32(9999999),
    maxBadHcalCells = cms.uint32(9999999),
    maxRecoveredEcalCells = cms.uint32(9999999),
    maxProblematicHcalCells = cms.uint32(9999999)
)

process.BeamSpotAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_offlineBeamSpot_*_*')
)

process.BeamSpotFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_offlineBeamSpot_*_*')
)

process.BeamSpotRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_offlineBeamSpot_*_*')
)

process.CommonEventContent = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_logErrorHarvester_*_*')
)

process.CondDBSetup = cms.PSet(
    DBParameters = cms.PSet(
        authenticationPath = cms.untracked.string('.'),
        enableReadOnlySessionOnUpdateConnection = cms.untracked.bool(False),
        idleConnectionCleanupPeriod = cms.untracked.int32(10),
        messageLevel = cms.untracked.int32(0),
        enablePoolAutomaticCleanUp = cms.untracked.bool(False),
        enableConnectionSharing = cms.untracked.bool(True),
        connectionRetrialTimeOut = cms.untracked.int32(60),
        connectionTimeOut = cms.untracked.int32(60),
        connectionRetrialPeriod = cms.untracked.int32(10)
    )
)

process.DATAMIXEREventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'keep CaloTowersSorted_calotoweroptmaker_*_*',
        'keep CSCDetIdCSCALCTDigiMuonDigiCollection_muonCSCDigis_MuonCSCALCTDigi_*',
        'keep CSCDetIdCSCCLCTDigiMuonDigiCollection_muonCSCDigis_MuonCSCCLCTDigi_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_muonCSCDigis_MuonCSCComparatorDigi_*',
        'keep CSCDetIdCSCCorrelatedLCTDigiMuonDigiCollection_csctfDigis_*_*',
        'keep CSCDetIdCSCCorrelatedLCTDigiMuonDigiCollection_muonCSCDigis_MuonCSCCorrelatedLCTDigi_*',
        'keep CSCDetIdCSCRPCDigiMuonDigiCollection_muonCSCDigis_MuonCSCRPCDigi_*',
        'keep CSCDetIdCSCStripDigiMuonDigiCollection_muonCSCDigis_MuonCSCStripDigi_*',
        'keep CSCDetIdCSCWireDigiMuonDigiCollection_muonCSCDigis_MuonCSCWireDigi_*',
        'keep DTLayerIdDTDigiMuonDigiCollection_muonDTDigis_*_*',
        'keep PixelDigiedmDetSetVector_siPixelDigis_*_*',
        'keep SiStripDigiedmDetSetVector_siStripDigis_*_*',
        'keep RPCDetIdRPCDigiMuonDigiCollection_muonRPCDigis_*_*',
        'keep HBHEDataFramesSorted_hcalDigis_*_*',
        'keep HFDataFramesSorted_hcalDigis_*_*',
        'keep HODataFramesSorted_hcalDigis_*_*',
        'keep EBDigiCollection_ecalDigis_*_*',
        'keep EEDigiCollection_ecalDigis_*_*',
        'keep ESDataFramesSorted_ecalPreshowerDigis_*_*')
)

process.DQMEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_MEtoEDMConverter_*_*')
)

process.DigiToRawFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep FEDRawDataCollection_source_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*')
)

process.Early10TeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early10TeVX322Y10000VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(1.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early10TeVX322Y1000VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.1),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early10TeVX322Y100VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.01),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early10TeVX322Y250VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.025),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early10TeVX322Y5000VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.5),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early10TeVX322Y500VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(300.0),
    Emittance = cms.double(1.406e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(3.8),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.05),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early2p2TeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(1100.0),
    Emittance = cms.double(6.4e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.5),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early7TeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(1100.0),
    Emittance = cms.double(2e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(4.2),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.Early900GeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(1100.0),
    Emittance = cms.double(1.564e-06),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(7.4),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.EarlyCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(200.0),
    Emittance = cms.double(1.006e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.EvtScalersAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep L1AcceptBunchCrossings_*_*_*',
        'keep L1TriggerScalerss_*_*_*',
        'keep Level1TriggerScalerss_*_*_*',
        'keep LumiScalerss_*_*_*',
        'keep BeamSpotOnlines_*_*_*',
        'keep DcsStatuss_*_*_*')
)

process.EvtScalersRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep L1AcceptBunchCrossings_*_*_*',
        'keep L1TriggerScalerss_*_*_*',
        'keep Level1TriggerScalerss_*_*_*',
        'keep LumiScalerss_*_*_*',
        'keep BeamSpotOnlines_*_*_*',
        'keep DcsStatuss_*_*_*')
)

process.FEVTDEBUGEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'drop *', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep *_g4SimHits_*_*', 'keep edmHepMCProduct_source_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep EBSrFlagsSorted_simEcalDigis_*_*', 'keep EESrFlagsSorted_simEcalDigis_*_*', 'keep CrossingFramePlaybackInfoExtended_*_*_*', 'keep PileupSummaryInfos_*_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenJets_*_*_*', 'keep *_genParticle_*_*', 'keep recoGenMETs_*_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep *_MEtoEDMConverter_*_*', 'keep *_randomEngineStateProducer_*_*', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*')+cms.untracked.vstring('keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenMETs_*_*_*', 'keep *_kt4GenJets_*_*', 'keep *_kt6GenJets_*_*', 'keep *_ak5GenJets_*_*', 'keep *_ak7GenJets_*_*', 'keep *_iterativeCone5GenJets_*_*', 'keep *_genParticle_*_*', 'keep edmHepMCProduct_source_*_*', 'keep SimTracks_g4SimHits_*_*', 'keep SimVertexs_g4SimHits_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep PileupSummaryInfos_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_simCscTriggerPrimitiveDigis_*_*', 'keep *_simDtTriggerPrimitiveDigis_*_*', 'keep *_simRpcTriggerDigis_*_*', 'keep *_simRctDigis_*_*', 'keep *_simCsctfDigis_*_*', 'keep *_simCsctfTrackDigis_*_*', 'keep *_simDttfDigis_*_*', 'keep *_simGctDigis_*_*', 'keep *_simGmtDigis_*_*', 'keep *_simGtDigis_*_*', 'keep *_cscTriggerPrimitiveDigis_*_*', 'keep *_dtTriggerPrimitiveDigis_*_*', 'keep *_rpcTriggerDigis_*_*', 'keep *_rctDigis_*_*', 'keep *_csctfDigis_*_*', 'keep *_csctfTrackDigis_*_*', 'keep *_dttfDigis_*_*', 'keep *_gctDigis_*_*', 'keep *_gmtDigis_*_*', 'keep *_gtDigis_*_*', 'keep *_gtEvmDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_trackingtruthprod_*_*', 'drop *_electrontruth_*_*', 'keep *_mergedtruth_MergedTrackTruth_*', 'keep CrossingFramePlaybackInfoExtended_*_*_*', 'keep *_simSiPixelDigis_*_*', 'keep *_simSiStripDigis_*_*', 'keep *_allTrackMCMatch_*_*', 'keep *_trackingParticleRecoTrackAsssociation_*_*', 'keep *_assoc2secStepTk_*_*', 'keep *_assoc2thStepTk_*_*', 'keep *_assoc2GsfTracks_*_*', 'keep *_assocOutInConversionTracks_*_*', 'keep *_assocInOutConversionTracks_*_*', 'keep *_simMuonCSCDigis_*_*', 'keep *_simMuonDTDigis_*_*', 'keep *_simMuonRPCDigis_*_*', 'keep *_simEcalDigis_*_*', 'keep *_simEcalPreshowerDigis_*_*', 'keep *_simEcalTriggerPrimitiveDigis_*_*', 'keep *_simHcalDigis_*_*', 'keep ZDCDataFramesSorted_simHcalUnsuppressedDigis_*_*', 'keep *_simHcalTriggerPrimitiveDigis_*_*'))
)

process.FEVTDEBUGHLTEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'drop *', 'drop *', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep *_g4SimHits_*_*', 'keep edmHepMCProduct_source_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep EBSrFlagsSorted_simEcalDigis_*_*', 'keep EESrFlagsSorted_simEcalDigis_*_*', 'keep CrossingFramePlaybackInfoExtended_*_*_*', 'keep PileupSummaryInfos_*_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenJets_*_*_*', 'keep *_genParticle_*_*', 'keep recoGenMETs_*_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep *_MEtoEDMConverter_*_*', 'keep *_randomEngineStateProducer_*_*', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*')+cms.untracked.vstring('keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenMETs_*_*_*', 'keep *_kt4GenJets_*_*', 'keep *_kt6GenJets_*_*', 'keep *_ak5GenJets_*_*', 'keep *_ak7GenJets_*_*', 'keep *_iterativeCone5GenJets_*_*', 'keep *_genParticle_*_*', 'keep edmHepMCProduct_source_*_*', 'keep SimTracks_g4SimHits_*_*', 'keep SimVertexs_g4SimHits_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep PileupSummaryInfos_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_simCscTriggerPrimitiveDigis_*_*', 'keep *_simDtTriggerPrimitiveDigis_*_*', 'keep *_simRpcTriggerDigis_*_*', 'keep *_simRctDigis_*_*', 'keep *_simCsctfDigis_*_*', 'keep *_simCsctfTrackDigis_*_*', 'keep *_simDttfDigis_*_*', 'keep *_simGctDigis_*_*', 'keep *_simGmtDigis_*_*', 'keep *_simGtDigis_*_*', 'keep *_cscTriggerPrimitiveDigis_*_*', 'keep *_dtTriggerPrimitiveDigis_*_*', 'keep *_rpcTriggerDigis_*_*', 'keep *_rctDigis_*_*', 'keep *_csctfDigis_*_*', 'keep *_csctfTrackDigis_*_*', 'keep *_dttfDigis_*_*', 'keep *_gctDigis_*_*', 'keep *_gmtDigis_*_*', 'keep *_gtDigis_*_*', 'keep *_gtEvmDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_trackingtruthprod_*_*', 'drop *_electrontruth_*_*', 'keep *_mergedtruth_MergedTrackTruth_*', 'keep CrossingFramePlaybackInfoExtended_*_*_*', 'keep *_simSiPixelDigis_*_*', 'keep *_simSiStripDigis_*_*', 'keep *_allTrackMCMatch_*_*', 'keep *_trackingParticleRecoTrackAsssociation_*_*', 'keep *_assoc2secStepTk_*_*', 'keep *_assoc2thStepTk_*_*', 'keep *_assoc2GsfTracks_*_*', 'keep *_assocOutInConversionTracks_*_*', 'keep *_assocInOutConversionTracks_*_*', 'keep *_simMuonCSCDigis_*_*', 'keep *_simMuonDTDigis_*_*', 'keep *_simMuonRPCDigis_*_*', 'keep *_simEcalDigis_*_*', 'keep *_simEcalPreshowerDigis_*_*', 'keep *_simEcalTriggerPrimitiveDigis_*_*', 'keep *_simHcalDigis_*_*', 'keep ZDCDataFramesSorted_simHcalUnsuppressedDigis_*_*', 'keep *_simHcalTriggerPrimitiveDigis_*_*', 'drop *_hlt*_*_*', 'keep *_hltAlCaEtaRecHitsFilter_*_*', 'keep *_hltAlCaPhiSymStream_*_*', 'keep *_hltAlCaPi0RecHitsFilter_*_*', 'keep *_hltBLifetimeL25AssociatorStartupU_*_*', 'keep *_hltBLifetimeL25BJetTagsStartupU_*_*', 'keep *_hltBLifetimeL25JetsStartupU_*_*', 'keep *_hltBLifetimeL25TagInfosStartupU_*_*', 'keep *_hltBLifetimeL3AssociatorStartupU_*_*', 'keep *_hltBLifetimeL3BJetTagsStartupU_*_*', 'keep *_hltBLifetimeL3JetsStartupU_*_*', 'keep *_hltBLifetimeL3TagInfosStartupU_*_*', 'keep *_hltBLifetimeRegionalCtfWithMaterialTracksStartupU_*_*', 'keep *_hltBSoftMuonL25BJetTagsUByDR_*_*', 'keep *_hltBSoftMuonL25JetsU_*_*', 'keep *_hltBSoftMuonL25TagInfosU_*_*', 'keep *_hltBSoftMuonL3BJetTagsUByDR_*_*', 'keep *_hltBSoftMuonL3BJetTagsUByPt_*_*', 'keep *_hltBSoftMuonL3TagInfosU_*_*', 'keep *_hltCkfL1IsoLargeWindowTrackCandidates_*_*', 'keep *_hltCkfL1NonIsoLargeWindowTrackCandidates_*_*', 'keep *_hltCorrectedHybridSuperClustersL1Isolated_*_*', 'keep *_hltCorrectedHybridSuperClustersL1NonIsolated_*_*', 'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1Isolated_*_*', 'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1NonIsolated_*_*', 'keep *_hltCsc2DRecHits_*_*', 'keep *_hltCscSegments_*_*', 'keep *_hltCtfL1IsoLargeWindowWithMaterialTracks_*_*', 'keep *_hltCtfL1NonIsoLargeWindowWithMaterialTracks_*_*', 'keep *_hltDt1DRecHits_*_*', 'keep *_hltDt4DSegments_*_*', 'keep *_hltFilterL25LeadingTrackPtCutDoubleIsoTau15Trk5_*_*', 'keep *_hltFilterL25LeadingTrackPtCutSingleIsoTau30Trk5MET20_*_*', 'keep *_hltFilterL2EcalIsolationDoubleIsoTau15Trk5_*_*', 'keep *_hltFilterL2EcalIsolationDoubleLooseIsoTau15_*_*', 'keep *_hltFilterL2EcalIsolationSingleIsoTau30Trk5MET20_*_*', 'keep *_hltFilterL2EcalIsolationSingleLooseIsoTau20_*_*', 'keep *_hltFilterL2EtCutDoubleIsoTau15Trk5_*_*', 'keep *_hltFilterL2EtCutDoubleLooseIsoTau15_*_*', 'keep *_hltFilterL2EtCutSingleIsoTau30Trk5MET20_*_*', 'keep *_hltFilterL2EtCutSingleLooseIsoTau20_*_*', 'keep *_hltFilterL3TrackIsolationDoubleIsoTau15Trk5_*_*', 'keep *_hltFilterL3TrackIsolationSingleIsoTau30Trk5MET20_*_*', 'keep *_hltGctDigis_*_*', 'keep *_hltGtDigis_*_*', 'keep *_hltHITCtfWithMaterialTracksHB8E29_*_*', 'keep *_hltHITCtfWithMaterialTracksHE8E29_*_*', 'keep *_hltHITIPTCorrectorHB8E29_*_*', 'keep *_hltHITIPTCorrectorHE8E29_*_*', 'keep *_hltHcalDigis_*_*', 'keep *_hltHoreco_*_*', 'keep *_hltIconeCentral1Regional_*_*', 'keep *_hltIconeCentral2Regional_*_*', 'keep *_hltIconeCentral3Regional_*_*', 'keep *_hltIconeCentral4Regional_*_*', 'keep *_hltIconeTau1Regional_*_*', 'keep *_hltIconeTau2Regional_*_*', 'keep *_hltIconeTau3Regional_*_*', 'keep *_hltIconeTau4Regional_*_*', 'keep *_hltIsolPixelTrackProdHB8E29_*_*', 'keep *_hltIsolPixelTrackProdHE8E29_*_*', 'keep *_hltIterativeCone5CaloJets_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep *_hltL1IsoEgammaRegionalCTFFinalFitWithMaterial_*_*', 'keep *_hltL1IsoEgammaRegionalCkfTrackCandidates_*_*', 'keep *_hltL1IsoEgammaRegionalPixelSeedGenerator_*_*', 'keep *_hltL1IsoHLTClusterShape_*_*', 'keep *_hltL1IsoLargeWindowElectronPixelSeeds_*_*', 'keep *_hltL1IsoPhotonHollowTrackIsol_*_*', 'keep *_hltL1IsoRecoEcalCandidate_*_*', 'keep *_hltL1IsoSiStripElectronPixelSeeds_*_*', 'keep *_hltL1IsoStartUpElectronPixelSeeds_*_*', 'keep *_hltL1IsolatedElectronHcalIsol_*_*', 'keep *_hltL1IsolatedPhotonEcalIsol_*_*', 'keep *_hltL1IsolatedPhotonHcalIsol_*_*', 'keep *_hltL1NonIsoEgammaRegionalCTFFinalFitWithMaterial_*_*', 'keep *_hltL1NonIsoEgammaRegionalCkfTrackCandidates_*_*', 'keep *_hltL1NonIsoEgammaRegionalPixelSeedGenerator_*_*', 'keep *_hltL1NonIsoHLTClusterShape_*_*', 'keep *_hltL1NonIsoLargeWindowElectronPixelSeeds_*_*', 'keep *_hltL1NonIsoPhotonHollowTrackIsol_*_*', 'keep *_hltL1NonIsoRecoEcalCandidate_*_*', 'keep *_hltL1NonIsoSiStripElectronPixelSeeds_*_*', 'keep *_hltL1NonIsoStartUpElectronPixelSeeds_*_*', 'keep *_hltL1NonIsolatedElectronHcalIsol_*_*', 'keep *_hltL1NonIsolatedPhotonEcalIsol_*_*', 'keep *_hltL1NonIsolatedPhotonHcalIsol_*_*', 'keep *_hltL1extraParticles_*_*', 'keep *_hltL1sDoubleLooseIsoTau15_*_*', 'keep *_hltL1sSingleLooseIsoTau20_*_*', 'keep *_hltL25TauConeIsolation_*_*', 'keep *_hltL25TauCtfWithMaterialTracks_*_*', 'keep *_hltL25TauJetTracksAssociator_*_*', 'keep *_hltL25TauLeadingTrackPtCutSelector_*_*', 'keep *_hltL2MuonCandidatesNoVtx_*_*', 'keep *_hltL2MuonCandidates_*_*', 'keep *_hltL2MuonIsolations_*_*', 'keep *_hltL2MuonSeeds_*_*', 'keep *_hltL2Muons_*_*', 'keep *_hltL2TauJets_*_*', 'keep *_hltL2TauNarrowConeIsolationProducer_*_*', 'keep *_hltL2TauRelaxingIsolationSelector_*_*', 'keep *_hltL3MuonCandidatesNoVtx_*_*', 'keep *_hltL3MuonCandidates_*_*', 'keep *_hltL3MuonIsolations_*_*', 'keep *_hltL3MuonsIOHit_*_*', 'keep *_hltL3MuonsLinksCombination_*_*', 'keep *_hltL3MuonsNoVtx_*_*', 'keep *_hltL3MuonsOIHit_*_*', 'keep *_hltL3MuonsOIState_*_*', 'keep *_hltL3Muons_*_*', 'keep *_hltL3TauConeIsolation_*_*', 'keep *_hltL3TauCtfWithMaterialTracks_*_*', 'keep *_hltL3TauIsolationSelector_*_*', 'keep *_hltL3TauJetTracksAssociator_*_*')+cms.untracked.vstring('keep *_hltL3TkFromL2OICombination_*_*', 'keep *_hltL3TkTracksFromL2IOHit_*_*', 'keep *_hltL3TkTracksFromL2OIHit_*_*', 'keep *_hltL3TkTracksFromL2OIState_*_*', 'keep *_hltL3TkTracksFromL2_*_*', 'keep *_hltL3TrackCandidateFromL2IOHit_*_*', 'keep *_hltL3TrackCandidateFromL2OIHit_*_*', 'keep *_hltL3TrackCandidateFromL2OIState_*_*', 'keep *_hltL3TrackCandidateFromL2_*_*', 'keep *_hltL3TrajSeedIOHit_*_*', 'keep *_hltL3TrajSeedOIHit_*_*', 'keep *_hltL3TrajSeedOIState_*_*', 'keep *_hltL3TrajectorySeedNoVtx_*_*', 'keep *_hltL3TrajectorySeed_*_*', 'keep *_hltMCJetCorJetIcone5HF07_*_*', 'keep *_hltMet_*_*', 'keep *_hltMuTrackJpsiCtfTrackCands_*_*', 'keep *_hltMuTrackJpsiCtfTracks_*_*', 'keep *_hltMuTrackJpsiPixelTrackCands_*_*', 'keep *_hltMuTrackJpsiPixelTrackSelector_*_*', 'keep *_hltMuTrackJpsiTrackSeeds_*_*', 'keep *_hltMuonCSCDigis_*_*', 'keep *_hltMuonDTDigis_*_*', 'keep *_hltMuonRPCDigis_*_*', 'keep *_hltOfflineBeamSpot_*_*', 'keep *_hltPixelMatchElectronsL1Iso_*_*', 'keep *_hltPixelMatchElectronsL1NonIso_*_*', 'keep *_hltPixelMatchLargeWindowElectronsL1Iso_*_*', 'keep *_hltPixelMatchLargeWindowElectronsL1NonIso_*_*', 'keep *_hltPixelTracks_*_*', 'keep *_hltPixelVertices_*_*', 'keep *_hltRpcRecHits_*_*', 'keep *_hltSiPixelClusters_*_*', 'keep *_hltSiPixelRecHits_*_*', 'keep *_hltSiStripClusters_*_*', 'keep *_hltSiStripRawToClustersFacility_*_*', 'keep *_hltTowerMakerForAll_*_*', 'keep *_hltTowerMakerForMuons_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*', 'keep L1MuGMTCands_hltGtDigis_*_*', 'keep L1MuGMTReadoutCollection_hltGtDigis_*_*', 'keep SiPixelClusteredmNewDetSetVector_hltSiPixelClusters_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEventWithRefs_*_*_*', 'keep triggerTriggerEvent_*_*_*'))
)

process.FEVTEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*')+cms.untracked.vstring('keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_logErrorHarvester_*_*'))
)

process.FEVTHLTALLEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'drop *', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*')+cms.untracked.vstring('keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_logErrorHarvester_*_*', 'keep *_*_*_HLT'))
)

process.FEVTSIMEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep *_g4SimHits_*_*', 'keep edmHepMCProduct_source_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep EBSrFlagsSorted_simEcalDigis_*_*', 'keep EESrFlagsSorted_simEcalDigis_*_*', 'keep CrossingFramePlaybackInfoExtended_*_*_*', 'keep PileupSummaryInfos_*_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenJets_*_*_*', 'keep *_genParticle_*_*', 'keep recoGenMETs_*_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep *_MEtoEDMConverter_*_*', 'keep *_randomEngineStateProducer_*_*', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*')+cms.untracked.vstring('keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenMETs_*_*_*', 'keep *_kt4GenJets_*_*', 'keep *_kt6GenJets_*_*', 'keep *_ak5GenJets_*_*', 'keep *_ak7GenJets_*_*', 'keep *_iterativeCone5GenJets_*_*', 'keep *_genParticle_*_*', 'keep edmHepMCProduct_source_*_*', 'keep SimTracks_g4SimHits_*_*', 'keep SimVertexs_g4SimHits_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep PileupSummaryInfos_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*'))
)

process.FlatVtxSmearingParameters = cms.PSet(
    MaxZ = cms.double(5.3),
    MaxX = cms.double(0.0015),
    MaxY = cms.double(0.0015),
    MinX = cms.double(-0.0015),
    MinY = cms.double(-0.0015),
    MinZ = cms.double(-5.3),
    TimeOffset = cms.double(0.0)
)

process.GaussVtxSmearingParameters = cms.PSet(
    MeanX = cms.double(0.0),
    MeanY = cms.double(0.0),
    MeanZ = cms.double(0.0),
    SigmaY = cms.double(0.0015),
    SigmaX = cms.double(0.0015),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0)
)

process.GenJetParameters = cms.PSet(
    Active_Area_Repeats = cms.int32(5),
    src = cms.InputTag("genParticlesForJets"),
    doAreaFastjet = cms.bool(False),
    doPVCorrection = cms.bool(False),
    Ghost_EtaMax = cms.double(6.0),
    doRhoFastjet = cms.bool(False),
    srcPVs = cms.InputTag(""),
    inputEtMin = cms.double(0.0),
    doPUOffsetCorr = cms.bool(False),
    nSigmaPU = cms.double(1.0),
    radiusPU = cms.double(0.5),
    Rho_EtaMax = cms.double(4.5),
    jetPtMin = cms.double(3.0),
    jetType = cms.string('GenJet'),
    GhostArea = cms.double(0.01),
    inputEMin = cms.double(0.0)
)

process.GeneratorInterfaceAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*')
)

process.GeneratorInterfaceRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*')
)

process.GeneratorInterfaceRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*')
)

process.HLTDEBUGEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_logErrorHarvester_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_*_*',
        'keep *_hltAlCaPhiSymStream_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_*_*',
        'keep *_hltBLifetimeL25AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL25BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL25JetsStartupU_*_*',
        'keep *_hltBLifetimeL25TagInfosStartupU_*_*',
        'keep *_hltBLifetimeL3AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL3BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL3JetsStartupU_*_*',
        'keep *_hltBLifetimeL3TagInfosStartupU_*_*',
        'keep *_hltBLifetimeRegionalCtfWithMaterialTracksStartupU_*_*',
        'keep *_hltBSoftMuonL25BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL25JetsU_*_*',
        'keep *_hltBSoftMuonL25TagInfosU_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByPt_*_*',
        'keep *_hltBSoftMuonL3TagInfosU_*_*',
        'keep *_hltCkfL1IsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCkfL1NonIsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1Isolated_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1NonIsolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1Isolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1NonIsolated_*_*',
        'keep *_hltCsc2DRecHits_*_*',
        'keep *_hltCscSegments_*_*',
        'keep *_hltCtfL1IsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltCtfL1NonIsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltDt1DRecHits_*_*',
        'keep *_hltDt4DSegments_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EcalIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL2EtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EtCutDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EtCutSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL3TrackIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL3TrackIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltGctDigis_*_*',
        'keep *_hltGtDigis_*_*',
        'keep *_hltHITCtfWithMaterialTracksHB8E29_*_*',
        'keep *_hltHITCtfWithMaterialTracksHE8E29_*_*',
        'keep *_hltHITIPTCorrectorHB8E29_*_*',
        'keep *_hltHITIPTCorrectorHE8E29_*_*',
        'keep *_hltHcalDigis_*_*',
        'keep *_hltHoreco_*_*',
        'keep *_hltIconeCentral1Regional_*_*',
        'keep *_hltIconeCentral2Regional_*_*',
        'keep *_hltIconeCentral3Regional_*_*',
        'keep *_hltIconeCentral4Regional_*_*',
        'keep *_hltIconeTau1Regional_*_*',
        'keep *_hltIconeTau2Regional_*_*',
        'keep *_hltIconeTau3Regional_*_*',
        'keep *_hltIconeTau4Regional_*_*',
        'keep *_hltIsolPixelTrackProdHB8E29_*_*',
        'keep *_hltIsolPixelTrackProdHE8E29_*_*',
        'keep *_hltIterativeCone5CaloJets_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep *_hltL1IsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1IsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1IsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1IsoHLTClusterShape_*_*',
        'keep *_hltL1IsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1IsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1IsoRecoEcalCandidate_*_*',
        'keep *_hltL1IsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1IsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1IsolatedElectronHcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1NonIsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1NonIsoHLTClusterShape_*_*',
        'keep *_hltL1NonIsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1NonIsoRecoEcalCandidate_*_*',
        'keep *_hltL1NonIsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsolatedElectronHcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1extraParticles_*_*',
        'keep *_hltL1sDoubleLooseIsoTau15_*_*',
        'keep *_hltL1sSingleLooseIsoTau20_*_*',
        'keep *_hltL25TauConeIsolation_*_*',
        'keep *_hltL25TauCtfWithMaterialTracks_*_*',
        'keep *_hltL25TauJetTracksAssociator_*_*',
        'keep *_hltL25TauLeadingTrackPtCutSelector_*_*',
        'keep *_hltL2MuonCandidatesNoVtx_*_*',
        'keep *_hltL2MuonCandidates_*_*',
        'keep *_hltL2MuonIsolations_*_*',
        'keep *_hltL2MuonSeeds_*_*',
        'keep *_hltL2Muons_*_*',
        'keep *_hltL2TauJets_*_*',
        'keep *_hltL2TauNarrowConeIsolationProducer_*_*',
        'keep *_hltL2TauRelaxingIsolationSelector_*_*',
        'keep *_hltL3MuonCandidatesNoVtx_*_*',
        'keep *_hltL3MuonCandidates_*_*',
        'keep *_hltL3MuonIsolations_*_*',
        'keep *_hltL3MuonsIOHit_*_*',
        'keep *_hltL3MuonsLinksCombination_*_*',
        'keep *_hltL3MuonsNoVtx_*_*',
        'keep *_hltL3MuonsOIHit_*_*',
        'keep *_hltL3MuonsOIState_*_*',
        'keep *_hltL3Muons_*_*',
        'keep *_hltL3TauConeIsolation_*_*',
        'keep *_hltL3TauCtfWithMaterialTracks_*_*',
        'keep *_hltL3TauIsolationSelector_*_*',
        'keep *_hltL3TauJetTracksAssociator_*_*',
        'keep *_hltL3TkFromL2OICombination_*_*',
        'keep *_hltL3TkTracksFromL2IOHit_*_*',
        'keep *_hltL3TkTracksFromL2OIHit_*_*',
        'keep *_hltL3TkTracksFromL2OIState_*_*',
        'keep *_hltL3TkTracksFromL2_*_*',
        'keep *_hltL3TrackCandidateFromL2IOHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIState_*_*',
        'keep *_hltL3TrackCandidateFromL2_*_*',
        'keep *_hltL3TrajSeedIOHit_*_*',
        'keep *_hltL3TrajSeedOIHit_*_*',
        'keep *_hltL3TrajSeedOIState_*_*',
        'keep *_hltL3TrajectorySeedNoVtx_*_*',
        'keep *_hltL3TrajectorySeed_*_*',
        'keep *_hltMCJetCorJetIcone5HF07_*_*',
        'keep *_hltMet_*_*',
        'keep *_hltMuTrackJpsiCtfTrackCands_*_*',
        'keep *_hltMuTrackJpsiCtfTracks_*_*',
        'keep *_hltMuTrackJpsiPixelTrackCands_*_*',
        'keep *_hltMuTrackJpsiPixelTrackSelector_*_*',
        'keep *_hltMuTrackJpsiTrackSeeds_*_*',
        'keep *_hltMuonCSCDigis_*_*',
        'keep *_hltMuonDTDigis_*_*',
        'keep *_hltMuonRPCDigis_*_*',
        'keep *_hltOfflineBeamSpot_*_*',
        'keep *_hltPixelMatchElectronsL1Iso_*_*',
        'keep *_hltPixelMatchElectronsL1NonIso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1Iso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1NonIso_*_*',
        'keep *_hltPixelTracks_*_*',
        'keep *_hltPixelVertices_*_*',
        'keep *_hltRpcRecHits_*_*',
        'keep *_hltSiPixelClusters_*_*',
        'keep *_hltSiPixelRecHits_*_*',
        'keep *_hltSiStripClusters_*_*',
        'keep *_hltSiStripRawToClustersFacility_*_*',
        'keep *_hltTowerMakerForAll_*_*',
        'keep *_hltTowerMakerForMuons_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep L1MuGMTCands_hltGtDigis_*_*',
        'keep L1MuGMTReadoutCollection_hltGtDigis_*_*',
        'keep SiPixelClusteredmNewDetSetVector_hltSiPixelClusters_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEventWithRefs_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.HLTDebugFEVT = cms.PSet(
    outputCommands = cms.vstring('drop *_hlt*_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_*_*',
        'keep *_hltAlCaPhiSymStream_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_*_*',
        'keep *_hltBLifetimeL25AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL25BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL25JetsStartupU_*_*',
        'keep *_hltBLifetimeL25TagInfosStartupU_*_*',
        'keep *_hltBLifetimeL3AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL3BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL3JetsStartupU_*_*',
        'keep *_hltBLifetimeL3TagInfosStartupU_*_*',
        'keep *_hltBLifetimeRegionalCtfWithMaterialTracksStartupU_*_*',
        'keep *_hltBSoftMuonL25BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL25JetsU_*_*',
        'keep *_hltBSoftMuonL25TagInfosU_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByPt_*_*',
        'keep *_hltBSoftMuonL3TagInfosU_*_*',
        'keep *_hltCkfL1IsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCkfL1NonIsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1Isolated_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1NonIsolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1Isolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1NonIsolated_*_*',
        'keep *_hltCsc2DRecHits_*_*',
        'keep *_hltCscSegments_*_*',
        'keep *_hltCtfL1IsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltCtfL1NonIsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltDt1DRecHits_*_*',
        'keep *_hltDt4DSegments_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EcalIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL2EtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EtCutDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EtCutSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL3TrackIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL3TrackIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltGctDigis_*_*',
        'keep *_hltGtDigis_*_*',
        'keep *_hltHITCtfWithMaterialTracksHB8E29_*_*',
        'keep *_hltHITCtfWithMaterialTracksHE8E29_*_*',
        'keep *_hltHITIPTCorrectorHB8E29_*_*',
        'keep *_hltHITIPTCorrectorHE8E29_*_*',
        'keep *_hltHcalDigis_*_*',
        'keep *_hltHoreco_*_*',
        'keep *_hltIconeCentral1Regional_*_*',
        'keep *_hltIconeCentral2Regional_*_*',
        'keep *_hltIconeCentral3Regional_*_*',
        'keep *_hltIconeCentral4Regional_*_*',
        'keep *_hltIconeTau1Regional_*_*',
        'keep *_hltIconeTau2Regional_*_*',
        'keep *_hltIconeTau3Regional_*_*',
        'keep *_hltIconeTau4Regional_*_*',
        'keep *_hltIsolPixelTrackProdHB8E29_*_*',
        'keep *_hltIsolPixelTrackProdHE8E29_*_*',
        'keep *_hltIterativeCone5CaloJets_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep *_hltL1IsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1IsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1IsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1IsoHLTClusterShape_*_*',
        'keep *_hltL1IsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1IsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1IsoRecoEcalCandidate_*_*',
        'keep *_hltL1IsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1IsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1IsolatedElectronHcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1NonIsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1NonIsoHLTClusterShape_*_*',
        'keep *_hltL1NonIsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1NonIsoRecoEcalCandidate_*_*',
        'keep *_hltL1NonIsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsolatedElectronHcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1extraParticles_*_*',
        'keep *_hltL1sDoubleLooseIsoTau15_*_*',
        'keep *_hltL1sSingleLooseIsoTau20_*_*',
        'keep *_hltL25TauConeIsolation_*_*',
        'keep *_hltL25TauCtfWithMaterialTracks_*_*',
        'keep *_hltL25TauJetTracksAssociator_*_*',
        'keep *_hltL25TauLeadingTrackPtCutSelector_*_*',
        'keep *_hltL2MuonCandidatesNoVtx_*_*',
        'keep *_hltL2MuonCandidates_*_*',
        'keep *_hltL2MuonIsolations_*_*',
        'keep *_hltL2MuonSeeds_*_*',
        'keep *_hltL2Muons_*_*',
        'keep *_hltL2TauJets_*_*',
        'keep *_hltL2TauNarrowConeIsolationProducer_*_*',
        'keep *_hltL2TauRelaxingIsolationSelector_*_*',
        'keep *_hltL3MuonCandidatesNoVtx_*_*',
        'keep *_hltL3MuonCandidates_*_*',
        'keep *_hltL3MuonIsolations_*_*',
        'keep *_hltL3MuonsIOHit_*_*',
        'keep *_hltL3MuonsLinksCombination_*_*',
        'keep *_hltL3MuonsNoVtx_*_*',
        'keep *_hltL3MuonsOIHit_*_*',
        'keep *_hltL3MuonsOIState_*_*',
        'keep *_hltL3Muons_*_*',
        'keep *_hltL3TauConeIsolation_*_*',
        'keep *_hltL3TauCtfWithMaterialTracks_*_*',
        'keep *_hltL3TauIsolationSelector_*_*',
        'keep *_hltL3TauJetTracksAssociator_*_*',
        'keep *_hltL3TkFromL2OICombination_*_*',
        'keep *_hltL3TkTracksFromL2IOHit_*_*',
        'keep *_hltL3TkTracksFromL2OIHit_*_*',
        'keep *_hltL3TkTracksFromL2OIState_*_*',
        'keep *_hltL3TkTracksFromL2_*_*',
        'keep *_hltL3TrackCandidateFromL2IOHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIState_*_*',
        'keep *_hltL3TrackCandidateFromL2_*_*',
        'keep *_hltL3TrajSeedIOHit_*_*',
        'keep *_hltL3TrajSeedOIHit_*_*',
        'keep *_hltL3TrajSeedOIState_*_*',
        'keep *_hltL3TrajectorySeedNoVtx_*_*',
        'keep *_hltL3TrajectorySeed_*_*',
        'keep *_hltMCJetCorJetIcone5HF07_*_*',
        'keep *_hltMet_*_*',
        'keep *_hltMuTrackJpsiCtfTrackCands_*_*',
        'keep *_hltMuTrackJpsiCtfTracks_*_*',
        'keep *_hltMuTrackJpsiPixelTrackCands_*_*',
        'keep *_hltMuTrackJpsiPixelTrackSelector_*_*',
        'keep *_hltMuTrackJpsiTrackSeeds_*_*',
        'keep *_hltMuonCSCDigis_*_*',
        'keep *_hltMuonDTDigis_*_*',
        'keep *_hltMuonRPCDigis_*_*',
        'keep *_hltOfflineBeamSpot_*_*',
        'keep *_hltPixelMatchElectronsL1Iso_*_*',
        'keep *_hltPixelMatchElectronsL1NonIso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1Iso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1NonIso_*_*',
        'keep *_hltPixelTracks_*_*',
        'keep *_hltPixelVertices_*_*',
        'keep *_hltRpcRecHits_*_*',
        'keep *_hltSiPixelClusters_*_*',
        'keep *_hltSiPixelRecHits_*_*',
        'keep *_hltSiStripClusters_*_*',
        'keep *_hltSiStripRawToClustersFacility_*_*',
        'keep *_hltTowerMakerForAll_*_*',
        'keep *_hltTowerMakerForMuons_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep L1MuGMTCands_hltGtDigis_*_*',
        'keep L1MuGMTReadoutCollection_hltGtDigis_*_*',
        'keep SiPixelClusteredmNewDetSetVector_hltSiPixelClusters_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEventWithRefs_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.HLTDebugRAW = cms.PSet(
    outputCommands = cms.vstring('drop *_hlt*_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_*_*',
        'keep *_hltAlCaPhiSymStream_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_*_*',
        'keep *_hltBLifetimeL25AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL25BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL25JetsStartupU_*_*',
        'keep *_hltBLifetimeL25TagInfosStartupU_*_*',
        'keep *_hltBLifetimeL3AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL3BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL3JetsStartupU_*_*',
        'keep *_hltBLifetimeL3TagInfosStartupU_*_*',
        'keep *_hltBLifetimeRegionalCtfWithMaterialTracksStartupU_*_*',
        'keep *_hltBSoftMuonL25BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL25JetsU_*_*',
        'keep *_hltBSoftMuonL25TagInfosU_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByPt_*_*',
        'keep *_hltBSoftMuonL3TagInfosU_*_*',
        'keep *_hltCkfL1IsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCkfL1NonIsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1Isolated_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1NonIsolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1Isolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1NonIsolated_*_*',
        'keep *_hltCsc2DRecHits_*_*',
        'keep *_hltCscSegments_*_*',
        'keep *_hltCtfL1IsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltCtfL1NonIsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltDt1DRecHits_*_*',
        'keep *_hltDt4DSegments_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EcalIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL2EtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EtCutDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EtCutSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL3TrackIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL3TrackIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltGctDigis_*_*',
        'keep *_hltGtDigis_*_*',
        'keep *_hltHITCtfWithMaterialTracksHB8E29_*_*',
        'keep *_hltHITCtfWithMaterialTracksHE8E29_*_*',
        'keep *_hltHITIPTCorrectorHB8E29_*_*',
        'keep *_hltHITIPTCorrectorHE8E29_*_*',
        'keep *_hltHcalDigis_*_*',
        'keep *_hltHoreco_*_*',
        'keep *_hltIconeCentral1Regional_*_*',
        'keep *_hltIconeCentral2Regional_*_*',
        'keep *_hltIconeCentral3Regional_*_*',
        'keep *_hltIconeCentral4Regional_*_*',
        'keep *_hltIconeTau1Regional_*_*',
        'keep *_hltIconeTau2Regional_*_*',
        'keep *_hltIconeTau3Regional_*_*',
        'keep *_hltIconeTau4Regional_*_*',
        'keep *_hltIsolPixelTrackProdHB8E29_*_*',
        'keep *_hltIsolPixelTrackProdHE8E29_*_*',
        'keep *_hltIterativeCone5CaloJets_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep *_hltL1IsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1IsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1IsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1IsoHLTClusterShape_*_*',
        'keep *_hltL1IsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1IsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1IsoRecoEcalCandidate_*_*',
        'keep *_hltL1IsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1IsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1IsolatedElectronHcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1NonIsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1NonIsoHLTClusterShape_*_*',
        'keep *_hltL1NonIsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1NonIsoRecoEcalCandidate_*_*',
        'keep *_hltL1NonIsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsolatedElectronHcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1extraParticles_*_*',
        'keep *_hltL1sDoubleLooseIsoTau15_*_*',
        'keep *_hltL1sSingleLooseIsoTau20_*_*',
        'keep *_hltL25TauConeIsolation_*_*',
        'keep *_hltL25TauCtfWithMaterialTracks_*_*',
        'keep *_hltL25TauJetTracksAssociator_*_*',
        'keep *_hltL25TauLeadingTrackPtCutSelector_*_*',
        'keep *_hltL2MuonCandidatesNoVtx_*_*',
        'keep *_hltL2MuonCandidates_*_*',
        'keep *_hltL2MuonIsolations_*_*',
        'keep *_hltL2MuonSeeds_*_*',
        'keep *_hltL2Muons_*_*',
        'keep *_hltL2TauJets_*_*',
        'keep *_hltL2TauNarrowConeIsolationProducer_*_*',
        'keep *_hltL2TauRelaxingIsolationSelector_*_*',
        'keep *_hltL3MuonCandidatesNoVtx_*_*',
        'keep *_hltL3MuonCandidates_*_*',
        'keep *_hltL3MuonIsolations_*_*',
        'keep *_hltL3MuonsIOHit_*_*',
        'keep *_hltL3MuonsLinksCombination_*_*',
        'keep *_hltL3MuonsNoVtx_*_*',
        'keep *_hltL3MuonsOIHit_*_*',
        'keep *_hltL3MuonsOIState_*_*',
        'keep *_hltL3Muons_*_*',
        'keep *_hltL3TauConeIsolation_*_*',
        'keep *_hltL3TauCtfWithMaterialTracks_*_*',
        'keep *_hltL3TauIsolationSelector_*_*',
        'keep *_hltL3TauJetTracksAssociator_*_*',
        'keep *_hltL3TkFromL2OICombination_*_*',
        'keep *_hltL3TkTracksFromL2IOHit_*_*',
        'keep *_hltL3TkTracksFromL2OIHit_*_*',
        'keep *_hltL3TkTracksFromL2OIState_*_*',
        'keep *_hltL3TkTracksFromL2_*_*',
        'keep *_hltL3TrackCandidateFromL2IOHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIState_*_*',
        'keep *_hltL3TrackCandidateFromL2_*_*',
        'keep *_hltL3TrajSeedIOHit_*_*',
        'keep *_hltL3TrajSeedOIHit_*_*',
        'keep *_hltL3TrajSeedOIState_*_*',
        'keep *_hltL3TrajectorySeedNoVtx_*_*',
        'keep *_hltL3TrajectorySeed_*_*',
        'keep *_hltMCJetCorJetIcone5HF07_*_*',
        'keep *_hltMet_*_*',
        'keep *_hltMuTrackJpsiCtfTrackCands_*_*',
        'keep *_hltMuTrackJpsiCtfTracks_*_*',
        'keep *_hltMuTrackJpsiPixelTrackCands_*_*',
        'keep *_hltMuTrackJpsiPixelTrackSelector_*_*',
        'keep *_hltMuTrackJpsiTrackSeeds_*_*',
        'keep *_hltMuonCSCDigis_*_*',
        'keep *_hltMuonDTDigis_*_*',
        'keep *_hltMuonRPCDigis_*_*',
        'keep *_hltOfflineBeamSpot_*_*',
        'keep *_hltPixelMatchElectronsL1Iso_*_*',
        'keep *_hltPixelMatchElectronsL1NonIso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1Iso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1NonIso_*_*',
        'keep *_hltPixelTracks_*_*',
        'keep *_hltPixelVertices_*_*',
        'keep *_hltRpcRecHits_*_*',
        'keep *_hltSiPixelClusters_*_*',
        'keep *_hltSiPixelRecHits_*_*',
        'keep *_hltSiStripClusters_*_*',
        'keep *_hltSiStripRawToClustersFacility_*_*',
        'keep *_hltTowerMakerForAll_*_*',
        'keep *_hltTowerMakerForMuons_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep L1MuGMTCands_hltGtDigis_*_*',
        'keep L1MuGMTReadoutCollection_hltGtDigis_*_*',
        'keep SiPixelClusteredmNewDetSetVector_hltSiPixelClusters_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEventWithRefs_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.HLTriggerAOD = cms.PSet(
    outputCommands = cms.vstring('drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.HLTriggerRAW = cms.PSet(
    outputCommands = cms.vstring('drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.HLTriggerRECO = cms.PSet(
    outputCommands = cms.vstring('drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.HectorEtaCut = cms.PSet(
    EtaCutForHector = cms.double(8.2)
)

process.IOMCRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_randomEngineStateProducer_*_*')
)

process.L1TriggerAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*',
        'keep LumiSummary_lumiProducer_*_*')
)

process.L1TriggerFEVTDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_simCscTriggerPrimitiveDigis_*_*',
        'keep *_simDtTriggerPrimitiveDigis_*_*',
        'keep *_simRpcTriggerDigis_*_*',
        'keep *_simRctDigis_*_*',
        'keep *_simCsctfDigis_*_*',
        'keep *_simCsctfTrackDigis_*_*',
        'keep *_simDttfDigis_*_*',
        'keep *_simGctDigis_*_*',
        'keep *_simGmtDigis_*_*',
        'keep *_simGtDigis_*_*',
        'keep *_cscTriggerPrimitiveDigis_*_*',
        'keep *_dtTriggerPrimitiveDigis_*_*',
        'keep *_rpcTriggerDigis_*_*',
        'keep *_rctDigis_*_*',
        'keep *_csctfDigis_*_*',
        'keep *_csctfTrackDigis_*_*',
        'keep *_dttfDigis_*_*',
        'keep *_gctDigis_*_*',
        'keep *_gmtDigis_*_*',
        'keep *_gtDigis_*_*',
        'keep *_gtEvmDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*',
        'keep LumiDetails_lumiProducer_*_*',
        'keep LumiSummary_lumiProducer_*_*')
)

process.L1TriggerRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*')
)

process.L1TriggerRAWDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*')
)

process.L1TriggerRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*',
        'keep L1MuGMTReadoutCollection_gtDigis_*_*',
        'keep L1GctEmCand*_gctDigis_*_*',
        'keep L1GctJetCand*_gctDigis_*_*',
        'keep L1GctEtHad*_gctDigis_*_*',
        'keep L1GctEtMiss*_gctDigis_*_*',
        'keep L1GctEtTotal*_gctDigis_*_*',
        'keep L1GctHtMiss*_gctDigis_*_*',
        'keep L1GctJetCounts*_gctDigis_*_*',
        'keep L1GctHFRingEtSums*_gctDigis_*_*',
        'keep L1GctHFBitCounts*_gctDigis_*_*',
        'keep LumiDetails_lumiProducer_*_*',
        'keep LumiSummary_lumiProducer_*_*')
)

process.MEtoEDMConverterAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.MEtoEDMConverterFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_MEtoEDMConverter_*_*')
)

process.MEtoEDMConverterRECO = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.MIXINGMODULEEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_cfWriter_*_*')
)

process.Nominal7TeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(200.0),
    Emittance = cms.double(2e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(4.2),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.0322),
    Z0 = cms.double(0.0)
)

process.NominalCollision1VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(55.0),
    Emittance = cms.double(1.006e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.025),
    X0 = cms.double(0.05),
    Z0 = cms.double(0.0)
)

process.NominalCollision2VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.000142),
    BetaStar = cms.double(55.0),
    Emittance = cms.double(1.006e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.025),
    X0 = cms.double(0.05),
    Z0 = cms.double(0.0)
)

process.NominalCollision3VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(55.0),
    Emittance = cms.double(1.006e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.025),
    X0 = cms.double(0.1),
    Z0 = cms.double(0.0)
)

process.NominalCollision4VtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(55.0),
    Emittance = cms.double(1.006e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.025),
    X0 = cms.double(0.2),
    Z0 = cms.double(0.0)
)

process.NominalCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.000142),
    BetaStar = cms.double(55.0),
    Emittance = cms.double(1.006e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.3),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.0),
    X0 = cms.double(0.05),
    Z0 = cms.double(0.0)
)

process.OutALCARECODtCalib = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECODtCalib')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_dt4DSegments_*_*',
        'keep *_dt4DSegmentsNoWire_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_dttfDigis_*_*',
        'keep *_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*')
)

process.OutALCARECODtCalib_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECODtCalib')
    ),
    outputCommands = cms.untracked.vstring('keep *_dt4DSegments_*_*',
        'keep *_dt4DSegmentsNoWire_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_dttfDigis_*_*',
        'keep *_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*')
)

process.OutALCARECOEcalCalElectron = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalElectron')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep recoGsfElectronCores_*_*_*',
        'keep recoSuperClusters_*_*_*',
        'keep *_electronGsfTracks_*_*',
        'keep  *_gsfElectrons_*_*',
        'keep  *_alCaIsolatedElectrons_*_*',
        'keep recoCaloMETs_met_*_*',
        'keep edmTriggerResults_TriggerResults__*',
        'keep edmHepMCProduct_*_*_*')
)

process.OutALCARECOEcalCalElectron_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalElectron')
    ),
    outputCommands = cms.untracked.vstring('keep recoGsfElectronCores_*_*_*',
        'keep recoSuperClusters_*_*_*',
        'keep *_electronGsfTracks_*_*',
        'keep  *_gsfElectrons_*_*',
        'keep  *_alCaIsolatedElectrons_*_*',
        'keep recoCaloMETs_met_*_*',
        'keep edmTriggerResults_TriggerResults__*',
        'keep edmHepMCProduct_*_*_*')
)

process.OutALCARECOEcalCalEtaCalib = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalEtaCalib')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ecalEtaCorrected_etaEcalRecHitsEB_*',
        'keep *_ecalEtaCorrected_etaEcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_etaEcalRecHitsES_*')
)

process.OutALCARECOEcalCalEtaCalib_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalEtaCalib')
    ),
    outputCommands = cms.untracked.vstring('keep *_ecalEtaCorrected_etaEcalRecHitsEB_*',
        'keep *_ecalEtaCorrected_etaEcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_etaEcalRecHitsES_*')
)

process.OutALCARECOEcalCalPhiSym = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalPhiSym')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ecalPhiSymCorrected_phiSymEcalRecHitsEB_*',
        'keep *_ecalPhiSymCorrected_phiSymEcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*')
)

process.OutALCARECOEcalCalPhiSym_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalPhiSym')
    ),
    outputCommands = cms.untracked.vstring('keep *_ecalPhiSymCorrected_phiSymEcalRecHitsEB_*',
        'keep *_ecalPhiSymCorrected_phiSymEcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*')
)

process.OutALCARECOEcalCalPi0Calib = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalPi0Calib')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ecalPi0Corrected_pi0EcalRecHitsEB_*',
        'keep *_ecalPi0Corrected_pi0EcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_pi0EcalRecHitsES_*')
)

process.OutALCARECOEcalCalPi0Calib_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOEcalCalPi0Calib')
    ),
    outputCommands = cms.untracked.vstring('keep *_ecalPi0Corrected_pi0EcalRecHitsEB_*',
        'keep *_ecalPi0Corrected_pi0EcalRecHitsEE_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_pi0EcalRecHitsES_*')
)

process.OutALCARECOHcalCalDijets = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalDijets')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_DiJProd_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.OutALCARECOHcalCalDijets_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalDijets')
    ),
    outputCommands = cms.untracked.vstring('keep *_DiJProd_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.OutALCARECOHcalCalGammaJet = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalGammaJet')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_GammaJetProd_*_*')
)

process.OutALCARECOHcalCalGammaJet_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalGammaJet')
    ),
    outputCommands = cms.untracked.vstring('keep *_GammaJetProd_*_*')
)

process.OutALCARECOHcalCalHO = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalHO')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_hoCalibProducer_*_*')
)

process.OutALCARECOHcalCalHOCosmics = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalHOCosmics')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep HOCalibVariabless_*_*_*')
)

process.OutALCARECOHcalCalHOCosmics_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalHOCosmics')
    ),
    outputCommands = cms.untracked.vstring('keep HOCalibVariabless_*_*_*')
)

process.OutALCARECOHcalCalHO_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalHO')
    ),
    outputCommands = cms.untracked.vstring('keep *_hoCalibProducer_*_*')
)

process.OutALCARECOHcalCalIsoTrk = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalIsoTrk')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_IsoProd_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.OutALCARECOHcalCalIsoTrkNoHLT = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalIsoTrkNoHLT')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_IsoProd_*_*')
)

process.OutALCARECOHcalCalIsoTrkNoHLT_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalIsoTrkNoHLT')
    ),
    outputCommands = cms.untracked.vstring('keep *_IsoProd_*_*')
)

process.OutALCARECOHcalCalIsoTrk_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalIsoTrk')
    ),
    outputCommands = cms.untracked.vstring('keep *_IsoProd_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.OutALCARECOHcalCalMinBias = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalMinBias')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_gtDigisAlCaMB_*_*',
        'keep HBHERecHitsSorted_hbherecoMB_*_*',
        'keep HORecHitsSorted_horecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMBspecial_*_*',
        'keep HBHERecHitsSorted_hbherecoNoise_*_*',
        'keep HORecHitsSorted_horecoNoise_*_*',
        'keep HFRecHitsSorted_hfrecoNoise_*_*')
)

process.OutALCARECOHcalCalMinBiasHI = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalMinBias')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_gtDigisAlCaMB_*_*',
        'keep HBHERecHitsSorted_hbhereco_*_*',
        'keep HORecHitsSorted_horeco_*_*',
        'keep HFRecHitsSorted_hfreco_*_*',
        'keep HFRecHitsSorted_hfrecoMBspecial_*_*',
        'keep HBHERecHitsSorted_hbherecoNoise_*_*',
        'keep HORecHitsSorted_horecoNoise_*_*',
        'keep HFRecHitsSorted_hfrecoNoise_*_*')
)

process.OutALCARECOHcalCalMinBiasHI_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalMinBias')
    ),
    outputCommands = cms.untracked.vstring('keep *_gtDigisAlCaMB_*_*',
        'keep HBHERecHitsSorted_hbhereco_*_*',
        'keep HORecHitsSorted_horeco_*_*',
        'keep HFRecHitsSorted_hfreco_*_*',
        'keep HFRecHitsSorted_hfrecoMBspecial_*_*',
        'keep HBHERecHitsSorted_hbherecoNoise_*_*',
        'keep HORecHitsSorted_horecoNoise_*_*',
        'keep HFRecHitsSorted_hfrecoNoise_*_*')
)

process.OutALCARECOHcalCalMinBias_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalMinBias')
    ),
    outputCommands = cms.untracked.vstring('keep *_gtDigisAlCaMB_*_*',
        'keep HBHERecHitsSorted_hbherecoMB_*_*',
        'keep HORecHitsSorted_horecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMBspecial_*_*',
        'keep HBHERecHitsSorted_hbherecoNoise_*_*',
        'keep HORecHitsSorted_horecoNoise_*_*',
        'keep HFRecHitsSorted_hfrecoNoise_*_*')
)

process.OutALCARECOHcalCalNoise = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalNoise')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_HcalNoiseProd_*_*',
        'keep edmTriggerResults_*_*_HLT')
)

process.OutALCARECOHcalCalNoise_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOHcalCalNoise')
    ),
    outputCommands = cms.untracked.vstring('keep *_HcalNoiseProd_*_*',
        'keep edmTriggerResults_*_*_HLT')
)

process.OutALCARECOMuAlBeamHalo = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlBeamHalo')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlBeamHalo_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlBeamHaloOverlaps = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlBeamHaloOverlaps')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlBeamHaloOverlaps_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlBeamHaloOverlaps_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlBeamHaloOverlaps')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlBeamHaloOverlaps_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlBeamHalo_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlBeamHalo')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlBeamHalo_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlCalIsolatedMu = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlCalIsolatedMu')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlCalIsolatedMu_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlCalIsolatedMu_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlCalIsolatedMu')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlCalIsolatedMu_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlGlobalCosmics = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlGlobalCosmics')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlGlobalCosmics_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlGlobalCosmicsInCollisions = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlGlobalCosmicsInCollisions')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlGlobalCosmicsInCollisions_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlGlobalCosmicsInCollisions_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlGlobalCosmicsInCollisions')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlGlobalCosmicsInCollisions_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlGlobalCosmics_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlGlobalCosmics')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlGlobalCosmics_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlOverlaps = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlOverlaps')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlOverlaps_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlOverlaps_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlOverlaps')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlOverlaps_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlStandAloneCosmics = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlStandAloneCosmics')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlStandAloneCosmics_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlStandAloneCosmics_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlStandAloneCosmics')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlStandAloneCosmics_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlZMuMu = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlZMuMu')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOMuAlZMuMu_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOMuAlZMuMu_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOMuAlZMuMu')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOMuAlZMuMu_*_*',
        'keep *_muonCSCDigis_*_*',
        'keep *_muonDTDigis_*_*',
        'keep *_muonRPCDigis_*_*',
        'keep *_dt1DRecHits_*_*',
        'keep *_dt2DSegments_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOPromptCalibProd = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOPromptCalibProd')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_alcaBeamSpotProducer_*_*')
)

process.OutALCARECOPromptCalibProd_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOPromptCalibProd')
    ),
    outputCommands = cms.untracked.vstring('keep *_alcaBeamSpotProducer_*_*')
)

process.OutALCARECORpcCalHLT = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECORpcCalHLT')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_muonDTDigis_*_*',
        'keep CSCDetIdCSCWireDigiMuonDigiCollection_*_*_*',
        'keep CSCDetIdCSCStripDigiMuonDigiCollection_*_*_*',
        'keep DTLayerIdDTDigiMuonDigiCollection_*_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep RPCDetIdRPCDigiMuonDigiCollection_*_*_*',
        'keep recoMuons_muonsNoRPC_*_*',
        'keep L1MuRegionalCands_*_RPCb_*',
        'keep L1MuRegionalCands_*_RPCf_*',
        'keep L1MuGMTCands_*_*_*',
        'keep L1MuGMTReadoutCollection_*_*_*')
)

process.OutALCARECORpcCalHLT_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECORpcCalHLT')
    ),
    outputCommands = cms.untracked.vstring('keep *_muonDTDigis_*_*',
        'keep CSCDetIdCSCWireDigiMuonDigiCollection_*_*_*',
        'keep CSCDetIdCSCStripDigiMuonDigiCollection_*_*_*',
        'keep DTLayerIdDTDigiMuonDigiCollection_*_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*',
        'keep RPCDetIdRPCDigiMuonDigiCollection_*_*_*',
        'keep recoMuons_muonsNoRPC_*_*',
        'keep L1MuRegionalCands_*_RPCb_*',
        'keep L1MuRegionalCands_*_RPCf_*',
        'keep L1MuGMTCands_*_*_*',
        'keep L1MuGMTReadoutCollection_*_*_*')
)

process.OutALCARECOSiPixelLorentzAngle = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOSiPixelLorentzAngle')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_globalMuons_*_*',
        'keep *_siStripClusters_*_*',
        'keep *_siPixelClusters_*_*',
        'drop *_*_*_HLT')
)

process.OutALCARECOSiPixelLorentzAngle_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOSiPixelLorentzAngle')
    ),
    outputCommands = cms.untracked.vstring('keep *_globalMuons_*_*',
        'keep *_siStripClusters_*_*',
        'keep *_siPixelClusters_*_*',
        'drop *_*_*_HLT')
)

process.OutALCARECOSiStripCalMinBias = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOSiStripCalMinBias')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOSiStripCalMinBias_*_*',
        'keep *_siStripClusters_*_*',
        'keep *_siPixelClusters_*_*',
        'keep DetIdedmEDCollection_siStripDigis_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*')
)

process.OutALCARECOSiStripCalMinBias_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOSiStripCalMinBias')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOSiStripCalMinBias_*_*',
        'keep *_siStripClusters_*_*',
        'keep *_siPixelClusters_*_*',
        'keep DetIdedmEDCollection_siStripDigis_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*')
)

process.OutALCARECOSiStripCalZeroBias = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOSiStripCalZeroBias')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOSiStripCalZeroBias_*_*',
        'keep *_calZeroBiasClusters_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep *_TriggerResults_*_*')
)

process.OutALCARECOSiStripCalZeroBias_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOSiStripCalZeroBias')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOSiStripCalZeroBias_*_*',
        'keep *_calZeroBiasClusters_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep *_TriggerResults_*_*')
)

process.OutALCARECOTkAlBeamHalo = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlBeamHalo')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlBeamHalo_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlBeamHalo_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlBeamHalo')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlBeamHalo_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlCosmics = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTF',
            'pathALCARECOTkAlCosmicsCosmicTF')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlCosmicsCTF_*_*',
        'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmics0T = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTF0T',
            'pathALCARECOTkAlCosmicsCosmicTF0T')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlCosmics*0T_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmics0THLT = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTF0THLT',
            'pathALCARECOTkAlCosmicsCosmicTF0THLT')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlCosmics*0T_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmics0THLT_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTF0THLT',
            'pathALCARECOTkAlCosmicsCosmicTF0THLT')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlCosmics*0T_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmics0T_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTF0T',
            'pathALCARECOTkAlCosmicsCosmicTF0T')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlCosmics*0T_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmicsHLT = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTFHLT',
            'pathALCARECOTkAlCosmicsCosmicTFHLT')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlCosmicsCTF_*_*',
        'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmicsHLT_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTFHLT',
            'pathALCARECOTkAlCosmicsCosmicTFHLT')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlCosmicsCTF_*_*',
        'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmicsInCollisions = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsInCollisions')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlCosmicsInCollisions_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmicsInCollisions_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsInCollisions')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlCosmicsInCollisions_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlCosmics_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlCosmicsCTF',
            'pathALCARECOTkAlCosmicsCosmicTF')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlCosmicsCTF_*_*',
        'keep *_ALCARECOTkAlCosmicsCosmicTF_*_*',
        'keep siStripDigis_DetIdCollection_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep Si*Cluster*_si*Clusters_*_*',
        'keep recoMuons_muons1Leg_*_*')
)

process.OutALCARECOTkAlJpsiMuMu = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlJpsiMuMu')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlJpsiMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlJpsiMuMu_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlJpsiMuMu')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlJpsiMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlLAS = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlLAS')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlLAST0Producer_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlLAS_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlLAS')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlLAST0Producer_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlMinBias = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlMinBias')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlMinBias_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep *_offlineBeamSpot_*_*')
)

process.OutALCARECOTkAlMinBiasHI = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlMinBiasHI')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlMinBiasHI_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep *_offlineBeamSpot_*_*')
)

process.OutALCARECOTkAlMinBiasHI_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlMinBiasHI')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlMinBiasHI_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep *_offlineBeamSpot_*_*')
)

process.OutALCARECOTkAlMinBias_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlMinBias')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlMinBias_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*',
        'keep *_offlinePrimaryVertices_*_*',
        'keep *_offlineBeamSpot_*_*')
)

process.OutALCARECOTkAlMuonIsolated = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlMuonIsolated')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlMuonIsolated_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlMuonIsolated_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlMuonIsolated')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlMuonIsolated_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlUpsilonMuMu = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlUpsilonMuMu')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlUpsilonMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlUpsilonMuMu_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlUpsilonMuMu')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlUpsilonMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlZMuMu = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlZMuMu')
    ),
    outputCommands = cms.untracked.vstring('drop *',
        'keep *_ALCARECOTkAlZMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.OutALCARECOTkAlZMuMu_noDrop = cms.PSet(
    SelectEvents = cms.untracked.PSet(
        SelectEvents = cms.vstring('pathALCARECOTkAlZMuMu')
    ),
    outputCommands = cms.untracked.vstring('keep *_ALCARECOTkAlZMuMu_*_*',
        'keep L1AcceptBunchCrossings_*_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_TriggerResults_*_*',
        'keep DcsStatuss_scalersRawToDigi_*_*')
)

process.RAWDEBUGEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'drop *',
        'drop *',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep *_g4SimHits_*_*',
        'keep edmHepMCProduct_source_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*',
        'keep EBSrFlagsSorted_simEcalDigis_*_*',
        'keep EESrFlagsSorted_simEcalDigis_*_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep PileupSummaryInfos_*_*_*',
        'keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*',
        'keep recoGenJets_*_*_*',
        'keep *_genParticle_*_*',
        'keep recoGenMETs_*_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep *_randomEngineStateProducer_*_*',
        'keep *_logErrorHarvester_*_*',
        'keep PixelDigiSimLinkedmDetSetVector_simSiPixelDigis_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simSiStripDigis_*_*',
        'keep *_allTrackMCMatch_*_*',
        'drop *_trackingtruthprod_*_*',
        'drop *_electrontruth_*_*',
        'keep *_mergedtruth_MergedTrackTruth_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*')
)

process.RAWDEBUGHLTEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'drop *',
        'drop *',
        'drop *',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep *_g4SimHits_*_*',
        'keep edmHepMCProduct_source_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*',
        'keep EBSrFlagsSorted_simEcalDigis_*_*',
        'keep EESrFlagsSorted_simEcalDigis_*_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep PileupSummaryInfos_*_*_*',
        'keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*',
        'keep recoGenJets_*_*_*',
        'keep *_genParticle_*_*',
        'keep recoGenMETs_*_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep *_randomEngineStateProducer_*_*',
        'keep *_logErrorHarvester_*_*',
        'keep PixelDigiSimLinkedmDetSetVector_simSiPixelDigis_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simSiStripDigis_*_*',
        'keep *_allTrackMCMatch_*_*',
        'drop *_trackingtruthprod_*_*',
        'drop *_electrontruth_*_*',
        'keep *_mergedtruth_MergedTrackTruth_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*',
        'keep *_l1GtRecord_*_*',
        'keep *_l1GtTriggerMenuLite_*_*',
        'keep *_conditionsInEdm_*_*',
        'keep *_l1extraParticles_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltAlCaEtaRecHitsFilter_*_*',
        'keep *_hltAlCaPhiSymStream_*_*',
        'keep *_hltAlCaPi0RecHitsFilter_*_*',
        'keep *_hltBLifetimeL25AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL25BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL25JetsStartupU_*_*',
        'keep *_hltBLifetimeL25TagInfosStartupU_*_*',
        'keep *_hltBLifetimeL3AssociatorStartupU_*_*',
        'keep *_hltBLifetimeL3BJetTagsStartupU_*_*',
        'keep *_hltBLifetimeL3JetsStartupU_*_*',
        'keep *_hltBLifetimeL3TagInfosStartupU_*_*',
        'keep *_hltBLifetimeRegionalCtfWithMaterialTracksStartupU_*_*',
        'keep *_hltBSoftMuonL25BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL25JetsU_*_*',
        'keep *_hltBSoftMuonL25TagInfosU_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByDR_*_*',
        'keep *_hltBSoftMuonL3BJetTagsUByPt_*_*',
        'keep *_hltBSoftMuonL3TagInfosU_*_*',
        'keep *_hltCkfL1IsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCkfL1NonIsoLargeWindowTrackCandidates_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1Isolated_*_*',
        'keep *_hltCorrectedHybridSuperClustersL1NonIsolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1Isolated_*_*',
        'keep *_hltCorrectedMulti5x5EndcapSuperClustersWithPreshowerL1NonIsolated_*_*',
        'keep *_hltCsc2DRecHits_*_*',
        'keep *_hltCscSegments_*_*',
        'keep *_hltCtfL1IsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltCtfL1NonIsoLargeWindowWithMaterialTracks_*_*',
        'keep *_hltDt1DRecHits_*_*',
        'keep *_hltDt4DSegments_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL25LeadingTrackPtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EcalIsolationDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EcalIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EcalIsolationSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL2EtCutDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL2EtCutDoubleLooseIsoTau15_*_*',
        'keep *_hltFilterL2EtCutSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltFilterL2EtCutSingleLooseIsoTau20_*_*',
        'keep *_hltFilterL3TrackIsolationDoubleIsoTau15Trk5_*_*',
        'keep *_hltFilterL3TrackIsolationSingleIsoTau30Trk5MET20_*_*',
        'keep *_hltGctDigis_*_*',
        'keep *_hltGtDigis_*_*',
        'keep *_hltHITCtfWithMaterialTracksHB8E29_*_*',
        'keep *_hltHITCtfWithMaterialTracksHE8E29_*_*',
        'keep *_hltHITIPTCorrectorHB8E29_*_*',
        'keep *_hltHITIPTCorrectorHE8E29_*_*',
        'keep *_hltHcalDigis_*_*',
        'keep *_hltHoreco_*_*',
        'keep *_hltIconeCentral1Regional_*_*',
        'keep *_hltIconeCentral2Regional_*_*',
        'keep *_hltIconeCentral3Regional_*_*',
        'keep *_hltIconeCentral4Regional_*_*',
        'keep *_hltIconeTau1Regional_*_*',
        'keep *_hltIconeTau2Regional_*_*',
        'keep *_hltIconeTau3Regional_*_*',
        'keep *_hltIconeTau4Regional_*_*',
        'keep *_hltIsolPixelTrackProdHB8E29_*_*',
        'keep *_hltIsolPixelTrackProdHE8E29_*_*',
        'keep *_hltIterativeCone5CaloJets_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep *_hltL1IsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1IsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1IsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1IsoHLTClusterShape_*_*',
        'keep *_hltL1IsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1IsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1IsoRecoEcalCandidate_*_*',
        'keep *_hltL1IsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1IsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1IsolatedElectronHcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1IsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCTFFinalFitWithMaterial_*_*',
        'keep *_hltL1NonIsoEgammaRegionalCkfTrackCandidates_*_*',
        'keep *_hltL1NonIsoEgammaRegionalPixelSeedGenerator_*_*',
        'keep *_hltL1NonIsoHLTClusterShape_*_*',
        'keep *_hltL1NonIsoLargeWindowElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoPhotonHollowTrackIsol_*_*',
        'keep *_hltL1NonIsoRecoEcalCandidate_*_*',
        'keep *_hltL1NonIsoSiStripElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsoStartUpElectronPixelSeeds_*_*',
        'keep *_hltL1NonIsolatedElectronHcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonEcalIsol_*_*',
        'keep *_hltL1NonIsolatedPhotonHcalIsol_*_*',
        'keep *_hltL1extraParticles_*_*',
        'keep *_hltL1sDoubleLooseIsoTau15_*_*',
        'keep *_hltL1sSingleLooseIsoTau20_*_*',
        'keep *_hltL25TauConeIsolation_*_*',
        'keep *_hltL25TauCtfWithMaterialTracks_*_*',
        'keep *_hltL25TauJetTracksAssociator_*_*',
        'keep *_hltL25TauLeadingTrackPtCutSelector_*_*',
        'keep *_hltL2MuonCandidatesNoVtx_*_*',
        'keep *_hltL2MuonCandidates_*_*',
        'keep *_hltL2MuonIsolations_*_*',
        'keep *_hltL2MuonSeeds_*_*',
        'keep *_hltL2Muons_*_*',
        'keep *_hltL2TauJets_*_*',
        'keep *_hltL2TauNarrowConeIsolationProducer_*_*',
        'keep *_hltL2TauRelaxingIsolationSelector_*_*',
        'keep *_hltL3MuonCandidatesNoVtx_*_*',
        'keep *_hltL3MuonCandidates_*_*',
        'keep *_hltL3MuonIsolations_*_*',
        'keep *_hltL3MuonsIOHit_*_*',
        'keep *_hltL3MuonsLinksCombination_*_*',
        'keep *_hltL3MuonsNoVtx_*_*',
        'keep *_hltL3MuonsOIHit_*_*',
        'keep *_hltL3MuonsOIState_*_*',
        'keep *_hltL3Muons_*_*',
        'keep *_hltL3TauConeIsolation_*_*',
        'keep *_hltL3TauCtfWithMaterialTracks_*_*',
        'keep *_hltL3TauIsolationSelector_*_*',
        'keep *_hltL3TauJetTracksAssociator_*_*',
        'keep *_hltL3TkFromL2OICombination_*_*',
        'keep *_hltL3TkTracksFromL2IOHit_*_*',
        'keep *_hltL3TkTracksFromL2OIHit_*_*',
        'keep *_hltL3TkTracksFromL2OIState_*_*',
        'keep *_hltL3TkTracksFromL2_*_*',
        'keep *_hltL3TrackCandidateFromL2IOHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIHit_*_*',
        'keep *_hltL3TrackCandidateFromL2OIState_*_*',
        'keep *_hltL3TrackCandidateFromL2_*_*',
        'keep *_hltL3TrajSeedIOHit_*_*',
        'keep *_hltL3TrajSeedOIHit_*_*',
        'keep *_hltL3TrajSeedOIState_*_*',
        'keep *_hltL3TrajectorySeedNoVtx_*_*',
        'keep *_hltL3TrajectorySeed_*_*',
        'keep *_hltMCJetCorJetIcone5HF07_*_*',
        'keep *_hltMet_*_*',
        'keep *_hltMuTrackJpsiCtfTrackCands_*_*',
        'keep *_hltMuTrackJpsiCtfTracks_*_*',
        'keep *_hltMuTrackJpsiPixelTrackCands_*_*',
        'keep *_hltMuTrackJpsiPixelTrackSelector_*_*',
        'keep *_hltMuTrackJpsiTrackSeeds_*_*',
        'keep *_hltMuonCSCDigis_*_*',
        'keep *_hltMuonDTDigis_*_*',
        'keep *_hltMuonRPCDigis_*_*',
        'keep *_hltOfflineBeamSpot_*_*',
        'keep *_hltPixelMatchElectronsL1Iso_*_*',
        'keep *_hltPixelMatchElectronsL1NonIso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1Iso_*_*',
        'keep *_hltPixelMatchLargeWindowElectronsL1NonIso_*_*',
        'keep *_hltPixelTracks_*_*',
        'keep *_hltPixelVertices_*_*',
        'keep *_hltRpcRecHits_*_*',
        'keep *_hltSiPixelClusters_*_*',
        'keep *_hltSiPixelRecHits_*_*',
        'keep *_hltSiStripClusters_*_*',
        'keep *_hltSiStripRawToClustersFacility_*_*',
        'keep *_hltTowerMakerForAll_*_*',
        'keep *_hltTowerMakerForMuons_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep L1GlobalTriggerReadoutRecord_hltGtDigis_*_*',
        'keep L1MuGMTCands_hltGtDigis_*_*',
        'keep L1MuGMTReadoutCollection_hltGtDigis_*_*',
        'keep SiPixelClusteredmNewDetSetVector_hltSiPixelClusters_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEventWithRefs_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.RAWEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*')
)

process.RAWRECOEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'keep  FEDRawDataCollection_rawDataCollector_*_*', 'keep  FEDRawDataCollection_source_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep FEDRawDataCollection_rawDataCollector_*_*', 'keep FEDRawDataCollection_source_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'drop *', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*')+cms.untracked.vstring('keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_logErrorHarvester_*_*'))
)

process.RAWSIMEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'drop *',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'keep  FEDRawDataCollection_rawDataCollector_*_*',
        'keep  FEDRawDataCollection_source_*_*',
        'drop *_hlt*_*_*',
        'keep *_hltL1GtObjectMap_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep edmTriggerResults_*_*_*',
        'keep triggerTriggerEvent_*_*_*',
        'keep *_g4SimHits_*_*',
        'keep edmHepMCProduct_source_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*',
        'keep EBSrFlagsSorted_simEcalDigis_*_*',
        'keep EESrFlagsSorted_simEcalDigis_*_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep PileupSummaryInfos_*_*_*',
        'keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*',
        'keep recoGenJets_*_*_*',
        'keep *_genParticle_*_*',
        'keep recoGenMETs_*_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep *_randomEngineStateProducer_*_*',
        'keep *_logErrorHarvester_*_*')
)

process.RECODEBUGEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'drop *', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*')+cms.untracked.vstring('keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_logErrorHarvester_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenMETs_*_*_*', 'keep *_kt4GenJets_*_*', 'keep *_kt6GenJets_*_*', 'keep *_ak5GenJets_*_*', 'keep *_ak7GenJets_*_*', 'keep *_iterativeCone5GenJets_*_*', 'keep *_genParticle_*_*', 'keep edmHepMCProduct_source_*_*', 'keep SimTracks_g4SimHits_*_*', 'keep SimVertexs_g4SimHits_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep PileupSummaryInfos_*_*_*', 'drop *_trackingtruthprod_*_*', 'drop *_electrontruth_*_*', 'keep *_mergedtruth_MergedTrackTruth_*', 'keep CrossingFramePlaybackInfoExtended_*_*_*', 'keep PixelDigiSimLinkedmDetSetVector_simSiPixelDigis_*_*', 'keep StripDigiSimLinkedmDetSetVector_simSiStripDigis_*_*', 'keep *_allTrackMCMatch_*_*'))
)

process.RECOEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*', 'keep recoPFCandidates_particleFlow_*_*')+cms.untracked.vstring('keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_logErrorHarvester_*_*'))
)

process.RECOSIMEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = (cms.untracked.vstring('drop *', 'drop *', 'keep DetIdedmEDCollection_siStripDigis_*_*', 'keep *_siPixelClusters_*_*', 'keep *_siStripClusters_*_*', 'keep *_dt1DRecHits_*_*', 'keep *_dt4DSegments_*_*', 'keep *_dt1DCosmicRecHits_*_*', 'keep *_dt4DCosmicSegments_*_*', 'keep *_csc2DRecHits_*_*', 'keep *_cscSegments_*_*', 'keep *_rpcRecHits_*_*', 'keep *_hbhereco_*_*', 'keep *_hfreco_*_*', 'keep *_horeco_*_*', 'keep HBHERecHitsSorted_hbherecoMB_*_*', 'keep HORecHitsSorted_horecoMB_*_*', 'keep HFRecHitsSorted_hfrecoMB_*_*', 'keep ZDCDataFramesSorted_*Digis_*_*', 'keep ZDCRecHitsSorted_*_*_*', 'keep *_reducedHcalRecHits_*_*', 'keep *_castorreco_*_*', 'keep *_ecalPreshowerRecHit_*_*', 'keep *_ecalRecHit_*_*', 'keep *_ecalCompactTrigPrim_*_*', 'keep *_ecalTPSkim_*_*', 'keep *_selectDigi_*_*', 'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*', 'keep *_hybridSuperClusters_*_*', 'keep recoSuperClusters_correctedHybridSuperClusters_*_*', 'keep *_multi5x5BasicClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClusters_*_*', 'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*', 'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*', 'drop recoClusterShapes_*_*_*', 'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*', 'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*', 'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*', 'keep *_CkfElectronCandidates_*_*', 'keep *_GsfGlobalElectronTest_*_*', 'keep *_electronMergedSeeds_*_*', 'keep recoGsfTracks_electronGsfTracks_*_*', 'keep recoGsfTrackExtras_electronGsfTracks_*_*', 'keep recoTrackExtras_electronGsfTracks_*_*', 'keep TrackingRecHitsOwned_electronGsfTracks_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoTrackExtras_generalTracks_*_*', 'keep TrackingRecHitsOwned_generalTracks_*_*', 'keep recoTracks_beamhaloTracks_*_*', 'keep recoTrackExtras_beamhaloTracks_*_*', 'keep TrackingRecHitsOwned_beamhaloTracks_*_*', 'keep recoTracks_regionalCosmicTracks_*_*', 'keep recoTrackExtras_regionalCosmicTracks_*_*', 'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*', 'keep recoTracks_rsWithMaterialTracks_*_*', 'keep recoTrackExtras_rsWithMaterialTracks_*_*', 'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*', 'keep *_ctfPixelLess_*_*', 'keep *_dedxTruncated40_*_*', 'keep *_dedxMedian_*_*', 'keep *_dedxHarmonic2_*_*', 'keep *_trackExtrapolator_*_*', 'keep *_kt4CaloJets_*_*', 'keep *_kt6CaloJets_*_*', 'keep *_ak5CaloJets_*_*', 'keep *_ak7CaloJets_*_*', 'keep *_iterativeCone5CaloJets_*_*', 'keep *_iterativeCone15CaloJets_*_*', 'keep *_kt4PFJets_*_*', 'keep *_kt6PFJets_*_*', 'keep *_ak5PFJets_*_*', 'keep *_ak7PFJets_*_*', 'keep *_iterativeCone5PFJets_*_*', 'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_ak5TrackJets_*_*', 'keep *_kt4TrackJets_*_*', 'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*', 'keep *_caloTowers_*_*', 'keep *_towerMaker_*_*', 'keep *_CastorTowerReco_*_*', 'keep *_ic5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*', 'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*', 'keep *_iterativeCone5JetExtender_*_*', 'keep *_kt4JetTracksAssociatorAtVertex_*_*', 'keep *_kt4JetTracksAssociatorAtCaloFace_*_*', 'keep *_kt4JetExtender_*_*', 'keep *_ak5JetTracksAssociatorAtVertex_*_*', 'keep *_ak5JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak5JetExtender_*_*', 'keep *_ak7JetTracksAssociatorAtVertex_*_*', 'keep *_ak7JetTracksAssociatorAtCaloFace_*_*', 'keep *_ak7JetExtender_*_*', 'keep *_ak5JetID_*_*', 'keep *_ak7JetID_*_*', 'keep *_ic5JetID_*_*', 'keep *_kt4JetID_*_*', 'keep *_kt6JetID_*_*', 'keep *_ak7BasicJets_*_*', 'keep *_ak7CastorJetID_*_*', 'keep recoCaloMETs_met_*_*', 'keep recoCaloMETs_metNoHF_*_*', 'keep recoCaloMETs_metHO_*_*', 'keep recoCaloMETs_corMetGlobalMuons_*_*', 'keep recoCaloMETs_metNoHFHO_*_*', 'keep recoCaloMETs_metOptHO_*_*', 'keep recoCaloMETs_metOpt_*_*', 'keep recoCaloMETs_metOptNoHFHO_*_*', 'keep recoCaloMETs_metOptNoHF_*_*', 'keep recoMETs_htMetAK5_*_*', 'keep recoMETs_htMetAK7_*_*', 'keep recoMETs_htMetIC5_*_*', 'keep recoMETs_htMetKT4_*_*', 'keep recoMETs_htMetKT6_*_*', 'keep recoMETs_tcMet_*_*', 'keep recoMETs_tcMetWithPFclusters_*_*', 'keep recoPFMETs_pfMet_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*', 'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*', 'keep recoHcalNoiseRBXs_hcalnoise_*_*', 'keep HcalNoiseSummary_hcalnoise_*_*', 'keep *HaloData_*_*_*', 'keep *BeamHaloSummary_BeamHaloSummary_*_*', 'keep *_MuonSeed_*_*', 'keep *_ancientMuonSeed_*_*', 'keep *_mergedStandAloneMuonSeeds_*_*', 'keep TrackingRecHitsOwned_globalMuons_*_*', 'keep TrackingRecHitsOwned_tevMuons_*_*', 'keep recoCaloMuons_calomuons_*_*', 'keep *_CosmicMuonSeed_*_*', 'keep recoTrackExtras_cosmicMuons_*_*', 'keep TrackingRecHitsOwned_cosmicMuons_*_*', 'keep recoTrackExtras_globalCosmicMuons_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons_*_*', 'keep recoTrackExtras_cosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*', 'keep recoTrackExtras_globalCosmicMuons1Leg_*_*', 'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*', 'keep recoTracks_cosmicsVetoTracks_*_*', 'keep *_SETMuonSeed_*_*', 'keep recoTracks_standAloneSETMuons_*_*', 'keep recoTrackExtras_standAloneSETMuons_*_*', 'keep TrackingRecHitsOwned_standAloneSETMuons_*_*', 'keep recoTracks_globalSETMuons_*_*', 'keep recoTrackExtras_globalSETMuons_*_*', 'keep TrackingRecHitsOwned_globalSETMuons_*_*', 'keep recoMuons_muonsWithSET_*_*', 'keep recoTracks_standAloneMuons_*_*', 'keep recoTrackExtras_standAloneMuons_*_*', 'keep TrackingRecHitsOwned_standAloneMuons_*_*', 'keep recoTracks_globalMuons_*_*', 'keep recoTrackExtras_globalMuons_*_*', 'keep recoTracks_tevMuons_*_*', 'keep recoTrackExtras_tevMuons_*_*', 'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*', 'keep recoTracks_generalTracks_*_*', 'keep recoMuons_muons_*_*', 'keep booledmValueMap_muid*_*_*', 'keep recoMuonTimeExtraedmValueMap_muons_*_*', 'keep *_muonShowerInformation_*_*', 'keep recoTracks_cosmicMuons_*_*', 'keep recoTracks_globalCosmicMuons_*_*', 'keep recoMuons_muonsFromCosmics_*_*', 'keep recoTracks_cosmicMuons1Leg_*_*', 'keep recoTracks_globalCosmicMuons1Leg_*_*', 'keep recoMuons_muonsFromCosmics1Leg_*_*', 'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*', 'keep uintedmValueMap_cosmicsVeto_*_*', 'keep *_muIsoDepositTk_*_*', 'keep *_muIsoDepositCalByAssociatorTowers_*_*', 'keep *_muIsoDepositCalByAssociatorHits_*_*', 'keep *_muIsoDepositJets_*_*', 'keep *_muGlobalIsoDepositCtfTk_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*', 'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*', 'keep *_muGlobalIsoDepositJets_*_*', 'keep *_impactParameterTagInfos_*_*', 'keep *_trackCountingHighEffBJetTags_*_*', 'keep *_trackCountingHighPurBJetTags_*_*', 'keep *_jetProbabilityBJetTags_*_*', 'keep *_jetBProbabilityBJetTags_*_*', 'keep *_secondaryVertexTagInfos_*_*', 'keep *_ghostTrackVertexTagInfos_*_*', 'keep *_simpleSecondaryVertexBJetTags_*_*', 'keep *_simpleSecondaryVertexHighEffBJetTags_*_*', 'keep *_simpleSecondaryVertexHighPurBJetTags_*_*', 'keep *_combinedSecondaryVertexBJetTags_*_*', 'keep *_combinedSecondaryVertexMVABJetTags_*_*', 'keep *_ghostTrackBJetTags_*_*', 'keep *_btagSoftElectrons_*_*', 'keep *_softElectronCands_*_*', 'keep *_softPFElectrons_*_*', 'keep *_softElectronTagInfos_*_*', 'keep *_softElectronBJetTags_*_*', 'keep *_softElectronByIP3dBJetTags_*_*', 'keep *_softElectronByPtBJetTags_*_*', 'keep *_softMuonTagInfos_*_*', 'keep *_softMuonBJetTags_*_*', 'keep *_softMuonByIP3dBJetTags_*_*', 'keep *_softMuonByPtBJetTags_*_*', 'keep *_combinedMVABJetTags_*_*', 'keep *_ak5PFJetsRecoTauPiZeros_*_*', 'keep *_hpsPFTauProducer*_*_*', 'keep *_hpsPFTauDiscrimination*_*_*', 'keep *_shrinkingConePFTauProducer*_*_*', 'keep *_shrinkingConePFTauDiscrimination*_*_*', 'keep *_hpsTancTaus_*_*', 'keep *_hpsTancTausDiscrimination*_*_*', 'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*', 'keep *_caloRecoTauTagInfoProducer_*_*', 'keep recoCaloTaus_caloRecoTauProducer*_*_*', 'keep *_caloRecoTauDiscrimination*_*_*', 'keep  *_offlinePrimaryVertices_*_*', 'keep  *_offlinePrimaryVerticesWithBS_*_*', 'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*', 'keep  *_nuclearInteractionMaker_*_*', 'keep *_generalV0Candidates_*_*', 'keep recoGsfElectronCores_gsfElectronCores_*_*', 'keep recoGsfElectrons_gsfElectrons_*_*', 'keep floatedmValueMap_eidRobustLoose_*_*', 'keep floatedmValueMap_eidRobustTight_*_*', 'keep floatedmValueMap_eidRobustHighEnergy_*_*', 'keep floatedmValueMap_eidLoose_*_*', 'keep floatedmValueMap_eidTight_*_*', 'keep recoPhotons_photons_*_*', 'keep recoPhotonCores_photonCore_*_*', 'keep recoConversions_conversions_*_*', 'drop *_conversions_uncleanedConversions_*', 'keep recoConversions_trackerOnlyConversions_*_*', 'keep recoTracks_ckfOutInTracksFromConversions_*_*', 'keep recoTracks_ckfInOutTracksFromConversions_*_*', 'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*', 'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*', 'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*', 'keep *_PhotonIDProd_*_*', 'keep *_hfRecoEcalCandidate_*_*', 'keep *_hfEMClusters_*_*', 'keep *_pixelTracks_*_*', 'keep *_pixelVertices_*_*', 'drop CaloTowersSorted_towerMakerPF_*_*', 'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*', 'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*', 'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*', 'keep recoPFClusters_particleFlowClusterECAL_*_*', 'keep recoPFClusters_particleFlowClusterHCAL_*_*', 'keep recoPFClusters_particleFlowClusterPS_*_*', 'keep recoPFBlocks_particleFlowBlock_*_*')+cms.untracked.vstring('keep recoPFCandidates_particleFlow_*_*', 'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*', 'keep *_pfElectronTranslator_*_*', 'keep *_trackerDrivenElectronSeeds_preid_*', 'keep *_offlineBeamSpot_*_*', 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*', 'keep *_l1GtRecord_*_*', 'keep *_l1GtTriggerMenuLite_*_*', 'keep *_conditionsInEdm_*_*', 'keep *_l1extraParticles_*_*', 'keep L1MuGMTReadoutCollection_gtDigis_*_*', 'keep L1GctEmCand*_gctDigis_*_*', 'keep L1GctJetCand*_gctDigis_*_*', 'keep L1GctEtHad*_gctDigis_*_*', 'keep L1GctEtMiss*_gctDigis_*_*', 'keep L1GctEtTotal*_gctDigis_*_*', 'keep L1GctHtMiss*_gctDigis_*_*', 'keep L1GctJetCounts*_gctDigis_*_*', 'keep L1GctHFRingEtSums*_gctDigis_*_*', 'keep L1GctHFBitCounts*_gctDigis_*_*', 'keep LumiDetails_lumiProducer_*_*', 'keep LumiSummary_lumiProducer_*_*', 'drop *_hlt*_*_*', 'keep *_hltL1GtObjectMap_*_*', 'keep edmTriggerResults_*_*_*', 'keep triggerTriggerEvent_*_*_*', 'keep L1AcceptBunchCrossings_*_*_*', 'keep L1TriggerScalerss_*_*_*', 'keep Level1TriggerScalerss_*_*_*', 'keep LumiScalerss_*_*_*', 'keep BeamSpotOnlines_*_*_*', 'keep DcsStatuss_*_*_*', 'keep *_logErrorHarvester_*_*', 'keep LHERunInfoProduct_source_*_*', 'keep LHEEventProduct_source_*_*', 'keep GenRunInfoProduct_generator_*_*', 'keep GenEventInfoProduct_generator_*_*', 'keep edmHepMCProduct_generator_*_*', 'keep GenFilterInfo_*_*_*', 'keep *_genParticles_*_*', 'keep recoGenMETs_*_*_*', 'keep *_kt4GenJets_*_*', 'keep *_kt6GenJets_*_*', 'keep *_ak5GenJets_*_*', 'keep *_ak7GenJets_*_*', 'keep *_iterativeCone5GenJets_*_*', 'keep *_genParticle_*_*', 'keep edmHepMCProduct_source_*_*', 'keep SimTracks_g4SimHits_*_*', 'keep SimVertexs_g4SimHits_*_*', 'keep *_allTrackMCMatch_*_*', 'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*', 'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*', 'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*', 'keep PileupSummaryInfos_*_*_*'))
)

process.REPACKRAWEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'drop FEDRawDataCollection_*_*_*',
        'keep FEDRawDataCollection_rawDataRepacker_*_*',
        'keep FEDRawDataCollection_virginRawDataRepacker_*_*')
)

process.REPACKRAWSIMEventContent = cms.PSet(
    splitLevel = cms.untracked.int32(0),
    outputCommands = cms.untracked.vstring('drop *',
        'drop FEDRawDataCollection_*_*_*',
        'keep FEDRawDataCollection_rawDataRepacker_*_*',
        'keep FEDRawDataCollection_virginRawDataRepacker_*_*',
        'keep *_g4SimHits_*_*',
        'keep edmHepMCProduct_source_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*',
        'keep EBSrFlagsSorted_simEcalDigis_*_*',
        'keep EESrFlagsSorted_simEcalDigis_*_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep PileupSummaryInfos_*_*_*',
        'keep LHERunInfoProduct_source_*_*',
        'keep LHEEventProduct_source_*_*',
        'keep GenRunInfoProduct_generator_*_*',
        'keep GenEventInfoProduct_generator_*_*',
        'keep edmHepMCProduct_generator_*_*',
        'keep GenFilterInfo_*_*_*',
        'keep *_genParticles_*_*',
        'keep recoGenJets_*_*_*',
        'keep *_genParticle_*_*',
        'keep recoGenMETs_*_*_*',
        'keep FEDRawDataCollection_source_*_*',
        'keep FEDRawDataCollection_rawDataCollector_*_*',
        'keep *_MEtoEDMConverter_*_*',
        'keep *_randomEngineStateProducer_*_*',
        'keep *_logErrorHarvester_*_*')
)

process.Realistic2p76TeV2011CollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(1100.0),
    Emittance = cms.double(1.7e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.22),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.3929),
    X0 = cms.double(0.244),
    Z0 = cms.double(0.4145)
)

process.Realistic7TeV2011CollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(150.0),
    Emittance = cms.double(6.7e-08),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(5.22),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.3929),
    X0 = cms.double(0.244),
    Z0 = cms.double(0.4145)
)

process.Realistic7TeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(350.0),
    Emittance = cms.double(1.072e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(6.26),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.3929),
    X0 = cms.double(0.244),
    Z0 = cms.double(0.4145)
)

process.Realistic8TeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(150.0),
    Emittance = cms.double(9e-08),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(6.26),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.393),
    X0 = cms.double(0.244),
    Z0 = cms.double(0.41)
)

process.Realistic900GeVCollisionVtxSmearingParameters = cms.PSet(
    Phi = cms.double(0.0),
    BetaStar = cms.double(1000.0),
    Emittance = cms.double(8.34e-07),
    Alpha = cms.double(0.0),
    SigmaZ = cms.double(6.17),
    TimeOffset = cms.double(0.0),
    Y0 = cms.double(0.3993),
    X0 = cms.double(0.2452),
    Z0 = cms.double(0.8222)
)

process.RecoBTagAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_trackCountingHighEffBJetTags_*_*',
        'keep *_trackCountingHighPurBJetTags_*_*',
        'keep *_jetProbabilityBJetTags_*_*',
        'keep *_jetBProbabilityBJetTags_*_*',
        'keep *_simpleSecondaryVertexBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighEffBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighPurBJetTags_*_*',
        'keep *_combinedSecondaryVertexBJetTags_*_*',
        'keep *_combinedSecondaryVertexMVABJetTags_*_*',
        'keep *_softElectronBJetTags_*_*',
        'keep *_softElectronByIP3dBJetTags_*_*',
        'keep *_softElectronByPtBJetTags_*_*',
        'keep *_softMuonBJetTags_*_*',
        'keep *_softMuonByIP3dBJetTags_*_*',
        'keep *_softMuonByPtBJetTags_*_*',
        'keep *_combinedMVABJetTags_*_*')
)

process.RecoBTagFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_impactParameterTagInfos_*_*',
        'keep *_trackCountingHighEffBJetTags_*_*',
        'keep *_trackCountingHighPurBJetTags_*_*',
        'keep *_jetProbabilityBJetTags_*_*',
        'keep *_jetBProbabilityBJetTags_*_*',
        'keep *_secondaryVertexTagInfos_*_*',
        'keep *_ghostTrackVertexTagInfos_*_*',
        'keep *_simpleSecondaryVertexBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighEffBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighPurBJetTags_*_*',
        'keep *_combinedSecondaryVertexBJetTags_*_*',
        'keep *_combinedSecondaryVertexMVABJetTags_*_*',
        'keep *_ghostTrackBJetTags_*_*',
        'keep *_btagSoftElectrons_*_*',
        'keep *_softElectronCands_*_*',
        'keep *_softPFElectrons_*_*',
        'keep *_softElectronTagInfos_*_*',
        'keep *_softElectronBJetTags_*_*',
        'keep *_softElectronByIP3dBJetTags_*_*',
        'keep *_softElectronByPtBJetTags_*_*',
        'keep *_softMuonTagInfos_*_*',
        'keep *_softMuonBJetTags_*_*',
        'keep *_softMuonByIP3dBJetTags_*_*',
        'keep *_softMuonByPtBJetTags_*_*',
        'keep *_combinedMVABJetTags_*_*')
)

process.RecoBTagRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_impactParameterTagInfos_*_*',
        'keep *_trackCountingHighEffBJetTags_*_*',
        'keep *_trackCountingHighPurBJetTags_*_*',
        'keep *_jetProbabilityBJetTags_*_*',
        'keep *_jetBProbabilityBJetTags_*_*',
        'keep *_secondaryVertexTagInfos_*_*',
        'keep *_ghostTrackVertexTagInfos_*_*',
        'keep *_simpleSecondaryVertexBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighEffBJetTags_*_*',
        'keep *_simpleSecondaryVertexHighPurBJetTags_*_*',
        'keep *_combinedSecondaryVertexBJetTags_*_*',
        'keep *_combinedSecondaryVertexMVABJetTags_*_*',
        'keep *_ghostTrackBJetTags_*_*',
        'keep *_btagSoftElectrons_*_*',
        'keep *_softElectronCands_*_*',
        'keep *_softPFElectrons_*_*',
        'keep *_softElectronTagInfos_*_*',
        'keep *_softElectronBJetTags_*_*',
        'keep *_softElectronByIP3dBJetTags_*_*',
        'keep *_softElectronByPtBJetTags_*_*',
        'keep *_softMuonTagInfos_*_*',
        'keep *_softMuonBJetTags_*_*',
        'keep *_softMuonByIP3dBJetTags_*_*',
        'keep *_softMuonByPtBJetTags_*_*',
        'keep *_combinedMVABJetTags_*_*')
)

process.RecoBTauAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.RecoBTauFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.RecoBTauRECO = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.RecoEcalAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_selectDigi_*_*',
        'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*',
        'keep recoSuperClusters_correctedHybridSuperClusters_*_*',
        'keep recoCaloClusters_hybridSuperClusters_*_*',
        'keep recoSuperClusters_hybridSuperClusters_uncleanOnlyHybridBarrelSuperClusters_*',
        'keep recoCaloClusters_multi5x5BasicClusters_multi5x5EndcapBasicClusters_*',
        'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*')
)

process.RecoEcalFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_selectDigi_*_*',
        'keep *_reducedEcalRecHits*_*_*',
        'keep *_interestingEcalDetId*_*_*',
        'keep *_ecalWeightUncalibRecHit_*_*',
        'keep *_ecalPreshowerRecHit_*_*',
        'keep *_hybridSuperClusters_*_*',
        'keep *_correctedHybridSuperClusters_*_*',
        'keep *_multi5x5*_*_*',
        'keep *_correctedMulti5x5*_*_*',
        'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*',
        'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*',
        'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*')
)

process.RecoEcalRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_selectDigi_*_*',
        'keep EcalRecHitsSorted_reducedEcalRecHits*_*_*',
        'keep *_hybridSuperClusters_*_*',
        'keep recoSuperClusters_correctedHybridSuperClusters_*_*',
        'keep *_multi5x5BasicClusters_*_*',
        'keep recoSuperClusters_multi5x5SuperClusters_*_*',
        'keep recoSuperClusters_multi5x5SuperClustersWithPreshower_*_*',
        'keep recoSuperClusters_correctedMulti5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusters_multi5x5SuperClustersWithPreshower_*_*',
        'keep recoPreshowerClusterShapes_multi5x5PreshowerClusterShape_*_*',
        'drop recoClusterShapes_*_*_*',
        'drop recoBasicClustersToOnerecoClusterShapesAssociation_*_*_*',
        'drop recoBasicClusters_multi5x5BasicClusters_multi5x5BarrelBasicClusters_*',
        'drop recoSuperClusters_multi5x5SuperClusters_multi5x5BarrelSuperClusters_*')
)

process.RecoEgammaAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoGsfElectronCores_gsfElectronCores_*_*',
        'keep recoGsfElectrons_gsfElectrons_*_*',
        'keep floatedmValueMap_eidRobustLoose_*_*',
        'keep floatedmValueMap_eidRobustTight_*_*',
        'keep floatedmValueMap_eidRobustHighEnergy_*_*',
        'keep floatedmValueMap_eidLoose_*_*',
        'keep floatedmValueMap_eidTight_*_*',
        'keep recoPhotonCores_photonCore_*_*',
        'keep recoPhotons_photons_*_*',
        'keep recoConversions_conversions_*_*',
        'drop *_conversions_uncleanedConversions_*',
        'keep recoConversions_trackerOnlyConversions_*_*',
        'keep recoTracks_ckfOutInTracksFromConversions_*_*',
        'keep recoTracks_ckfInOutTracksFromConversions_*_*',
        'keep *_PhotonIDProd_*_*',
        'keep *_hfRecoEcalCandidate_*_*',
        'keep *_hfEMClusters_*_*')
)

process.RecoEgammaFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_gsfElectronCores_*_*',
        'keep *_gsfElectrons_*_*',
        'keep *_eidRobustLoose_*_*',
        'keep *_eidRobustTight_*_*',
        'keep *_eidRobustHighEnergy_*_*',
        'keep *_eidLoose_*_*',
        'keep *_eidTight_*_*',
        'keep *_conversions_*_*',
        'drop *_conversions_uncleanedConversions_*',
        'keep *_photonCore_*_*',
        'keep *_photons_*_*',
        'keep *_trackerOnlyConversions_*_*',
        'keep *_ckfOutInTracksFromConversions_*_*',
        'keep *_ckfInOutTracksFromConversions_*_*',
        'keep *_PhotonIDProd_*_*',
        'keep *_hfRecoEcalCandidate_*_*',
        'keep *_hfEMClusters_*_*')
)

process.RecoEgammaRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoGsfElectronCores_gsfElectronCores_*_*',
        'keep recoGsfElectrons_gsfElectrons_*_*',
        'keep floatedmValueMap_eidRobustLoose_*_*',
        'keep floatedmValueMap_eidRobustTight_*_*',
        'keep floatedmValueMap_eidRobustHighEnergy_*_*',
        'keep floatedmValueMap_eidLoose_*_*',
        'keep floatedmValueMap_eidTight_*_*',
        'keep recoPhotons_photons_*_*',
        'keep recoPhotonCores_photonCore_*_*',
        'keep recoConversions_conversions_*_*',
        'drop *_conversions_uncleanedConversions_*',
        'keep recoConversions_trackerOnlyConversions_*_*',
        'keep recoTracks_ckfOutInTracksFromConversions_*_*',
        'keep recoTracks_ckfInOutTracksFromConversions_*_*',
        'keep recoTrackExtras_ckfOutInTracksFromConversions_*_*',
        'keep recoTrackExtras_ckfInOutTracksFromConversions_*_*',
        'keep TrackingRecHitsOwned_ckfOutInTracksFromConversions_*_*',
        'keep TrackingRecHitsOwned_ckfInOutTracksFromConversions_*_*',
        'keep *_PhotonIDProd_*_*',
        'keep *_hfRecoEcalCandidate_*_*',
        'keep *_hfEMClusters_*_*')
)

process.RecoGenJetsAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_kt4GenJets_*_*',
        'keep *_kt6GenJets_*_*',
        'keep *_ak5GenJets_*_*',
        'keep *_ak7GenJets_*_*',
        'keep *_genParticle_*_*')
)

process.RecoGenJetsFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoGenJets_*_*_*',
        'keep *_genParticle_*_*')
)

process.RecoGenJetsRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_kt4GenJets_*_*',
        'keep *_kt6GenJets_*_*',
        'keep *_ak5GenJets_*_*',
        'keep *_ak7GenJets_*_*',
        'keep *_iterativeCone5GenJets_*_*',
        'keep *_genParticle_*_*')
)

process.RecoGenMETAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoGenMETs_*_*_*')
)

process.RecoGenMETFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoGenMETs_*_*_*')
)

process.RecoGenMETRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoGenMETs_*_*_*')
)

process.RecoHcalNoiseAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('drop recoHcalNoiseRBXs_hcalnoise_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*')
)

process.RecoHcalNoiseFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoHcalNoiseRBXs_hcalnoise_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*')
)

process.RecoHcalNoiseRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoHcalNoiseRBXs_hcalnoise_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*')
)

process.RecoJetsAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_kt4CaloJets_*_*',
        'keep *_kt6CaloJets_*_*',
        'keep *_ak5CaloJets_*_*',
        'keep *_ak7CaloJets_*_*',
        'keep *_kt4PFJets_*_*',
        'keep *_kt6PFJets_*_*',
        'keep *_ak5PFJets_*_*',
        'keep *_ak7PFJets_*_*',
        'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_ak5TrackJets_*_*',
        'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*',
        'keep *_caloTowers_*_*',
        'keep *_towerMaker_*_*',
        'keep *_CastorTowerReco_*_*',
        'keep *_ak5JetTracksAssociatorAtVertex_*_*',
        'keep *_ak7JetTracksAssociatorAtVertex_*_*',
        'keep *_kt4JetExtender_*_*',
        'keep *_ak5JetExtender_*_*',
        'keep *_ak7JetExtender_*_*',
        'keep *_ak5JetID_*_*',
        'keep *_ak7JetID_*_*',
        'keep *_kt4JetID_*_*',
        'keep *_kt6JetID_*_*',
        'keep *_ak7BasicJets_*_*',
        'keep *_ak7CastorJetID_*_*')
)

process.RecoJetsFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoCaloJets_*_*_*',
        'keep recoPFJets_*_*_*',
        'keep recoTrackJets_*_*_*',
        'keep recoJPTJets_*_*_*',
        'keep *_caloTowers_*_*',
        'keep *_towerMaker_*_*',
        'keep *_CastorTowerReco_*_*',
        'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*',
        'keep *_ic5JetTracksAssociatorAtVertex_*_*',
        'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*',
        'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*',
        'keep *_iterativeCone5JetExtender_*_*',
        'keep *_sisCone5JetTracksAssociatorAtVertex_*_*',
        'keep *_sisCone5JetTracksAssociatorAtCaloFace_*_*',
        'keep *_sisCone5JetExtender_*_*',
        'keep *_kt4JetTracksAssociatorAtVertex_*_*',
        'keep *_kt4JetTracksAssociatorAtCaloFace_*_*',
        'keep *_kt4JetExtender_*_*',
        'keep *_ak5JetTracksAssociatorAtVertex_*_*',
        'keep *_ak5JetTracksAssociatorAtCaloFace_*_*',
        'keep *_ak5JetExtender_*_*',
        'keep *_ak7JetTracksAssociatorAtVertex_*_*',
        'keep *_ak7JetTracksAssociatorAtCaloFace_*_*',
        'keep *_ak7JetExtender_*_*',
        'keep *_ak5JetID_*_*',
        'keep *_ak7JetID_*_*',
        'keep *_sc5JetID_*_*',
        'keep *_sc7JetID_*_*',
        'keep *_ic5JetID_*_*',
        'keep *_ic7JetID_*_*',
        'keep *_gk5JetID_*_*',
        'keep *_gk7JetID_*_*',
        'keep *_kt4JetID_*_*',
        'keep *_kt6JetID_*_*',
        'keep *_ca4JetID_*_*',
        'keep *_ca6JetID_*_*',
        'keep *_kt4CaloJets_*_*',
        'keep *_kt6CaloJets_*_*',
        'keep *_ak5CaloJets_*_*',
        'keep *_ak7CaloJets_*_*',
        'keep *_iterativeCone5CaloJets_*_*',
        'keep *_iterativeCone15CaloJets_*_*',
        'keep *_kt4PFJets_*_*',
        'keep *_kt6PFJets_*_*',
        'keep *_ak5PFJets_*_*',
        'keep *_ak7PFJets_*_*',
        'keep *_iterativeCone5PFJets_*_*',
        'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_ak5TrackJets_*_*',
        'keep *_kt4TrackJets_*_*',
        'keep *_ak7BasicJets_*_*',
        'keep *_ak7CastorJetID_*_*')
)

process.RecoJetsRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_kt4CaloJets_*_*',
        'keep *_kt6CaloJets_*_*',
        'keep *_ak5CaloJets_*_*',
        'keep *_ak7CaloJets_*_*',
        'keep *_iterativeCone5CaloJets_*_*',
        'keep *_iterativeCone15CaloJets_*_*',
        'keep *_kt4PFJets_*_*',
        'keep *_kt6PFJets_*_*',
        'keep *_ak5PFJets_*_*',
        'keep *_ak7PFJets_*_*',
        'keep *_iterativeCone5PFJets_*_*',
        'keep *_JetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_ak5TrackJets_*_*',
        'keep *_kt4TrackJets_*_*',
        'keep recoRecoChargedRefCandidates_trackRefsForJets_*_*',
        'keep *_caloTowers_*_*',
        'keep *_towerMaker_*_*',
        'keep *_CastorTowerReco_*_*',
        'keep *_ic5JetTracksAssociatorAtVertex_*_*',
        'keep *_iterativeCone5JetTracksAssociatorAtVertex_*_*',
        'keep *_iterativeCone5JetTracksAssociatorAtCaloFace_*_*',
        'keep *_iterativeCone5JetExtender_*_*',
        'keep *_kt4JetTracksAssociatorAtVertex_*_*',
        'keep *_kt4JetTracksAssociatorAtCaloFace_*_*',
        'keep *_kt4JetExtender_*_*',
        'keep *_ak5JetTracksAssociatorAtVertex_*_*',
        'keep *_ak5JetTracksAssociatorAtCaloFace_*_*',
        'keep *_ak5JetExtender_*_*',
        'keep *_ak7JetTracksAssociatorAtVertex_*_*',
        'keep *_ak7JetTracksAssociatorAtCaloFace_*_*',
        'keep *_ak7JetExtender_*_*',
        'keep *_ak5JetID_*_*',
        'keep *_ak7JetID_*_*',
        'keep *_ic5JetID_*_*',
        'keep *_kt4JetID_*_*',
        'keep *_kt6JetID_*_*',
        'keep *_ak7BasicJets_*_*',
        'keep *_ak7CastorJetID_*_*')
)

process.RecoLocalCaloAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_castorreco_*_*',
        'keep *_reducedHcalRecHits_*_*')
)

process.RecoLocalCaloFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_hbhereco_*_*',
        'keep *_hbheprereco_*_*',
        'keep *_hfreco_*_*',
        'keep *_horeco_*_*',
        'keep HBHERecHitsSorted_hbherecoMB_*_*',
        'keep HBHERecHitsSorted_hbheprerecoMB_*_*',
        'keep HORecHitsSorted_horecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMB_*_*',
        'keep ZDCDataFramesSorted_*Digis_*_*',
        'keep ZDCRecHitsSorted_*_*_*',
        'keep *_reducedHcalRecHits_*_*',
        'keep *_castorreco_*_*',
        'keep *_ecalGlobalUncalibRecHit_*_*',
        'keep *_ecalPreshowerRecHit_*_*',
        'keep *_ecalRecHit_*_*')
)

process.RecoLocalCaloRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_hbhereco_*_*',
        'keep *_hfreco_*_*',
        'keep *_horeco_*_*',
        'keep HBHERecHitsSorted_hbherecoMB_*_*',
        'keep HORecHitsSorted_horecoMB_*_*',
        'keep HFRecHitsSorted_hfrecoMB_*_*',
        'keep ZDCDataFramesSorted_*Digis_*_*',
        'keep ZDCRecHitsSorted_*_*_*',
        'keep *_reducedHcalRecHits_*_*',
        'keep *_castorreco_*_*',
        'keep *_ecalPreshowerRecHit_*_*',
        'keep *_ecalRecHit_*_*',
        'keep *_ecalCompactTrigPrim_*_*',
        'keep *_ecalTPSkim_*_*')
)

process.RecoLocalMuonAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.RecoLocalMuonFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_dt1DRecHits_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*')
)

process.RecoLocalMuonRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_dt1DRecHits_*_*',
        'keep *_dt4DSegments_*_*',
        'keep *_dt1DCosmicRecHits_*_*',
        'keep *_dt4DCosmicSegments_*_*',
        'keep *_csc2DRecHits_*_*',
        'keep *_cscSegments_*_*',
        'keep *_rpcRecHits_*_*')
)

process.RecoLocalTrackerAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.RecoLocalTrackerFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep DetIdedmEDCollection_siStripDigis_*_*',
        'keep *_siPixelClusters_*_*',
        'keep *_siStripClusters_*_*')
)

process.RecoLocalTrackerRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep DetIdedmEDCollection_siStripDigis_*_*',
        'keep *_siPixelClusters_*_*',
        'keep *_siStripClusters_*_*')
)

process.RecoMETAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoCaloMETs_met_*_*',
        'keep recoCaloMETs_metNoHF_*_*',
        'keep recoCaloMETs_metHO_*_*',
        'keep recoCaloMETs_corMetGlobalMuons_*_*',
        'keep recoMETs_htMetAK5_*_*',
        'keep recoMETs_htMetAK7_*_*',
        'keep recoMETs_htMetIC5_*_*',
        'keep recoMETs_htMetKT4_*_*',
        'keep recoMETs_htMetKT6_*_*',
        'keep recoMETs_tcMet_*_*',
        'keep recoMETs_tcMetWithPFclusters_*_*',
        'keep recoPFMETs_pfMet_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*',
        'drop recoHcalNoiseRBXs_*_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*',
        'keep *GlobalHaloData_*_*_*',
        'keep *BeamHaloSummary_BeamHaloSummary_*_*')
)

process.RecoMETFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoCaloMETs_met_*_*',
        'keep recoCaloMETs_metNoHF_*_*',
        'keep recoCaloMETs_metHO_*_*',
        'keep recoCaloMETs_corMetGlobalMuons_*_*',
        'keep recoCaloMETs_metNoHFHO_*_*',
        'keep recoCaloMETs_metOptHO_*_*',
        'keep recoCaloMETs_metOpt_*_*',
        'keep recoCaloMETs_metOptNoHFHO_*_*',
        'keep recoCaloMETs_metOptNoHF_*_*',
        'keep recoMETs_htMetAK5_*_*',
        'keep recoMETs_htMetAK7_*_*',
        'keep recoMETs_htMetIC5_*_*',
        'keep recoMETs_htMetKT4_*_*',
        'keep recoMETs_htMetKT6_*_*',
        'keep recoMETs_tcMet_*_*',
        'keep recoMETs_tcMetWithPFclusters_*_*',
        'keep recoPFMETs_pfMet_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*',
        'keep recoHcalNoiseRBXs_hcalnoise_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*',
        'keep *HaloData_*_*_*',
        'keep *BeamHaloSummary_BeamHaloSummary_*_*')
)

process.RecoMETRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoCaloMETs_met_*_*',
        'keep recoCaloMETs_metNoHF_*_*',
        'keep recoCaloMETs_metHO_*_*',
        'keep recoCaloMETs_corMetGlobalMuons_*_*',
        'keep recoCaloMETs_metNoHFHO_*_*',
        'keep recoCaloMETs_metOptHO_*_*',
        'keep recoCaloMETs_metOpt_*_*',
        'keep recoCaloMETs_metOptNoHFHO_*_*',
        'keep recoCaloMETs_metOptNoHF_*_*',
        'keep recoMETs_htMetAK5_*_*',
        'keep recoMETs_htMetAK7_*_*',
        'keep recoMETs_htMetIC5_*_*',
        'keep recoMETs_htMetKT4_*_*',
        'keep recoMETs_htMetKT6_*_*',
        'keep recoMETs_tcMet_*_*',
        'keep recoMETs_tcMetWithPFclusters_*_*',
        'keep recoPFMETs_pfMet_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonMETValueMapProducer_*_*',
        'keep recoMuonMETCorrectionDataedmValueMap_muonTCMETValueMapProducer_*_*',
        'keep recoHcalNoiseRBXs_hcalnoise_*_*',
        'keep HcalNoiseSummary_hcalnoise_*_*',
        'keep *HaloData_*_*_*',
        'keep *BeamHaloSummary_BeamHaloSummary_*_*')
)

process.RecoMuonAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoTracks_standAloneMuons_*_*',
        'keep recoTrackExtras_standAloneMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneMuons_*_*',
        'keep recoTracks_globalMuons_*_*',
        'keep recoTrackExtras_globalMuons_*_*',
        'keep recoTracks_tevMuons_*_*',
        'keep recoTrackExtras_tevMuons_*_*',
        'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*',
        'keep recoMuonTimeExtraedmValueMap_muons_*_*',
        'keep *_muonShowerInformation_*_*',
        'keep recoTracks_cosmicMuons_*_*',
        'keep recoTracks_globalCosmicMuons_*_*',
        'keep recoMuons_muonsFromCosmics_*_*',
        'keep recoTracks_cosmicMuons1Leg_*_*',
        'keep recoTracks_globalCosmicMuons1Leg_*_*',
        'keep recoMuons_muonsFromCosmics1Leg_*_*',
        'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*',
        'keep uintedmValueMap_cosmicsVeto_*_*',
        'keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*')
)

process.RecoMuonFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_MuonSeed_*_*',
        'keep *_ancientMuonSeed_*_*',
        'keep *_mergedStandAloneMuonSeeds_*_*',
        'keep TrackingRecHitsOwned_globalMuons_*_*',
        'keep TrackingRecHitsOwned_tevMuons_*_*',
        'keep recoCaloMuons_calomuons_*_*',
        'keep *_CosmicMuonSeed_*_*',
        'keep recoTrackExtras_cosmicMuons_*_*',
        'keep TrackingRecHitsOwned_cosmicMuons_*_*',
        'keep recoTrackExtras_globalCosmicMuons_*_*',
        'keep TrackingRecHitsOwned_globalCosmicMuons_*_*',
        'keep recoTrackExtras_cosmicMuons1Leg_*_*',
        'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*',
        'keep recoTrackExtras_globalCosmicMuons1Leg_*_*',
        'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*',
        'keep recoTracks_cosmicsVetoTracks_*_*',
        'keep *_SETMuonSeed_*_*',
        'keep recoTracks_standAloneSETMuons_*_*',
        'keep recoTrackExtras_standAloneSETMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneSETMuons_*_*',
        'keep recoTracks_globalSETMuons_*_*',
        'keep recoTrackExtras_globalSETMuons_*_*',
        'keep TrackingRecHitsOwned_globalSETMuons_*_*',
        'keep recoMuons_muonsWithSET_*_*',
        'keep recoTracks_standAloneMuons_*_*',
        'keep recoTrackExtras_standAloneMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneMuons_*_*',
        'keep recoTracks_globalMuons_*_*',
        'keep recoTrackExtras_globalMuons_*_*',
        'keep recoTracks_tevMuons_*_*',
        'keep recoTrackExtras_tevMuons_*_*',
        'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*',
        'keep recoMuonTimeExtraedmValueMap_muons_*_*',
        'keep *_muonShowerInformation_*_*',
        'keep recoTracks_cosmicMuons_*_*',
        'keep recoTracks_globalCosmicMuons_*_*',
        'keep recoMuons_muonsFromCosmics_*_*',
        'keep recoTracks_cosmicMuons1Leg_*_*',
        'keep recoTracks_globalCosmicMuons1Leg_*_*',
        'keep recoMuons_muonsFromCosmics1Leg_*_*',
        'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*',
        'keep uintedmValueMap_cosmicsVeto_*_*',
        'keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*',
        'keep *_muGlobalIsoDepositCtfTk_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*',
        'keep *_muGlobalIsoDepositJets_*_*')
)

process.RecoMuonIsolationAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*')
)

process.RecoMuonIsolationFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*',
        'keep *_muGlobalIsoDepositCtfTk_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*',
        'keep *_muGlobalIsoDepositJets_*_*')
)

process.RecoMuonIsolationParamGlobal = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_muParamGlobalIsoDepositGsTk_*_*',
        'keep *_muParamGlobalIsoDepositCalEcal_*_*',
        'keep *_muParamGlobalIsoDepositCalHcal_*_*',
        'keep *_muParamGlobalIsoDepositCtfTk_*_*',
        'keep *_muParamGlobalIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muParamGlobalIsoDepositCalByAssociatorHits_*_*',
        'keep *_muParamGlobalIsoDepositJets_*_*')
)

process.RecoMuonIsolationRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*',
        'keep *_muGlobalIsoDepositCtfTk_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*',
        'keep *_muGlobalIsoDepositJets_*_*')
)

process.RecoMuonRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_MuonSeed_*_*',
        'keep *_ancientMuonSeed_*_*',
        'keep *_mergedStandAloneMuonSeeds_*_*',
        'keep TrackingRecHitsOwned_globalMuons_*_*',
        'keep TrackingRecHitsOwned_tevMuons_*_*',
        'keep recoCaloMuons_calomuons_*_*',
        'keep *_CosmicMuonSeed_*_*',
        'keep recoTrackExtras_cosmicMuons_*_*',
        'keep TrackingRecHitsOwned_cosmicMuons_*_*',
        'keep recoTrackExtras_globalCosmicMuons_*_*',
        'keep TrackingRecHitsOwned_globalCosmicMuons_*_*',
        'keep recoTrackExtras_cosmicMuons1Leg_*_*',
        'keep TrackingRecHitsOwned_cosmicMuons1Leg_*_*',
        'keep recoTrackExtras_globalCosmicMuons1Leg_*_*',
        'keep TrackingRecHitsOwned_globalCosmicMuons1Leg_*_*',
        'keep recoTracks_cosmicsVetoTracks_*_*',
        'keep *_SETMuonSeed_*_*',
        'keep recoTracks_standAloneSETMuons_*_*',
        'keep recoTrackExtras_standAloneSETMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneSETMuons_*_*',
        'keep recoTracks_globalSETMuons_*_*',
        'keep recoTrackExtras_globalSETMuons_*_*',
        'keep TrackingRecHitsOwned_globalSETMuons_*_*',
        'keep recoMuons_muonsWithSET_*_*',
        'keep recoTracks_standAloneMuons_*_*',
        'keep recoTrackExtras_standAloneMuons_*_*',
        'keep TrackingRecHitsOwned_standAloneMuons_*_*',
        'keep recoTracks_globalMuons_*_*',
        'keep recoTrackExtras_globalMuons_*_*',
        'keep recoTracks_tevMuons_*_*',
        'keep recoTrackExtras_tevMuons_*_*',
        'keep recoTracksToOnerecoTracksAssociation_tevMuons_*_*',
        'keep recoTracks_generalTracks_*_*',
        'keep recoMuons_muons_*_*',
        'keep booledmValueMap_muid*_*_*',
        'keep recoMuonTimeExtraedmValueMap_muons_*_*',
        'keep *_muonShowerInformation_*_*',
        'keep recoTracks_cosmicMuons_*_*',
        'keep recoTracks_globalCosmicMuons_*_*',
        'keep recoMuons_muonsFromCosmics_*_*',
        'keep recoTracks_cosmicMuons1Leg_*_*',
        'keep recoTracks_globalCosmicMuons1Leg_*_*',
        'keep recoMuons_muonsFromCosmics1Leg_*_*',
        'keep recoMuonCosmicCompatibilityedmValueMap_cosmicsVeto_*_*',
        'keep uintedmValueMap_cosmicsVeto_*_*',
        'keep *_muIsoDepositTk_*_*',
        'keep *_muIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muIsoDepositCalByAssociatorHits_*_*',
        'keep *_muIsoDepositJets_*_*',
        'keep *_muGlobalIsoDepositCtfTk_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorTowers_*_*',
        'keep *_muGlobalIsoDepositCalByAssociatorHits_*_*',
        'keep *_muGlobalIsoDepositJets_*_*')
)

process.RecoParticleFlowAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('drop CaloTowersSorted_towerMakerPF_*_*',
        'drop *_pfElectronTranslator_*_*',
        'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*',
        'keep recoPFCandidates_particleFlow_*_*',
        'keep recoCaloClusters_pfElectronTranslator_*_*',
        'keep recoPreshowerClusters_pfElectronTranslator_*_*',
        'keep recoSuperClusters_pfElectronTranslator_*_*')
)

process.RecoParticleFlowFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('drop CaloTowersSorted_towerMakerPF_*_*',
        'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*',
        'keep recoPFClusters_particleFlowClusterECAL_*_*',
        'keep recoPFClusters_particleFlowClusterHCAL_*_*',
        'keep recoPFClusters_particleFlowClusterHFEM_*_*',
        'keep recoPFClusters_particleFlowClusterHFHAD_*_*',
        'keep recoPFClusters_particleFlowClusterPS_*_*',
        'keep recoPFBlocks_particleFlowBlock_*_*',
        'keep recoPFCandidates_particleFlow_*_*',
        'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*',
        'keep *_pfElectronTranslator_*_*',
        'keep *_trackerDrivenElectronSeeds_preid_*')
)

process.RecoParticleFlowRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('drop CaloTowersSorted_towerMakerPF_*_*',
        'keep recoPFRecHits_particleFlowClusterECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFEM_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterHFHAD_Cleaned_*',
        'keep recoPFRecHits_particleFlowClusterPS_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitECAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitHCAL_Cleaned_*',
        'keep recoPFRecHits_particleFlowRecHitPS_Cleaned_*',
        'keep recoPFClusters_particleFlowClusterECAL_*_*',
        'keep recoPFClusters_particleFlowClusterHCAL_*_*',
        'keep recoPFClusters_particleFlowClusterPS_*_*',
        'keep recoPFBlocks_particleFlowBlock_*_*',
        'keep recoPFCandidates_particleFlow_*_*',
        'keep recoPFDisplacedVertexs_particleFlowDisplacedVertex_*_*',
        'keep *_pfElectronTranslator_*_*',
        'keep *_trackerDrivenElectronSeeds_preid_*')
)

process.RecoPixelVertexingFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_pixelTracks_*_*',
        'keep *_pixelVertices_*_*')
)

process.RecoPixelVertexingRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_pixelTracks_*_*',
        'keep *_pixelVertices_*_*')
)

process.RecoTauTagAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_ak5PFJetsRecoTauPiZeros_*_*',
        'keep *_hpsPFTauProducer*_*_*',
        'keep *_hpsPFTauDiscrimination*_*_*',
        'keep *_shrinkingConePFTauProducer*_*_*',
        'keep *_shrinkingConePFTauDiscrimination*_*_*',
        'keep *_hpsTancTaus_*_*',
        'keep *_hpsTancTausDiscrimination*_*_*',
        'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_caloRecoTauTagInfoProducer_*_*',
        'keep recoCaloTaus_caloRecoTauProducer*_*_*',
        'keep *_caloRecoTauDiscrimination*_*_*')
)

process.RecoTauTagFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_ak5PFJetsRecoTauPiZeros_*_*',
        'keep *_hpsPFTauProducer*_*_*',
        'keep *_hpsPFTauDiscrimination*_*_*',
        'keep *_shrinkingConePFTauProducer*_*_*',
        'keep *_shrinkingConePFTauDiscrimination*_*_*',
        'keep *_hpsTancTaus_*_*',
        'keep *_hpsTancTausDiscrimination*_*_*',
        'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_caloRecoTauTagInfoProducer_*_*',
        'keep recoCaloTaus_caloRecoTauProducer*_*_*',
        'keep *_caloRecoTauDiscrimination*_*_*')
)

process.RecoTauTagRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_ak5PFJetsRecoTauPiZeros_*_*',
        'keep *_hpsPFTauProducer*_*_*',
        'keep *_hpsPFTauDiscrimination*_*_*',
        'keep *_shrinkingConePFTauProducer*_*_*',
        'keep *_shrinkingConePFTauDiscrimination*_*_*',
        'keep *_hpsTancTaus_*_*',
        'keep *_hpsTancTausDiscrimination*_*_*',
        'keep *_TCTauJetPlusTrackZSPCorJetAntiKt5_*_*',
        'keep *_caloRecoTauTagInfoProducer_*_*',
        'keep recoCaloTaus_caloRecoTauProducer*_*_*',
        'keep *_caloRecoTauDiscrimination*_*_*')
)

process.RecoTrackerAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoTracks_generalTracks_*_*',
        'keep recoTracks_rsWithMaterialTracks_*_*',
        'keep recoTracks_beamhaloTracks_*_*',
        'keep recoTracks_regionalCosmicTracks_*_*',
        'keep recoTracks_ctfPixelLess_*_*',
        'keep *_dedxHarmonic2_*_*',
        'keep *_trackExtrapolator_*_*')
)

process.RecoTrackerFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoTracks_generalTracks_*_*',
        'keep recoTrackExtras_generalTracks_*_*',
        'keep TrackingRecHitsOwned_generalTracks_*_*',
        'keep recoTracks_beamhaloTracks_*_*',
        'keep recoTrackExtras_beamhaloTracks_*_*',
        'keep TrackingRecHitsOwned_beamhaloTracks_*_*',
        'keep recoTracks_regionalCosmicTracks_*_*',
        'keep recoTrackExtras_regionalCosmicTracks_*_*',
        'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*',
        'keep recoTracks_rsWithMaterialTracks_*_*',
        'keep recoTrackExtras_rsWithMaterialTracks_*_*',
        'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*',
        'keep *_ctfPixelLess_*_*',
        'keep *_dedxTruncated40_*_*',
        'keep *_dedxMedian_*_*',
        'keep *_dedxHarmonic2_*_*',
        'keep *_trackExtrapolator_*_*')
)

process.RecoTrackerRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoTracks_generalTracks_*_*',
        'keep recoTrackExtras_generalTracks_*_*',
        'keep TrackingRecHitsOwned_generalTracks_*_*',
        'keep recoTracks_beamhaloTracks_*_*',
        'keep recoTrackExtras_beamhaloTracks_*_*',
        'keep TrackingRecHitsOwned_beamhaloTracks_*_*',
        'keep recoTracks_regionalCosmicTracks_*_*',
        'keep recoTrackExtras_regionalCosmicTracks_*_*',
        'keep TrackingRecHitsOwned_regionalCosmicTracks_*_*',
        'keep recoTracks_rsWithMaterialTracks_*_*',
        'keep recoTrackExtras_rsWithMaterialTracks_*_*',
        'keep TrackingRecHitsOwned_rsWithMaterialTracks_*_*',
        'keep *_ctfPixelLess_*_*',
        'keep *_dedxTruncated40_*_*',
        'keep *_dedxMedian_*_*',
        'keep *_dedxHarmonic2_*_*',
        'keep *_trackExtrapolator_*_*')
)

process.RecoVertexAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep  *_offlinePrimaryVertices_*_*',
        'keep  *_offlinePrimaryVerticesWithBS_*_*',
        'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*',
        'keep  *_nuclearInteractionMaker_*_*',
        'keep *_generalV0Candidates_*_*')
)

process.RecoVertexFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep  *_offlinePrimaryVertices_*_*',
        'keep  *_offlinePrimaryVerticesWithBS_*_*',
        'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*',
        'keep  *_nuclearInteractionMaker_*_*',
        'keep *_generalV0Candidates_*_*')
)

process.RecoVertexRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep  *_offlinePrimaryVertices_*_*',
        'keep  *_offlinePrimaryVerticesWithBS_*_*',
        'keep  *_offlinePrimaryVerticesFromCosmicTracks_*_*',
        'keep  *_nuclearInteractionMaker_*_*',
        'keep *_generalV0Candidates_*_*')
)

process.SimCalorimetryAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.SimCalorimetryFEVTDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_simEcalDigis_*_*',
        'keep *_simEcalPreshowerDigis_*_*',
        'keep *_simEcalTriggerPrimitiveDigis_*_*',
        'keep *_simHcalDigis_*_*',
        'keep ZDCDataFramesSorted_simHcalUnsuppressedDigis_*_*',
        'keep *_simHcalTriggerPrimitiveDigis_*_*')
)

process.SimCalorimetryRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep EBSrFlagsSorted_simEcalDigis_*_*',
        'keep EESrFlagsSorted_simEcalDigis_*_*')
)

process.SimCalorimetryRECO = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.SimG4CoreAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.SimG4CoreRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_g4SimHits_*_*',
        'keep edmHepMCProduct_source_*_*')
)

process.SimG4CoreRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep edmHepMCProduct_source_*_*',
        'keep SimTracks_g4SimHits_*_*',
        'keep SimVertexs_g4SimHits_*_*')
)

process.SimGeneralAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep PileupSummaryInfos_*_*_*')
)

process.SimGeneralFEVTDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('drop *_trackingtruthprod_*_*',
        'drop *_electrontruth_*_*',
        'keep *_mergedtruth_MergedTrackTruth_*',
        'keep CrossingFramePlaybackInfoExtended_*_*_*')
)

process.SimGeneralRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep CrossingFramePlaybackInfoExtended_*_*_*',
        'keep PileupSummaryInfos_*_*_*')
)

process.SimGeneralRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep PileupSummaryInfos_*_*_*')
)

process.SimMuonAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.SimMuonFEVTDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_simMuonCSCDigis_*_*',
        'keep *_simMuonDTDigis_*_*',
        'keep *_simMuonRPCDigis_*_*')
)

process.SimMuonRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep CSCDetIdCSCComparatorDigiMuonDigiCollection_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*')
)

process.SimMuonRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep StripDigiSimLinkedmDetSetVector_simMuonCSCDigis_*_*',
        'keep DTLayerIdDTDigiSimLinkMuonDigiCollection_simMuonDTDigis_*_*',
        'keep RPCDigiSimLinkedmDetSetVector_simMuonRPCDigis_*_*')
)

process.SimTrackerAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_allTrackMCMatch_*_*')
)

process.SimTrackerDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('keep PixelDigiSimLinkedmDetSetVector_simSiPixelDigis_*_*',
        'keep StripDigiSimLinkedmDetSetVector_simSiStripDigis_*_*',
        'keep *_allTrackMCMatch_*_*')
)

process.SimTrackerFEVTDEBUG = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_simSiPixelDigis_*_*',
        'keep *_simSiStripDigis_*_*',
        'keep *_allTrackMCMatch_*_*',
        'keep *_trackingParticleRecoTrackAsssociation_*_*',
        'keep *_assoc2secStepTk_*_*',
        'keep *_assoc2thStepTk_*_*',
        'keep *_assoc2GsfTracks_*_*',
        'keep *_assocOutInConversionTracks_*_*',
        'keep *_assocInOutConversionTracks_*_*')
)

process.SimTrackerRAW = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_allTrackMCMatch_*_*')
)

process.SimTrackerRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_allTrackMCMatch_*_*')
)

process.TrackingToolsAOD = cms.PSet(
    outputCommands = cms.untracked.vstring('keep recoTracks_GsfGlobalElectronTest_*_*',
        'keep recoGsfTracks_electronGsfTracks_*_*')
)

process.TrackingToolsFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_CkfElectronCandidates_*_*',
        'keep *_GsfGlobalElectronTest_*_*',
        'keep *_electronMergedSeeds_*_*',
        'keep *_electronGsfTracks_*_*')
)

process.TrackingToolsRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_CkfElectronCandidates_*_*',
        'keep *_GsfGlobalElectronTest_*_*',
        'keep *_electronMergedSeeds_*_*',
        'keep recoGsfTracks_electronGsfTracks_*_*',
        'keep recoGsfTrackExtras_electronGsfTracks_*_*',
        'keep recoTrackExtras_electronGsfTracks_*_*',
        'keep TrackingRecHitsOwned_electronGsfTracks_*_*')
)

process.VtxSmearedCommon = cms.PSet(
    src = cms.InputTag("generator")
)

process.common_heavy_suppression = cms.PSet(
    NeutronThreshold = cms.double(30.0),
    ProtonThreshold = cms.double(30.0),
    IonThreshold = cms.double(30.0)
)

process.common_maximum_time = cms.PSet(
    MaxTrackTime = cms.double(500.0),
    MaxTrackTimes = cms.vdouble(2000.0, 0.0, 0.0),
    MaxTimeNames = cms.vstring('ZDCRegion',
        'QuadRegion',
        'InterimRegion')
)

process.configurationMetadata = cms.untracked.PSet(
    version = cms.untracked.string('$Revision: 1.1 $'),
    name = cms.untracked.string('$Source: /cvs/CMSSW/CMSSW/Configuration/GenProduction/python/PYTHIA6_Bd2MuMu_TuneZ2_7TeV_cff.py,v $'),
    annotation = cms.untracked.string('Bd -> mu mu at 7TeV')
)

process.ecalLocalRecoAOD = cms.PSet(
    outputCommands = cms.untracked.vstring()
)

process.ecalLocalRecoFEVT = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_ecalGlobalUncalibRecHit_*_*',
        'keep *_ecalPreshowerRecHit_*_*',
        'keep *_ecalRecHit_*_*')
)

process.ecalLocalRecoRECO = cms.PSet(
    outputCommands = cms.untracked.vstring('keep *_ecalPreshowerRecHit_*_*',
        'keep *_ecalRecHit_*_*',
        'keep *_ecalCompactTrigPrim_*_*',
        'keep *_ecalTPSkim_*_*')
)

process.fieldScaling = cms.PSet(
    scalingVolumes = cms.vint32(14100, 14200, 17600, 17800, 17900,
        18100, 18300, 18400, 18600, 23100,
        23300, 23400, 23600, 23800, 23900,
        24100, 28600, 28800, 28900, 29100,
        29300, 29400, 29600, 28609, 28809,
        28909, 29109, 29309, 29409, 29609,
        28610, 28810, 28910, 29110, 29310,
        29410, 29610, 28611, 28811, 28911,
        29111, 29311, 29411, 29611),
    scalingFactors = cms.vdouble(1, 1, 0.994, 1.004, 1.004,
        1.005, 1.004, 1.004, 0.994, 0.965,
        0.958, 0.958, 0.953, 0.958, 0.958,
        0.965, 0.918, 0.924, 0.924, 0.906,
        0.924, 0.924, 0.918, 0.991, 0.998,
        0.998, 0.978, 0.998, 0.998, 0.991,
        0.991, 0.998, 0.998, 0.978, 0.998,
        0.998, 0.991, 0.991, 0.998, 0.998,
        0.978, 0.998, 0.998, 0.991)
)

process.maxEvents = cms.untracked.PSet(
    input = cms.untracked.int32(1)
)

process.mixCaloHits = cms.PSet(
    input = cms.VInputTag(cms.InputTag("g4SimHits","CaloHitsTk"), cms.InputTag("g4SimHits","CastorBU"), cms.InputTag("g4SimHits","CastorFI"), cms.InputTag("g4SimHits","CastorPL"), cms.InputTag("g4SimHits","CastorTU"),
        cms.InputTag("g4SimHits","EcalHitsEB"), cms.InputTag("g4SimHits","EcalHitsEE"), cms.InputTag("g4SimHits","EcalHitsES"), cms.InputTag("g4SimHits","EcalTBH4BeamHits"), cms.InputTag("g4SimHits","HcalHits"),
        cms.InputTag("g4SimHits","HcalTB06BeamHits"), cms.InputTag("g4SimHits","ZDCHITS")),
    type = cms.string('PCaloHit'),
    subdets = cms.vstring('CaloHitsTk',
        'CastorBU',
        'CastorFI',
        'CastorPL',
        'CastorTU',
        'EcalHitsEB',
        'EcalHitsEE',
        'EcalHitsES',
        'EcalTBH4BeamHits',
        'HcalHits',
        'HcalTB06BeamHits',
        'ZDCHITS')
)

process.mixHepMCProducts = cms.PSet(
    input = cms.VInputTag(cms.InputTag("generator")),
    type = cms.string('HepMCProduct')
)

process.mixPCFCaloHits = cms.PSet(
    input = cms.VInputTag(cms.InputTag("CFWriter","g4SimHitsCaloHitsTk"), cms.InputTag("CFWriter","g4SimHitsCastorBU"), cms.InputTag("CFWriter","g4SimHitsCastorFI"), cms.InputTag("CFWriter","g4SimHitsCastorPL"), cms.InputTag("CFWriter","g4SimHitsCastorTU"),
        cms.InputTag("CFWriter","g4SimHitsEcalHitsEB"), cms.InputTag("CFWriter","g4SimHitsEcalHitsEE"), cms.InputTag("CFWriter","g4SimHitsEcalHitsES"), cms.InputTag("CFWriter","g4SimHitsEcalTBH4BeamHits"), cms.InputTag("CFWriter","g4SimHitsHcalHits"),
        cms.InputTag("CFWriter","g4SimHitsHcalTB06BeamHits"), cms.InputTag("CFWriter","g4SimHitsZDCHITS")),
    type = cms.string('PCaloHitPCrossingFrame'),
    subdets = cms.vstring('CaloHitsTk',
        'CastorBU',
        'CastorFI',
        'CastorPL',
        'CastorTU',
        'EcalHitsEB',
        'EcalHitsEE',
        'EcalHitsES',
        'EcalTBH4BeamHits',
        'HcalHits',
        'HcalTB06BeamHits',
        'ZDCHITS')
)

process.mixPCFHepMCProducts = cms.PSet(
    input = cms.VInputTag(cms.InputTag("CFWriter","generator")),
    type = cms.string('HepMCProductPCrossingFrame')
)

process.mixPCFSimHits = cms.PSet(
    input = cms.VInputTag(cms.InputTag("CFWriter","g4SimHitsBSCHits"), cms.InputTag("CFWriter","g4SimHitsFP420SI"), cms.InputTag("CFWriter","g4SimHitsMuonCSCHits"), cms.InputTag("CFWriter","g4SimHitsMuonDTHits"), cms.InputTag("CFWriter","g4SimHitsMuonRPCHits"),
        cms.InputTag("CFWriter","g4SimHitsTotemHitsRP"), cms.InputTag("CFWriter","g4SimHitsTotemHitsT1"), cms.InputTag("CFWriter","g4SimHitsTotemHitsT2Gem"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsPixelBarrelHighTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsPixelBarrelLowTof"),
        cms.InputTag("CFWriter","g4SimHitsTrackerHitsPixelEndcapHighTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsPixelEndcapLowTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTECHighTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTECLowTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTIBHighTof"),
        cms.InputTag("CFWriter","g4SimHitsTrackerHitsTIBLowTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTIDHighTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTIDLowTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTOBHighTof"), cms.InputTag("CFWriter","g4SimHitsTrackerHitsTOBLowTof")),
    type = cms.string('PSimHitPCrossingFrame'),
    subdets = cms.vstring('BSCHits',
        'FP420SI',
        'MuonCSCHits',
        'MuonDTHits',
        'MuonRPCHits',
        'TotemHitsRP',
        'TotemHitsT1',
        'TotemHitsT2Gem',
        'TrackerHitsPixelBarrelHighTof',
        'TrackerHitsPixelBarrelLowTof',
        'TrackerHitsPixelEndcapHighTof',
        'TrackerHitsPixelEndcapLowTof',
        'TrackerHitsTECHighTof',
        'TrackerHitsTECLowTof',
        'TrackerHitsTIBHighTof',
        'TrackerHitsTIBLowTof',
        'TrackerHitsTIDHighTof',
        'TrackerHitsTIDLowTof',
        'TrackerHitsTOBHighTof',
        'TrackerHitsTOBLowTof')
)

process.mixPCFSimTracks = cms.PSet(
    input = cms.VInputTag(cms.InputTag("CFWriter","g4SimHits")),
    type = cms.string('SimTrackPCrossingFrame')
)

process.mixPCFSimVertices = cms.PSet(
    input = cms.VInputTag(cms.InputTag("CFWriter","g4SimHits")),
    type = cms.string('SimVertexPCrossingFrame')
)

process.mixSimHits = cms.PSet(
    input = cms.VInputTag(cms.InputTag("g4SimHits","BSCHits"), cms.InputTag("g4SimHits","FP420SI"), cms.InputTag("g4SimHits","MuonCSCHits"), cms.InputTag("g4SimHits","MuonDTHits"), cms.InputTag("g4SimHits","MuonRPCHits"),
        cms.InputTag("g4SimHits","TotemHitsRP"), cms.InputTag("g4SimHits","TotemHitsT1"), cms.InputTag("g4SimHits","TotemHitsT2Gem"), cms.InputTag("g4SimHits","TrackerHitsPixelBarrelHighTof"), cms.InputTag("g4SimHits","TrackerHitsPixelBarrelLowTof"),
        cms.InputTag("g4SimHits","TrackerHitsPixelEndcapHighTof"), cms.InputTag("g4SimHits","TrackerHitsPixelEndcapLowTof"), cms.InputTag("g4SimHits","TrackerHitsTECHighTof"), cms.InputTag("g4SimHits","TrackerHitsTECLowTof"), cms.InputTag("g4SimHits","TrackerHitsTIBHighTof"),
        cms.InputTag("g4SimHits","TrackerHitsTIBLowTof"), cms.InputTag("g4SimHits","TrackerHitsTIDHighTof"), cms.InputTag("g4SimHits","TrackerHitsTIDLowTof"), cms.InputTag("g4SimHits","TrackerHitsTOBHighTof"), cms.InputTag("g4SimHits","TrackerHitsTOBLowTof")),
    type = cms.string('PSimHit'),
    subdets = cms.vstring('BSCHits',
        'FP420SI',
        'MuonCSCHits',
        'MuonDTHits',
        'MuonRPCHits',
        'TotemHitsRP',
        'TotemHitsT1',
        'TotemHitsT2Gem',
        'TrackerHitsPixelBarrelHighTof',
        'TrackerHitsPixelBarrelLowTof',
        'TrackerHitsPixelEndcapHighTof',
        'TrackerHitsPixelEndcapLowTof',
        'TrackerHitsTECHighTof',
        'TrackerHitsTECLowTof',
        'TrackerHitsTIBHighTof',
        'TrackerHitsTIBLowTof',
        'TrackerHitsTIDHighTof',
        'TrackerHitsTIDLowTof',
        'TrackerHitsTOBHighTof',
        'TrackerHitsTOBLowTof')
)

process.mixSimTracks = cms.PSet(
    input = cms.VInputTag(cms.InputTag("g4SimHits")),
    type = cms.string('SimTrack')
)

process.mixSimVertices = cms.PSet(
    input = cms.VInputTag(cms.InputTag("g4SimHits")),
    type = cms.string('SimVertex')
)

process.options = cms.untracked.PSet(

)

process.schedule = cms.Schedule(process.generation_step,process.genfiltersummary_step,process.simulation_step,process.endjob_step,process.RAWSIMoutput_step)
