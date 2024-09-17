{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module BlueRipple.Tools.StateLeg.ModeledACS
  (
    modeledACSBySLD
  ) where


import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichCensus as DMC
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.ACS_Tables_Loaders as BRC
import qualified BlueRipple.Data.ACS_Tables as BRC
import qualified BlueRipple.Data.Small.Loaders as BRL
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Geographic as GT

import qualified BlueRipple.Data.CachingCore as BRCC
import qualified Knit.Report as K

import qualified Frames as F
import qualified Data.Vinyl.TypeLevel as V
import Control.Lens (view)

type SLDKeyR = '[GT.StateAbbreviation] V.++ BRC.LDLocationR

tsModelConfig :: Text -> Int -> DTM3.ModelConfig
tsModelConfig modelId n =  DTM3.ModelConfig True (DTM3.dmr modelId n)
                           DTM3.AlphaHierNonCentered DTM3.ThetaSimple DTM3.NormalDist

modeledACSBySLD :: forall r . (K.KnitEffects r, BRCC.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (DP.PSData SLDKeyR))
modeledACSBySLD = do
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
  (jointFromMarginalPredictorCSR_ASR_C, _) <- DDP.cachedACSa5ByPUMA  srcWindow cachedSrc 2022 -- most recent available
                                              >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                              (Right "CSR_ASR_ByPUMA")
                                              (Right "model/demographic/csr_asr_PUMA")
                                              (DTM3.Model $ tsModelConfig "CSR_ASR_ByPUMA" 71) -- use model not just mean
                                              False -- do not whiten
                                              Nothing Nothing Nothing . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _) <- DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2022 -- most recent available
                                               >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                               (Right "CASR_SER_ByPUMA")
                                               (Right "model/demographic/casr_ase_PUMA")
                                               (DTM3.Model $ tsModelConfig "CASR_ASE_ByPUMA" 141)
                                               False -- do not whiten
                                               Nothing Nothing Nothing . fmap (fmap F.rcast)
  let optimalWeightsConfig = DTP.defaultOptimalWeightsAlgoConfig {DTP.owcMaxTimeM = Just 0.1, DTP.owcProbRelTolerance = 1e-4}
      optimalWeightsLogStyle = DTP.OWKLogLevel K.Diagnostic
  (acsCASERBySLD, _products) <- BRC.censusTablesForSLDs 2024 BRC.TY2022
                                >>= DMC.predictedCensusCASER' DMC.stateAbbrFromFIPS
                                DMS.GMDensity
                                (DTP.viaOptimalWeights optimalWeightsConfig optimalWeightsLogStyle DTP.euclideanFull)
                                (Right "model/stateLeg2024/sldDemographics")
                                jointFromMarginalPredictorCSR_ASR_C
                                jointFromMarginalPredictorCASR_ASE_C
  BRCC.retrieveOrMakeD "model/stateLeg2024/data/sld2024_ACS2022_PSData.bin" acsCASERBySLD
    $ \x -> DP.PSData . fmap F.rcast <$> (BRL.addStateAbbrUsingFIPS $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)
