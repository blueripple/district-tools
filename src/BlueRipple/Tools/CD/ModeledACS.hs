{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
module BlueRipple.Tools.CD.ModeledACS
  (
    modeledACSByCD
  , acsByCDJointTableCompare
  , JointType(..)
  , CDKeyR
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
--import qualified Frames.Serialize as FS
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Misc as FM
import qualified Frames.Constraints as FC
import qualified Data.Vinyl.TypeLevel as V

import qualified Control.Foldl as FL
import qualified Frames.MapReduce as FMR
import Control.Lens (view)

--import qualified Data.Map.Strict as M

type CDKeyR = '[GT.StateAbbreviation] V.++ BRC.LDLocationR

tsModelConfig :: Text -> Int -> DTM3.ModelConfig
tsModelConfig modelId n =  DTM3.ModelConfig True (DTM3.dmr modelId n)
                           DTM3.AlphaHierNonCentered DTM3.ThetaSimple DTM3.NormalDist

data JointType = Prod | Modeled deriving stock (Show, Eq)

modeledACSByCD :: forall r . (K.KnitEffects r, BRCC.CacheEffects r) => JointType -> K.Sem r (K.ActionWithCacheTime r (DP.PSData CDKeyR))
modeledACSByCD jointType = do
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
                                               (Right "CASR_ASE_ByPUMA")
                                               (Right "model/demographic/casr_ase_PUMA")
                                               (DTM3.Model $ tsModelConfig "CASR_ASE_ByPUMA" 141)
                                               False -- do not whiten
                                               Nothing Nothing Nothing . fmap (fmap F.rcast)
  let optimalWeightsConfig = DTP.defaultOptimalWeightsAlgoConfig {DTP.owcMaxTimeM = Just 0.1, DTP.owcProbRelTolerance = 1e-4}
      optimalWeightsLogStyle = DTP.OWKLogLevel K.Diagnostic
  (acsCASERByCD, products) <- BRC.censusTablesCD 2024 BRC.TY2022
                              >>= DMC.predictedCensusCASER' DMC.stateAbbrFromFIPS
                              DMS.GMDensity
                              (DTP.viaOptimalWeights optimalWeightsConfig optimalWeightsLogStyle DTP.euclideanFull)
                              (Right "model/cd2024/cdDemographics")
                              jointFromMarginalPredictorCSR_ASR_C
                              jointFromMarginalPredictorCASR_ASE_C
  let outputCK t = "model/cd2024/data/cd2024_ACS2022_" <> t <> "PSData.bin"
      (ck, tables_C) = case jointType of
        Prod -> (outputCK "products", products)
        Modeled -> (outputCK "modeled", acsCASERByCD)
  BRCC.retrieveOrMakeD ck tables_C
    $ \x -> DP.PSData . fmap F.rcast <$> (BRL.addStateAbbrUsingFIPS $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)


acsTablePopAndPWD :: forall ks . (Ord (F.Record ks)
                                 , ks F.⊆ DP.PSDataR ks
                                 , FC.ElemsOf (DP.PSDataR ks) '[DT.PopCount, DT.PWPopPerSqMile]
                                 , FSI.RecVec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                                 )
                  => (F.Record ks -> Bool) -> FL.Fold (F.Record (DP.PSDataR ks)) (F.FrameRec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
acsTablePopAndPWD f = FMR.concatFold
                      $ FMR.mapReduceFold
                      (FMR.unpackFilterRow $ f . F.rcast)
                      (FMR.assignKeysAndData @ks @[DT.PopCount, DT.PWPopPerSqMile])
                      (FMR.foldAndAddKey $ DT.pwDensityAndPopFldRec DT.Geometric)

acsByCDJointTableCompare :: (K.KnitEffects r, BRCC.CacheEffects r) => (F.Record CDKeyR -> Bool) -> K.Sem r ()
acsByCDJointTableCompare f = do
  prod <- DP.unPSData <$> (K.ignoreCacheTimeM $ modeledACSByCD Prod)
  modeled <- DP.unPSData <$> (K.ignoreCacheTimeM $ modeledACSByCD Modeled)
  let popAndPWD = FL.fold (acsTablePopAndPWD @CDKeyR f)
      merge k lPP rPP = (k, lPP, rPP)
  merged <- K.knitEither $ FM.mapMergeFramesE @CDKeyR merge show "products" "modeled" (popAndPWD prod) (popAndPWD modeled)
  K.logLE K.Info $ show merged
  pure ()
