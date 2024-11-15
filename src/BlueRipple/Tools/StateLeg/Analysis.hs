{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module BlueRipple.Tools.StateLeg.Analysis
  (
    module BlueRipple.Tools.StateLeg.Analysis
  , module BlueRipple.Tools.StateLeg.ModeledACS
  )
where


import qualified BlueRipple.Tools.StateLeg.ModeledACS as MACS
import BlueRipple.Tools.StateLeg.ModeledACS (JointType(..))
import qualified BlueRipple.Data.Small.Loaders as BDL
import qualified BlueRipple.Model.CategorizeElection as CE
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Data.Redistricting as BLR
--import qualified BlueRipple.Data.RedistrictingTables as BLR
import qualified BlueRipple.Data.DistrictOverlaps as DO
--import qualified BlueRipple.Data.ACS_Tables_Loaders as BRC
import qualified BlueRipple.Data.ACS_Tables as BRC
import qualified BlueRipple.Data.Small.DataFrames as BSD

--import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.Types.Modeling as MT
import qualified BlueRipple.Data.Small.Loaders as BRS

--import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.CachingCore as BRCC
--import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Frames as F
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Streamly.OrMissing as FOM
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Constraints as FC
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Control.Foldl as FL
import qualified Data.Map.Strict as M
import Control.Lens (view, (^.))

import qualified Knit.Report as K

type OrMissingInt = FOM.OrMissing Int
type OrMissingDouble = FOM.OrMissing Double


FTH.declareColumn "CD" ''OrMissingInt
FTH.declareColumn "CDPPL" ''OrMissingDouble


type ModelDRA_R = (FJ.JoinResult [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
                   (MACS.SLDKeyR V.++ '[MR.ModelCI]) BLR.DRAnalysisR)
                  V.++ '[CE.DistCategory]


modelAndDRA :: K.KnitEffects r
            => MC.PSMap MACS.SLDKeyR MT.ConfidenceInterval
            -> F.Frame BLR.DRAnalysis
            -> Map Text Bool
            -> Map Text Bool
            -> Text
            -> K.Sem r (F.FrameRec ModelDRA_R)
modelAndDRA modeled draSLD upperOnlyMap singleCDMap sa = do
  let draSLD_forState = F.filterFrame ((== sa) . view GT.stateAbbreviation) draSLD
      (modeledAndDRA, missingModelDRA)
          = FJ.leftJoinWithMissing @[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName] (modeledMapToFrame modeled) draSLD_forState
  when (not $ null missingModelDRA) $ K.knitError $ "analyzeState: Missing keys in modeledDVs/dra join: " <> show missingModelDRA
  let (safe, lean, tilt) = (0.1, 0.05, 0.02)
      lrF = CE.leanRating safe lean tilt
      compText :: (FC.ElemsOf rs [ET.DemShare, MR.ModelCI]) => F.Record rs -> Text
      compText r =
        let lrPPL = lrF (r ^. ET.demShare)
            lrDPL = lrF $ MT.ciMid . view MR.modelCI $ r
        in CE.pPLAndDPL lrPPL lrDPL
      compareOn f x y = compare (f x) (f y)
      compareRows x y = compareOn (view GT.stateAbbreviation) x y
                        <> compareOn (view GT.districtTypeC) x y
                        <> GT.districtNameCompare (x ^. GT.districtName) (y ^. GT.districtName)
      competitivePPL r = let x = r ^. ET.demShare in x >= 0.4 && x <= 0.6
      sortAndFilter = F.toFrame . sortBy compareRows . filter competitivePPL . FL.fold FL.list
      withModelComparison = fmap (\r -> r F.<+> FT.recordSingleton @CE.DistCategory (compText r)) $ sortAndFilter modeledAndDRA
  pure withModelComparison

type ModelDRAOverlap_R = ModelDRA_R V.++ [DO.Overlap, CD, CDPPL]

modelAndDRAWithOverlaps :: (K.KnitEffects r, BRCC.CacheEffects r)
                        => F.FrameRec ModelDRA_R
                        -> F.Frame BLR.DRAnalysis
                        -> Map Text Bool
                        -> Map Text Bool
                        -> Text
                        -> K.Sem r (F.FrameRec ModelDRAOverlap_R)
modelAndDRAWithOverlaps modelAndDRA draCD upperOnlyMap singleCDMap sa = do
  let draCDPPLMap = FL.fold (FL.premap (\r -> (r ^. GT.districtName, r ^. ET.demShare )) FL.map)
                    $ F.filterFrame ((== sa) . view GT.stateAbbreviation) draCD
  maxOverlapsM <- fmap (>>= DO.maxSLD_CDOverlaps) $ DO.sldCDOverlaps upperOnlyMap singleCDMap 2024 BRC.TY2021 sa
  case maxOverlapsM of
    Just maxOverlaps -> do
      let (withOverlaps, missingOverlaps)
            = FJ.leftJoinWithMissing @[GT.DistrictTypeC, GT.DistrictName] modelAndDRA maxOverlaps
      when (not $ null missingOverlaps) $ K.knitError $ "br-Gaba: Missing keys in modeledDVs+dra/overlaps join: " <> show missingOverlaps
      let cd r = r ^. GT.congressionalDistrict
          omCD = FT.recordSingleton @CD . FOM.Present . cd
          omCDPPL r = FT.recordSingleton @CDPPL $ FOM.toOrMissing $ M.lookup (show $ cd r) draCDPPLMap
          addBoth r = FT.mutate (\q -> omCD q F.<+> omCDPPL q) r
      pure $ fmap (F.rcast . addBoth) withOverlaps
    Nothing -> do
      let overlapCols :: F.Record [DO.Overlap, CD, CDPPL] = 1 F.&: FOM.Missing F.&: FOM.Missing F.&: V.RNil
          withOverlaps = FT.mutate (const overlapCols) <$> modelAndDRA
      pure withOverlaps

modelC :: (K.KnitEffects r, BRCC.CacheEffects r)
       => MACS.JointType
       -> MC.ActionConfig a b
       -> Maybe (MR.Scenario DP.PredictorsR)
       -> MC.PrefConfig b
       -> Maybe (MR.Scenario DP.PredictorsR)
       -> Text
       -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap MACS.SLDKeyR MT.ConfidenceInterval))
modelC jointType tc tScenarioM pc pScenarioM sa = do
  let cacheStructure sa' psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                                  psName "AllCells" sa'
      psDataForState :: Text -> DP.PSData MACS.SLDKeyR -> DP.PSData MACS.SLDKeyR
      psDataForState sa' = DP.PSData . F.filterFrame ((== sa') . view GT.stateAbbreviation) . DP.unPSData
  modeledACSBySLDPSData_C <- MACS.modeledACSBySLD jointType
  let stateSLDs_C = fmap (psDataForState sa) modeledACSBySLDPSData_C
  presidentialElections_C <- BRS.presidentialElectionsWithIncumbency
  draShareOverrides_C <- DP.loadOverrides (BLR.draDataPath <> "DRA_Shares/DRA_Share.csv") "DRA 2016-2021"
  let dVSPres2020 = DP.ElexTargetConfig "PresWO" draShareOverrides_C 2020 presidentialElections_C
      dVSModel psName
        = MR.runFullModelAH @MACS.SLDKeyR 2020 (cacheStructure sa psName) tc tScenarioM pc pScenarioM (MR.VoteDTargets dVSPres2020)
  dVSModel (sa <> "_SLD") stateSLDs_C

analyzeStateGaba :: (K.KnitEffects r, BRCC.CacheEffects r)
                 => MACS.JointType
                 -> MC.ActionConfig a b
                 -> Maybe (MR.Scenario DP.PredictorsR)
                 -> MC.PrefConfig b
                 -> Maybe (MR.Scenario DP.PredictorsR)
                 -> Map Text Bool
                 -> Map Text Bool
                 -> Text
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ModelDRAOverlap_R))
analyzeStateGaba jointType tc tScenarioM pc pScenarioM upperOnlyMap singleCDMap sa = do
  modeled_C <- modelC jointType tc tScenarioM pc pScenarioM sa --dVSModel (sa <> "_SLD") stateSLDs_C
  draSLD_C <- BLR.allPassedSLD 2024 BRC.TY2021
  let modeledAndDRA_C = flip K.wctBind ((,) <$> modeled_C <*> draSLD_C)
                        $ \(modeled, draSLD) -> modelAndDRA modeled draSLD upperOnlyMap singleCDMap sa
  draCD_C <- BLR.allPassedCongressional 2024 BRC.TY2021
  BRCC.retrieveOrMakeFrame ("gaba/" <> sa <> "_analysis.bin") ((,) <$> modeledAndDRA_C <*> draCD_C)
    $ \(modeledAndDRA, draCD) -> modelAndDRAWithOverlaps modeledAndDRA draCD upperOnlyMap singleCDMap sa


type ModeledR = MACS.SLDKeyR V.++ '[MR.ModelCI]

stateUpperOnlyMap :: (K.KnitEffects r, BRCC.CacheEffects r) => K.Sem r (Map Text Bool)
stateUpperOnlyMap = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. BSD.sLDUpperOnly)) FL.map)
                    <$> K.ignoreCacheTimeM BRS.stateAbbrCrosswalkLoader

modeledMapToFrame :: MC.PSMap MACS.SLDKeyR MT.ConfidenceInterval -> F.FrameRec ModeledR
modeledMapToFrame = F.toFrame . fmap (\(k, ci) -> k F.<+> FT.recordSingleton @MR.ModelCI ci) . M.toList . MC.unPSMap
