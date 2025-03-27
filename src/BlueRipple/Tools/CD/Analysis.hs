{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module BlueRipple.Tools.CD.Analysis
  (
    module BlueRipple.Tools.CD.Analysis
  )
where


import qualified BlueRipple.Model.CategorizeElection as CE
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Data.Redistricting as BLR
import qualified BlueRipple.Data.DistrictOverlaps as DO
import qualified BlueRipple.Data.ACS_Tables as BRC
import qualified BlueRipple.Data.Small.DataFrames as BSD

import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.Types.Modeling as MT
import qualified BlueRipple.Data.Small.Loaders as BRS

import qualified BlueRipple.Data.CachingCore as BRCC

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
modelAndDRA modeled draSLD _upperOnlyMap _singleCDMap sa = do
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


modeledMapToFrame :: MC.PSMap MACS.SLDKeyR MT.ConfidenceInterval -> F.FrameRec ModeledR
modeledMapToFrame = F.toFrame . fmap (\(k, ci) -> k F.<+> FT.recordSingleton @MR.ModelCI ci) . M.toList . MC.unPSMap
