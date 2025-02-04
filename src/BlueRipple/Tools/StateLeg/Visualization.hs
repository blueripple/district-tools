{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings  #-}
module BlueRipple.Tools.StateLeg.Visualization
  (
    module BlueRipple.Tools.StateLeg.Visualization
  )
where

import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Types.Election as ET
import qualified BlueRipple.Data.Types.Modeling as MT
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

import qualified Frames as F
import qualified Frames.Constraints as FC
import qualified Control.Foldl as FL
import Control.Lens ((^.))

import qualified Knit.Report as K

import qualified Path

data LabeledFrame a rs = LabeledFrame { label :: a, frame :: F.FrameRec rs}

modelDRAComparisonChart :: (K.KnitEffects r
                           , FC.ElemsOf rs [GT.StateAbbreviation, MR.ModelCI, ET.DemShare, GT.DistrictName, GT.DistrictTypeC]
                           )
                        => BR.PostPaths Path.Abs -> BR.PostInfo
                        -> Text -> Text -> FV.ViewConfig
                        -> Text -> [LabeledFrame Text rs]
                        -> K.Sem r GV.VegaLite
modelDRAComparisonChart pp pi' chartID title vc labelName labeledRows = do
  let colData t r = [ (labelName, GV.Str t)
                    , ("State", GV.Str $ r ^. GT.stateAbbreviation)
                    , ("District", GV.Str $ show (r ^. GT.districtTypeC) <> "-" <> r ^. GT.districtName)
                    , ("Model_Lo" , GV.Number $ MT.ciLower $ r ^. MR.modelCI)
                    , ("Model" , GV.Number $ MT.ciMid $ r ^. MR.modelCI)
                    , ("Model_Hi" , GV.Number $ MT.ciUpper $ r ^. MR.modelCI)
                    , ("Historical", GV.Number $ r ^. ET.demShare)
                    ]
      lfToRows (LabeledFrame t rows) = fmap (colData t) $ FL.fold FL.list rows
      jsonRows = FL.fold (VJ.rowsToJSON' lfToRows [] Nothing) labeledRows
  jsonFilePrefix <- K.getNextUnusedId $ ("2023-StateLeg_" <> chartID)
  jsonUrl <-  BRK.brAddJSON pp pi' jsonFilePrefix jsonRows
  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
      encHistorical = GV.position GV.X [GV.PName "Historical", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      encModel = GV.position GV.Y [GV.PName "Model", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      encLabel = GV.color [GV.MName labelName, GV.MmType GV.Nominal]
      markMid = GV.mark GV.Circle [GV.MTooltip GV.TTData]
      midSpec = GV.asSpec [(GV.encoding . encHistorical . encModel . encLabel) [], markMid]
      encModelLo = GV.position GV.Y [GV.PName "Model_Lo", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      encModelHi = GV.position GV.Y2 [GV.PName "Model_Hi", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      markError = GV.mark GV.ErrorBar [GV.MTooltip GV.TTEncoding]
      errorSpec = GV.asSpec [(GV.encoding . encHistorical . encModelLo . encModelHi . encLabel) [], markError]
      encHistoricalY = GV.position GV.Y [GV.PName "Historical", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      lineSpec = GV.asSpec [(GV.encoding . encHistorical . encHistoricalY) [], GV.mark GV.Line []]
      layers = GV.layer [midSpec, errorSpec, lineSpec]
  pure $  FV.configuredVegaLite vc [FV.title title
                                  , layers
                                  , vlData
                                  ]
