Advanced simulation
===================

Advanced controls are opt-in and preserve the same deterministic identities and
source/oracle boundaries as ordinary generation.

.. warning::

   Calibration, campaign dynamics, and scale helpers are experimental in this
   release. Treat their serialized profiles and manifests as version-specific
   artifacts and consult the migration notes before upgrading.

Calibration
-----------

.. autosummary::
   :nosignatures:

   fraudtwin.fit_calibration_profile
   fraudtwin.load_reference_data
   fraudtwin.load_calibration_profile
   fraudtwin.write_calibration_profile
   fraudtwin.compute_fidelity_report
   fraudtwin.CalibrationProfile
   fraudtwin.CalibrationModel
   fraudtwin.CalibrationMetric
   fraudtwin.CalibrationProvenance
   fraudtwin.FeatureDependency
   fraudtwin.FidelityMetric
   fraudtwin.FidelityReport
   fraudtwin.FittedDistribution
   fraudtwin.ReferenceDataset
   fraudtwin.ResolvedCalibration
   fraudtwin.StatisticalSummary

Counterfactuals, difficulty, and camouflage
--------------------------------------------

.. autosummary::
   :nosignatures:

   fraudtwin.generate_counterfactuals
   fraudtwin.resolve_counterfactual
   fraudtwin.select_source_trajectories
   fraudtwin.apply_difficulty
   fraudtwin.resolve_difficulty
   fraudtwin.apply_camouflage
   fraudtwin.resolve_camouflage
   fraudtwin.CounterfactualDataset
   fraudtwin.CounterfactualChangeSet
   fraudtwin.CounterfactualScope
   fraudtwin.DistanceFunction
   fraudtwin.ResolvedDifficulty
   fraudtwin.ResolvedCamouflage
   fraudtwin.ScenarioDifficultyPlan
   fraudtwin.CamouflagePlan
   fraudtwin.SourceTrajectory

Campaign dynamics and labels
----------------------------

.. autosummary::
   :nosignatures:

   fraudtwin.evolve_campaigns
   fraudtwin.resolve_campaign_dynamics
   fraudtwin.validate_campaign_dynamics
   fraudtwin.apply_label_observation
   fraudtwin.visible_label_at
   fraudtwin.reconstruct_label_history
