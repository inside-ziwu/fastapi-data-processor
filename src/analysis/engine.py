"""Analysis computation engine."""

import logging

import polars as pl
from typing import Dict, List, Any, Optional
from .computations import ComputationRegistry


class AnalysisEngine:
    """Engine for applying analysis computations to data."""

    def __init__(self):
        self.registry = ComputationRegistry()

    def apply_computations(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply all registered computations to DataFrame."""
        result = df

        # Apply computations in order
        for computation in self.registry.get_computations():
            try:
                result = computation.apply(result)
            except Exception as e:
                # Log error but continue with other computations
                logger = logging.getLogger(__name__)
                logger.warning(f"Computation {computation.name} failed: {e}")
                continue

        return result

    def add_computation(self, computation):
        """Add a computation to the engine."""
        self.registry.register(computation)

    def get_computation_results(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Get results from all computations."""
        results = {}

        for computation in self.registry.get_computations():
            try:
                results[computation.name] = computation.compute(df)
            except Exception as e:
                results[computation.name] = {"error": str(e)}

        return results
