"""Computation registry and base classes."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
import polars as pl


class BaseComputation(ABC):
    """Base class for all computations."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply computation to DataFrame and return modified DataFrame."""
        pass

    @abstractmethod
    def compute(self, df: pl.DataFrame) -> Any:
        """Compute and return result value."""
        pass


class ComputationRegistry:
    """Registry for analysis computations."""

    def __init__(self):
        self._computations: List[BaseComputation] = []

    def register(self, computation: BaseComputation):
        """Register a computation."""
        self._computations.append(computation)

    def get_computations(self) -> List[BaseComputation]:
        """Get all registered computations."""
        return self._computations.copy()

    def clear(self):
        """Clear all computations."""
        self._computations.clear()


# Example computation implementations
class CPLComputation(BaseComputation):
    """Compute Cost Per Lead."""

    def __init__(self):
        super().__init__("cpl")

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add CPL columns to DataFrame."""
        if "spending_net" not in df.columns or "total_leads" not in df.columns:
            return df

        return df.with_columns(
            [
                (pl.col("spending_net") / pl.col("total_leads").clip(1)).alias(
                    "cpl"
                ),
                (
                    pl.col("spending_net_t") / pl.col("total_leads_t").clip(1)
                ).alias("cpl_t"),
                (
                    pl.col("spending_net_t_minus_1")
                    / pl.col("total_leads_t_minus_1").clip(1)
                ).alias("cpl_t_minus_1"),
            ]
        )

    def compute(self, df: pl.DataFrame) -> Dict[str, float]:
        """Compute overall CPL statistics."""
        if "spending_net" not in df.columns or "total_leads" not in df.columns:
            return {}

        total_spending = df["spending_net"].sum()
        total_leads = df["total_leads"].sum()

        return {
            "total_cpl": total_spending / max(total_leads, 1),
            "total_spending": total_spending,
            "total_leads": total_leads,
        }
