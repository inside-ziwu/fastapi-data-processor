"""Analysis computations module."""

from .engine import AnalysisEngine
from .computations import ComputationRegistry, BaseComputation
from .metrics_computations import (
    IntermediateMetricsComputation,
    CPLMetricsComputation,
    DailyAveragesComputation,
    LiveMetricsComputation,
    ConversionRatesComputation,
    VideoMetricsComputation,
    LocalMetricsComputation,
    PrivateMessageMetricsComputation,
)

__all__ = [
    "AnalysisEngine",
    "ComputationRegistry",
    "BaseComputation",
    "IntermediateMetricsComputation",
    "CPLMetricsComputation",
    "DailyAveragesComputation",
    "LiveMetricsComputation",
    "ConversionRatesComputation",
    "VideoMetricsComputation",
    "LocalMetricsComputation",
    "PrivateMessageMetricsComputation",
]


def create_default_analysis_engine() -> AnalysisEngine:
    """Create analysis engine with all metrics pre-registered."""
    engine = AnalysisEngine()
    
    # Register all computations in proper order
    # 1. Intermediate metrics first (creates derived columns)
    engine.add_computation(IntermediateMetricsComputation())
    
    # 2. CPL metrics (depends on intermediate metrics)
    engine.add_computation(CPLMetricsComputation())
    
    # 3. Daily averages
    engine.add_computation(DailyAveragesComputation())
    
    # 4. Live streaming metrics
    engine.add_computation(LiveMetricsComputation())
    
    # 5. Conversion rates
    engine.add_computation(ConversionRatesComputation())
    
    # 6. Video/anchor metrics
    engine.add_computation(VideoMetricsComputation())
    
    # 7. Local metrics
    engine.add_computation(LocalMetricsComputation())
    
    # 8. Private message metrics
    engine.add_computation(PrivateMessageMetricsComputation())
    
    return engine