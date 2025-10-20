"""
Configuration parameters for Databricks Serverless SQL Warehouse simulation.
"""

from dataclasses import dataclass, field
from typing import Dict
import numpy as np


@dataclass
class DashboardConfig:
    """Configuration for dashboard refresh workload."""
    
    num_dashboards: int = 50
    refreshes_per_day: int = 24  # Hourly refreshes
    
    # Runtime characteristics (in seconds)
    avg_refresh_runtime: float = 30.0
    refresh_runtime_std: float = 10.0
    min_refresh_runtime: float = 5.0
    max_refresh_runtime: float = 120.0
    
    # Overlap configuration: controls how many dashboards refresh at similar times
    # 0.0 = perfectly spread out, 1.0 = all at once
    refresh_overlap_factor: float = 0.3
    
    def __post_init__(self):
        """Validate configuration."""
        assert self.num_dashboards > 0, "Must have at least one dashboard"
        assert self.refreshes_per_day > 0, "Must have at least one refresh per day"
        assert self.avg_refresh_runtime > 0, "Average runtime must be positive"


@dataclass
class GenieConfig:
    """Configuration for Genie interactive query workload."""
    
    # User activity patterns
    peak_concurrent_users_min: int = 15
    peak_concurrent_users_max: int = 30
    
    # Query patterns
    avg_queries_per_user_per_hour: float = 1.0
    
    # Query service time distribution (in seconds)
    # Cache hits are fast, misses take longer
    cache_hit_rate: float = 0.4
    cache_hit_avg_time: float = 2.0
    cache_hit_std: float = 0.5
    cache_miss_avg_time: float = 8.0
    cache_miss_std: float = 3.0
    
    # Business hours configuration (hours in 24h format)
    business_hours_start: int = 8
    business_hours_end: int = 18
    
    # GenAI SQL function usage
    fraction_using_genai: float = 0  # 0% of queries use GenAI functions
    genai_dbu_per_call: float = 0.05  # Approximate DBUs per GenAI call
    
    def __post_init__(self):
        """Validate configuration."""
        assert 0 <= self.cache_hit_rate <= 1, "Cache hit rate must be between 0 and 1"
        assert self.peak_concurrent_users_min <= self.peak_concurrent_users_max, \
            "Peak concurrent min must be <= max"


@dataclass
class ServerlessWarehouseConfig:
    """Configuration for Serverless SQL Warehouse."""
    
    # T-shirt size and corresponding DBUs per hour per cluster
    size: str = "XSmall"
    
    # DBU rates per cluster size (DBUs per hour per cluster)
    # Based on actual Databricks SQL Serverless warehouse sizing for AWS Enterprise
    # Source: https://www.databricks.com/product/pricing/product-pricing/instance-types
    size_dbu_mapping: Dict[str, float] = field(default_factory=lambda: {
        "2XSmall": 4.0,     # 2X-Small: 4 DBUs/hour per cluster
        "XSmall": 6.0,      # X-Small: 6 DBUs/hour per cluster
        "Small": 12.0,      # Small: 12 DBUs/hour per cluster
        "Medium": 24.0,     # Medium: 24 DBUs/hour per cluster (default)
        "Large": 40.0,      # Large: 40 DBUs/hour per cluster
        "XLarge": 80.0,     # X-Large: 80 DBUs/hour per cluster
        "2XLarge": 144.0,   # 2X-Large: 144 DBUs/hour per cluster
        "3XLarge": 272.0,   # 3X-Large: 272 DBUs/hour per cluster
        "4XLarge": 528.0,   # 4X-Large: 528 DBUs/hour per cluster
    })
    
    # Target concurrency per cluster before scaling up
    target_concurrency_per_cluster: int = 4
    
    # Scaling behavior
    scale_up_threshold: float = 0.8  # Scale up when at 80% of target concurrency
    scale_down_threshold: float = 0.3  # Scale down when below 30% utilization
    scale_up_delay_seconds: float = 10.0  # Fast scale-up
    scale_down_delay_seconds: float = 10.0  # Conservative scale-down
    min_clusters: int = 0  # True Serverless: scale to zero when idle
    max_clusters: int = 4
    
    # Idle behavior: Serverless bills only when busy
    # We'll track "active seconds" to calculate DBU usage
    idle_shutdown_seconds: float = 120.0  # Time before considering completely idle
    
    @property
    def dbus_per_hour(self) -> float:
        """Get DBUs per hour for the configured size."""
        return self.size_dbu_mapping.get(self.size, 24.0)
    
    @property
    def effective_concurrency_per_cluster(self) -> int:
        """
        Get effective concurrency adjusted for warehouse performance.
        Larger/faster warehouses can handle more concurrent queries because
        they complete faster, freeing up slots more quickly.
        """
        baseline_dbus = 24.0  # Medium warehouse
        dbus = self.dbus_per_hour
        
        # Performance multiplier (how much faster than Medium)
        # Larger warehouses execute queries faster
        performance_factor = (dbus / baseline_dbus) ** 0.5
        
        # Increase target concurrency for faster warehouses
        # This accounts for higher throughput (queries/second)
        adjusted_concurrency = int(self.target_concurrency_per_cluster * performance_factor)
        
        # Ensure at least 2 concurrent queries
        return max(2, adjusted_concurrency)

    
    def __post_init__(self):
        """Validate configuration."""
        assert self.size in self.size_dbu_mapping, f"Unknown size: {self.size}"
        assert self.target_concurrency_per_cluster > 0, "Target concurrency must be positive"


@dataclass
class PricingConfig:
    """Pricing configuration."""
    
    sql_serverless_dbu_rate: float = 0.70  # $/DBU
    serverless_realtime_inference_dbu_rate: float = 0.70  # $/DBU (adjustable per model)
    
    def __post_init__(self):
        """Validate configuration."""
        assert self.sql_serverless_dbu_rate > 0, "DBU rate must be positive"


@dataclass
class SimulationConfig:
    """Overall simulation configuration."""
    
    # Simulation duration
    simulation_days: int = 7  # Run for 1 week
    
    # Time resolution (seconds per simulation step)
    time_step_seconds: float = 10.0  # 10-second resolution
    
    # Random seed for reproducibility
    random_seed: int = 42
    
    # Component configurations
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    genie: GenieConfig = field(default_factory=GenieConfig)
    warehouse: ServerlessWarehouseConfig = field(default_factory=ServerlessWarehouseConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    
    @property
    def total_seconds(self) -> int:
        """Total simulation duration in seconds."""
        return self.simulation_days * 24 * 3600
    
    @property
    def num_steps(self) -> int:
        """Number of simulation time steps."""
        return int(self.total_seconds / self.time_step_seconds)
    
    def __post_init__(self):
        """Validate configuration and set random seed."""
        assert self.simulation_days > 0, "Must simulate at least 1 day"
        assert self.time_step_seconds > 0, "Time step must be positive"
        np.random.seed(self.random_seed)


def create_default_config() -> SimulationConfig:
    """Create default simulation configuration."""
    return SimulationConfig()


def create_custom_config(
    num_dashboards: int = None,
    peak_concurrent_users: tuple = None,
    warehouse_size: str = None,
    dbu_rate: float = None,
    simulation_days: int = None
) -> SimulationConfig:
    """
    Create a custom configuration with specified parameters.
    
    Args:
        num_dashboards: Number of dashboards to simulate
        peak_concurrent_users: Tuple of (min, max) concurrent users
        warehouse_size: T-shirt size of warehouse
        dbu_rate: SQL Serverless DBU rate
        simulation_days: Number of days to simulate
    """
    config = SimulationConfig()
    
    if num_dashboards is not None:
        config.dashboard.num_dashboards = num_dashboards
    
    if peak_concurrent_users is not None:
        config.genie.peak_concurrent_users_min = peak_concurrent_users[0]
        config.genie.peak_concurrent_users_max = peak_concurrent_users[1]
    
    if warehouse_size is not None:
        config.warehouse.size = warehouse_size
    
    if dbu_rate is not None:
        config.pricing.sql_serverless_dbu_rate = dbu_rate
    
    if simulation_days is not None:
        config.simulation_days = simulation_days
    
    return config

