"""
Configuration loader for YAML-based configuration.
"""

import yaml
from pathlib import Path
from typing import Optional

from .config import (
    SimulationConfig,
    DashboardConfig,
    GenieConfig,
    ServerlessWarehouseConfig,
    PricingConfig
)


def load_config_from_yaml(yaml_path: str = "config.yaml") -> SimulationConfig:
    """
    Load simulation configuration from YAML file.
    
    Args:
        yaml_path: Path to YAML config file (default: config.yaml in project root)
    
    Returns:
        SimulationConfig object
    """
    yaml_file = Path(yaml_path)
    
    if not yaml_file.exists():
        raise FileNotFoundError(
            f"Config file not found: {yaml_path}\n"
            f"Please create a config.yaml file or specify a valid path."
        )
    
    with open(yaml_file, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    # Extract sections
    sim = config_dict.get('simulation', {})
    dash = config_dict.get('dashboard', {})
    genie = config_dict.get('genie', {})
    warehouse = config_dict.get('warehouse', {})
    pricing = config_dict.get('pricing', {})
    
    # Build configuration objects
    dashboard_config = DashboardConfig(
        num_dashboards=dash.get('num_dashboards', 50),
        refreshes_per_day=dash.get('refreshes_per_day', 24),
        avg_refresh_runtime=dash.get('avg_refresh_runtime', 30.0),
        refresh_runtime_std=dash.get('refresh_runtime_std', 10.0),
        min_refresh_runtime=dash.get('min_refresh_runtime', 5.0),
        max_refresh_runtime=dash.get('max_refresh_runtime', 120.0),
        refresh_overlap_factor=dash.get('refresh_overlap_factor', 0.3)
    )
    
    genie_config = GenieConfig(
        peak_concurrent_users_min=genie.get('peak_concurrent_users_min', 15),
        peak_concurrent_users_max=genie.get('peak_concurrent_users_max', 30),
        avg_queries_per_user_per_hour=genie.get('avg_queries_per_user_per_hour', 6.0),
        cache_hit_rate=genie.get('cache_hit_rate', 0.4),
        cache_hit_avg_time=genie.get('cache_hit_avg_time', 2.0),
        cache_hit_std=genie.get('cache_hit_std', 0.5),
        cache_miss_avg_time=genie.get('cache_miss_avg_time', 8.0),
        cache_miss_std=genie.get('cache_miss_std', 3.0),
        business_hours_start=genie.get('business_hours_start', 8),
        business_hours_end=genie.get('business_hours_end', 18),
        fraction_using_genai=genie.get('fraction_using_genai', 0.1),
        genai_dbu_per_call=genie.get('genai_dbu_per_call', 0.05)
    )
    
    warehouse_config = ServerlessWarehouseConfig(
        size=warehouse.get('size', 'Medium'),
        target_concurrency_per_cluster=warehouse.get('target_concurrency_per_cluster', 4),
        scale_up_threshold=warehouse.get('scale_up_threshold', 0.8),
        scale_down_threshold=warehouse.get('scale_down_threshold', 0.3),
        scale_up_delay_seconds=warehouse.get('scale_up_delay_seconds', 10.0),
        scale_down_delay_seconds=warehouse.get('scale_down_delay_seconds', 60.0),
        min_clusters=warehouse.get('min_clusters', 1),
        max_clusters=warehouse.get('max_clusters', 10),
        idle_shutdown_seconds=warehouse.get('idle_shutdown_seconds', 120.0)
    )
    
    pricing_config = PricingConfig(
        sql_serverless_dbu_rate=pricing.get('sql_serverless_dbu_rate', 0.70),
        serverless_realtime_inference_dbu_rate=pricing.get('serverless_realtime_inference_dbu_rate', 0.70)
    )
    
    # Build main config
    simulation_config = SimulationConfig(
        simulation_days=sim.get('days', 7),
        time_step_seconds=sim.get('time_step_seconds', 10.0),
        random_seed=sim.get('random_seed', 42),
        enable_progress_logging=sim.get('enable_progress_logging', True),
        progress_log_interval=sim.get('progress_log_interval', 10000),
        dashboard=dashboard_config,
        genie=genie_config,
        warehouse=warehouse_config,
        pricing=pricing_config
    )
    
    return simulation_config


def print_config_summary(config: SimulationConfig):
    """Print a summary of the loaded configuration."""
    print("Configuration Loaded:")
    print(f"  Simulation: {config.simulation_days} days")
    print(f"  Dashboards: {config.dashboard.num_dashboards} ({config.dashboard.refreshes_per_day} refreshes/day)")
    print(f"  Concurrent Users: {config.genie.peak_concurrent_users_min}-{config.genie.peak_concurrent_users_max} peak")
    print(f"  Genie Query Rate: {config.genie.avg_queries_per_user_per_hour} queries/user/hour")
    print(f"  Warehouse: {config.warehouse.size} ({config.warehouse.dbus_per_hour} DBUs/hour)")
    print(f"  Clusters: min={config.warehouse.min_clusters}, max={config.warehouse.max_clusters}")
    print(f"  DBU Rate: ${config.pricing.sql_serverless_dbu_rate}/DBU")

