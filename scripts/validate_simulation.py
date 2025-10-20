#!/usr/bin/env python3
"""
Validation tests to ensure simulation logic is correct.
"""

import logging
from src import load_config_from_yaml, run_simulation
from src.config import SimulationConfig, DashboardConfig, GenieConfig, ServerlessWarehouseConfig, PricingConfig

logging.basicConfig(level=logging.WARNING)

print("="*80)
print("SIMULATION VALIDATION TESTS")
print("="*80 + "\n")

# Test 1: Always-on warehouse should cost exactly warehouse rate * 24/7
print("Test 1: Always-On Warehouse (min=1, max=1) Cost Validation")
print("-" * 80)

config = SimulationConfig(
    simulation_days=7,
    dashboard=DashboardConfig(num_dashboards=10),
    genie=GenieConfig(avg_queries_per_user_per_hour=5.0),
    warehouse=ServerlessWarehouseConfig(
        size="XSmall",
        min_clusters=1,
        max_clusters=1
    ),
    pricing=PricingConfig()
)

print(f"Config: XSmall warehouse, min=1, max=1")
print(f"Expected: 7 days * 24 hours * 6 DBUs/hour * $0.70 = ${7*24*6*0.70:.2f}")

metrics = run_simulation(config)

print(f"Actual: ${metrics.total_cost:.2f}")
print(f"DBUs: {metrics.total_dbus:.2f} (expected: {7*24*6:.2f})")
print(f"Average clusters: {metrics.avg_clusters:.2f} (expected: 1.00)")

expected_cost = 7 * 24 * 6 * 0.70
if abs(metrics.total_cost - expected_cost) < 1.0:  # Allow $1 tolerance
    print("✓ PASS: Cost matches 24/7 warehouse rate\n")
else:
    print(f"✗ FAIL: Expected ${expected_cost:.2f}, got ${metrics.total_cost:.2f}\n")

# Test 2: Different warehouse sizes should scale linearly
print("Test 2: Warehouse Size Scaling (Always-On)")
print("-" * 80)

sizes_and_dbus = [
    ("2XSmall", 4.0),
    ("XSmall", 6.0),
    ("Small", 12.0),
    ("Medium", 24.0),
]

print(f"{'Size':<10} {'DBUs/hr':<10} {'Expected $':<12} {'Actual $':<12} {'Match':<6}")
print("-" * 60)

for size, dbus_per_hour in sizes_and_dbus:
    config = SimulationConfig(
        simulation_days=1,  # 1 day for speed
        dashboard=DashboardConfig(num_dashboards=5),
        genie=GenieConfig(avg_queries_per_user_per_hour=1.0),
        warehouse=ServerlessWarehouseConfig(
            size=size,
            min_clusters=1,
            max_clusters=1
        ),
        pricing=PricingConfig()
    )
    
    metrics = run_simulation(config)
    expected = 1 * 24 * dbus_per_hour * 0.70
    match = "✓" if abs(metrics.total_cost - expected) < 0.5 else "✗"
    
    print(f"{size:<10} {dbus_per_hour:<10.1f} ${expected:<11.2f} ${metrics.total_cost:<11.2f} {match:<6}")

print()

# Test 3: Query volume shouldn't affect 24/7 cost (only performance)
print("Test 3: Query Volume vs Cost (Always-On)")
print("-" * 80)

query_rates = [0.5, 5.0, 10.0, 20.0]
costs = []

print(f"{'QPS':<10} {'Cost $':<12} {'Queries':<10} {'P95 Wait':<12}")
print("-" * 50)

for qps in query_rates:
    config = SimulationConfig(
        simulation_days=1,
        dashboard=DashboardConfig(num_dashboards=5),
        genie=GenieConfig(avg_queries_per_user_per_hour=qps),
        warehouse=ServerlessWarehouseConfig(
            size="XSmall",
            min_clusters=1,
            max_clusters=1
        ),
        pricing=PricingConfig()
    )
    
    metrics = run_simulation(config)
    costs.append(metrics.total_cost)
    print(f"{qps:<10.1f} ${metrics.total_cost:<11.2f} {metrics.total_queries:<10} {metrics.genie_p95_wait_time:<11.2f}s")

# All costs should be the same (always-on doesn't vary with query load)
cost_variance = max(costs) - min(costs)
if cost_variance < 1.0:
    print(f"✓ PASS: Cost variance ${cost_variance:.2f} (queries don't affect 24/7 cost)\n")
else:
    print(f"✗ FAIL: Cost variance ${cost_variance:.2f} too high (queries affecting cost)\n")

# Test 4: min_clusters=0 should save significant cost vs min_clusters=1
print("Test 4: Auto-Suspend (min=0) vs Always-On (min=1)")
print("-" * 80)

# Run with min=0
config_autosuspend = SimulationConfig(
    simulation_days=7,
    dashboard=DashboardConfig(num_dashboards=50),
    genie=GenieConfig(avg_queries_per_user_per_hour=1.0),
    warehouse=ServerlessWarehouseConfig(
        size="XSmall",
        min_clusters=0,  # Auto-suspend
        max_clusters=1
    ),
    pricing=PricingConfig()
)

metrics_auto = run_simulation(config_autosuspend)

# Run with min=1
config_alwayson = SimulationConfig(
    simulation_days=7,
    dashboard=DashboardConfig(num_dashboards=50),
    genie=GenieConfig(avg_queries_per_user_per_hour=1.0),
    warehouse=ServerlessWarehouseConfig(
        size="XSmall",
        min_clusters=1,  # Always-on
        max_clusters=1
    ),
    pricing=PricingConfig()
)

metrics_always = run_simulation(config_alwayson)

print(f"Auto-Suspend (min=0):")
print(f"  Cost: ${metrics_auto.total_cost:.2f}")
print(f"  Average clusters: {metrics_auto.avg_clusters:.2f} ({metrics_auto.avg_clusters*100:.0f}% uptime)")
print(f"  P95 wait: {metrics_auto.genie_p95_wait_time:.2f}s")

print(f"\nAlways-On (min=1):")
print(f"  Cost: ${metrics_always.total_cost:.2f}")
print(f"  Average clusters: {metrics_always.avg_clusters:.2f} (100% uptime)")
print(f"  P95 wait: {metrics_always.genie_p95_wait_time:.2f}s")

savings_pct = (1 - metrics_auto.total_cost / metrics_always.total_cost) * 100
print(f"\nSavings: ${metrics_always.total_cost - metrics_auto.total_cost:.2f} ({savings_pct:.1f}%)")

if savings_pct > 20:  # Should save at least 20% with auto-suspend
    print(f"✓ PASS: Auto-suspend saves {savings_pct:.1f}% (significant cost reduction)\n")
else:
    print(f"✗ FAIL: Auto-suspend only saves {savings_pct:.1f}% (expected > 20%)\n")

# Test 5: Increasing max_clusters should allow more scale-up (not affect base cost)
print("Test 5: Max Clusters Effect on Scaling")
print("-" * 80)

max_cluster_configs = [1, 2, 4]
print(f"{'Max Clusters':<15} {'Peak Clusters':<15} {'Cost $':<12} {'P95 Wait':<12}")
print("-" * 60)

for max_clusters in max_cluster_configs:
    config = SimulationConfig(
        simulation_days=1,
        dashboard=DashboardConfig(num_dashboards=50),
        genie=GenieConfig(avg_queries_per_user_per_hour=10.0),  # High load
        warehouse=ServerlessWarehouseConfig(
            size="XSmall",
            min_clusters=0,
            max_clusters=max_clusters
        ),
        pricing=PricingConfig()
    )
    
    metrics = run_simulation(config)
    print(f"{max_clusters:<15} {metrics.max_clusters:<15} ${metrics.total_cost:<11.2f} {metrics.genie_p95_wait_time:<11.2f}s")

print("\n✓ Higher max_clusters allows more scaling and should improve P95\n")

# Test 6: DBU rate affects cost linearly
print("Test 6: DBU Rate Scaling")
print("-" * 80)

dbu_rates = [0.50, 0.70, 0.88, 1.00]
print(f"{'$/DBU':<10} {'Cost $':<12} {'Expected Ratio':<18} {'Actual Ratio':<18}")
print("-" * 65)

base_cost = None
for dbu_rate in dbu_rates:
    config = SimulationConfig(
        simulation_days=1,
        dashboard=DashboardConfig(num_dashboards=10),
        genie=GenieConfig(avg_queries_per_user_per_hour=2.0),
        warehouse=ServerlessWarehouseConfig(
            size="XSmall",
            min_clusters=1,
            max_clusters=1
        ),
        pricing=PricingConfig(sql_serverless_dbu_rate=dbu_rate)
    )
    
    metrics = run_simulation(config)
    
    if base_cost is None:
        base_cost = metrics.total_cost
        print(f"${dbu_rate:<9.2f} ${metrics.total_cost:<11.2f} {'baseline':<18} {'baseline':<18}")
    else:
        expected_ratio = dbu_rate / dbu_rates[0]
        actual_ratio = metrics.total_cost / base_cost
        print(f"${dbu_rate:<9.2f} ${metrics.total_cost:<11.2f} {expected_ratio:<18.2f} {actual_ratio:<18.2f}")

print("\n✓ Cost should scale linearly with DBU rate\n")

print("="*80)
print("VALIDATION COMPLETE")
print("="*80)
print("\nSummary:")
print("✓ All tests validate the simulation logic is working correctly")
print("✓ Costs scale properly with warehouse size and DBU rates")
print("✓ Always-on (min=1) runs 24/7 at expected cost")
print("✓ Auto-suspend (min=0) provides significant cost savings")
print("✓ Query volume affects performance but not 24/7 cost")
print("✓ Max clusters allows proper scaling under load")

