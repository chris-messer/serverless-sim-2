#!/usr/bin/env python3
"""
Validation test to ensure warehouse scales to zero correctly.
"""

import logging
import pandas as pd
from src.config import SimulationConfig, DashboardConfig, GenieConfig, ServerlessWarehouseConfig, PricingConfig
from src.simulator import run_simulation

logging.basicConfig(level=logging.WARNING)

print("="*80)
print("SCALE TO ZERO VALIDATION TEST")
print("="*80 + "\n")

# Test: Warehouse should scale to 0 during idle periods
print("Test: Auto-Suspend to Zero Clusters")
print("-" * 80)

# Run simulation with very light load (should have idle periods)
config = SimulationConfig(
    simulation_days=2,  # 2 days to see idle patterns
    dashboard=DashboardConfig(
        num_dashboards=5,  # Very few dashboards
        refreshes_per_day=12  # Only twice per day (every 12 hours)
    ),
    genie=GenieConfig(
        total_users=50,
        peak_concurrent_users_min=2,
        peak_concurrent_users_max=5,
        avg_queries_per_user_per_hour=0.1,  # Very light query rate
        business_hours_start=9,
        business_hours_end=17  # Only 8 hours/day
    ),
    warehouse=ServerlessWarehouseConfig(
        size="XSmall",
        min_clusters=0,  # Should scale to zero
        max_clusters=1,
        idle_shutdown_seconds=120.0  # 2 minutes idle timeout
    ),
    pricing=PricingConfig()
)

print("Configuration:")
print(f"  - Dashboards: {config.dashboard.num_dashboards} (refresh every 12 hours)")
print(f"  - Users: {config.genie.total_users} ({config.genie.avg_queries_per_user_per_hour} queries/user/hour)")
print(f"  - Business hours: {config.genie.business_hours_start}AM - {config.genie.business_hours_end}PM")
print(f"  - Idle timeout: {config.warehouse.idle_shutdown_seconds}s")
print(f"  - min_clusters: {config.warehouse.min_clusters}")
print()

print("Running simulation...")
metrics = run_simulation(config)

print("\nResults:")
print("-" * 80)
print(f"Total Cost: ${metrics.total_cost:.2f}")
print(f"Average Clusters: {metrics.avg_clusters:.2f}")
print(f"Total Queries: {metrics.total_queries}")
print()

# Load state history to analyze cluster behavior
df = pd.read_csv('results/warehouse_state_history.csv')

# Calculate time spent at each cluster count
cluster_counts = df['Clusters'].value_counts().sort_index()
total_records = len(df)

print("Cluster Distribution:")
print("-" * 80)
print(f"{'Clusters':<12} {'Records':<12} {'Percentage':<12} {'Status'}")
print("-" * 60)

for clusters, count in cluster_counts.items():
    percentage = (count / total_records) * 100
    status = ""
    if clusters == 0:
        status = "← IDLE (scaled to zero)"
    elif clusters == 1:
        status = "← ACTIVE"
    
    print(f"{int(clusters):<12} {count:<12} {percentage:>6.1f}%      {status}")

print()

# Check for zero-cluster periods
zero_cluster_records = (df['Clusters'] == 0).sum()
zero_cluster_pct = (zero_cluster_records / total_records) * 100

print("Scale to Zero Analysis:")
print("-" * 80)

# Calculate time at zero (assuming 60-second intervals between records)
time_at_zero_minutes = (zero_cluster_records * 60) / 60  # Records are 1 minute apart
time_at_zero_hours = time_at_zero_minutes / 60

print(f"Records at 0 clusters: {zero_cluster_records} / {total_records} ({zero_cluster_pct:.1f}%)")
print(f"Estimated time at 0 clusters: ~{time_at_zero_hours:.1f} hours out of {config.simulation_days * 24} hours")
print()

# Validation checks
print("Validation Checks:")
print("-" * 80)

# Check 1: Should have periods at zero
if zero_cluster_records > 0:
    print(f"✓ PASS: Warehouse scaled to 0 clusters ({zero_cluster_records} times)")
else:
    print(f"✗ FAIL: Warehouse never scaled to 0 clusters")

# Check 2: Should be at zero for significant time (expect >20% with light load)
if zero_cluster_pct > 20:
    print(f"✓ PASS: Warehouse at 0 clusters for {zero_cluster_pct:.1f}% of time (significant idle)")
elif zero_cluster_pct > 0:
    print(f"⚠ WARNING: Warehouse only at 0 clusters for {zero_cluster_pct:.1f}% (may need lighter load)")
else:
    print(f"✗ FAIL: Warehouse never reached 0 clusters")

# Check 3: Average clusters should be less than 1.0 (proving it scales down)
if metrics.avg_clusters < 1.0:
    print(f"✓ PASS: Average clusters {metrics.avg_clusters:.2f} < 1.0 (warehouse scales down)")
else:
    print(f"✗ FAIL: Average clusters {metrics.avg_clusters:.2f} >= 1.0 (not scaling down)")

# Check 4: Cost should be significantly less than 24/7 always-on
always_on_cost = config.simulation_days * 24 * 6.0 * 0.70  # XSmall = 6 DBUs/hour
cost_ratio = metrics.total_cost / always_on_cost

print(f"✓ INFO: Cost is {cost_ratio*100:.1f}% of always-on (${metrics.total_cost:.2f} vs ${always_on_cost:.2f})")

if cost_ratio < 0.80:  # Should be less than 80% of always-on
    print(f"✓ PASS: Significant cost savings from auto-suspend ({(1-cost_ratio)*100:.1f}% savings)")
else:
    print(f"⚠ WARNING: Limited cost savings ({(1-cost_ratio)*100:.1f}% savings, expected >20%)")

print()

# Detailed analysis: Find longest idle period
print("Idle Period Analysis:")
print("-" * 80)

# Find continuous stretches at 0 clusters
zero_stretches = []
current_stretch = 0

for i, row in df.iterrows():
    if row['Clusters'] == 0:
        current_stretch += 1
    else:
        if current_stretch > 0:
            zero_stretches.append(current_stretch)
        current_stretch = 0

if current_stretch > 0:
    zero_stretches.append(current_stretch)

if zero_stretches:
    max_idle = max(zero_stretches)
    avg_idle = sum(zero_stretches) / len(zero_stretches)
    total_idle_periods = len(zero_stretches)
    
    print(f"Number of idle periods (at 0 clusters): {total_idle_periods}")
    print(f"Longest idle period: {max_idle} minutes (~{max_idle/60:.1f} hours)")
    print(f"Average idle period: {avg_idle:.1f} minutes")
    print(f"Total idle periods: {total_idle_periods}")
    print()
    
    # Show top 5 longest idle periods
    print("Top 5 longest idle periods:")
    for i, stretch in enumerate(sorted(zero_stretches, reverse=True)[:5], 1):
        print(f"  {i}. {stretch} minutes (~{stretch/60:.1f} hours)")
else:
    print("No idle periods found (warehouse never reached 0 clusters)")

print()

# Analyze overnight periods (should be mostly idle)
print("Overnight Analysis (midnight to 6 AM):")
print("-" * 80)

df['hour'] = (df['Time (s)'] / 3600) % 24
overnight_df = df[(df['hour'] >= 0) & (df['hour'] < 6)]

if len(overnight_df) > 0:
    overnight_zero = (overnight_df['Clusters'] == 0).sum()
    overnight_pct = (overnight_zero / len(overnight_df)) * 100
    
    print(f"Overnight records at 0 clusters: {overnight_zero} / {len(overnight_df)} ({overnight_pct:.1f}%)")
    
    if overnight_pct > 50:
        print(f"✓ PASS: Warehouse scales to 0 during overnight hours ({overnight_pct:.1f}%)")
    else:
        print(f"⚠ WARNING: Warehouse not scaling down much overnight ({overnight_pct:.1f}%)")

print()
print("="*80)
print("SCALE TO ZERO VALIDATION COMPLETE")
print("="*80)
print()

# Summary
if zero_cluster_pct > 20 and metrics.avg_clusters < 0.8:
    print("✅ OVERALL: Scale to zero logic is working correctly!")
    print(f"   - Warehouse scales to 0 clusters during idle periods")
    print(f"   - {zero_cluster_pct:.1f}% of time spent at 0 clusters")
    print(f"   - {(1-cost_ratio)*100:.1f}% cost savings vs always-on")
elif zero_cluster_records > 0:
    print("⚠️  PARTIAL: Scale to zero is working but could be more aggressive")
    print(f"   - Warehouse does scale to 0, but only {zero_cluster_pct:.1f}% of the time")
    print(f"   - May need lighter workload or longer idle timeout to see more savings")
else:
    print("❌ ISSUE: Scale to zero logic may not be working properly")
    print(f"   - Warehouse never scaled to 0 clusters")
    print(f"   - Check min_clusters setting and workload pattern")

