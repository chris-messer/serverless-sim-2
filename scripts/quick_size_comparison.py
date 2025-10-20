#!/usr/bin/env python3
"""
Quick warehouse size comparison (uses 2-day simulation for speed).
"""

import sys
import time
sys.path.insert(0, '.')

from src import load_config_from_yaml, run_simulation

# Load base configuration
config = load_config_from_yaml("config.yaml")

# Use shorter simulation for faster comparison
original_days = config.simulation_days
config.simulation_days = 2  # Just 2 days for quick comparison

# Test different warehouse sizes
sizes_to_test = ["XSmall", "Small", "Medium", "Large"]

print("\n" + "="*90)
print("QUICK WAREHOUSE SIZE COMPARISON (2-day simulation)")
print("="*90)
print(f"\nWorkload: {config.genie.total_users} users, "
      f"{config.genie.avg_queries_per_user_per_hour} queries/user/hour, "
      f"{config.dashboard.num_dashboards} dashboards")
print("\n" + "-"*90)
print(f"{'Size':<10} {'DBUs/hr':<10} {'Monthly $':<12} {'P95 Wait':<12} {'Performance':<18} {'Assessment'}")
print("-"*90)

for size in sizes_to_test:
    config.warehouse.size = size
    
    start = time.time()
    metrics = run_simulation(config)
    elapsed = time.time() - start
    
    monthly_cost = metrics.total_cost / config.simulation_days * 30
    p95_wait = metrics.genie_p95_wait_time
    dbus_per_hour = config.warehouse.dbus_per_hour
    
    # Calculate performance multiplier
    baseline_dbus = 24.0
    perf_mult = (baseline_dbus / dbus_per_hour) ** 0.5
    if perf_mult < 1.0:
        perf_desc = f"{1/perf_mult:.2f}x faster"
    elif perf_mult > 1.0:
        perf_desc = f"{perf_mult:.2f}x slower"
    else:
        perf_desc = "baseline"
    
    # Performance assessment
    if p95_wait < 2:
        assessment = "⭐ Excellent"
    elif p95_wait < 5:
        assessment = "✓ Good"
    elif p95_wait < 10:
        assessment = "⚠ Acceptable"
    else:
        assessment = "✗ Needs attention"
    
    print(f"{size:<10} {dbus_per_hour:<10.0f} ${monthly_cost:<11,.0f} "
          f"{p95_wait:<11.2f}s {perf_desc:<18} {assessment}")

print("-"*90)
print("\n💡 KEY TRADE-OFFS:")
print("   • Larger warehouses = Faster query execution = Better user experience")
print("   • But they cost more per hour")
print("   • Faster queries may also reduce queuing, further improving wait times")
print("   • Choose based on your performance requirements vs budget")
print("\n" + "="*90 + "\n")

