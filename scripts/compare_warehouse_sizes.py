#!/usr/bin/env python3
"""
Compare different warehouse sizes with the same workload.
Shows the cost vs performance trade-off.
"""

import sys
sys.path.insert(0, '.')

from src import load_config_from_yaml, run_simulation

# Load base configuration
config = load_config_from_yaml("config.yaml")

# Test different warehouse sizes
sizes_to_test = ["2XSmall", "XSmall", "Small", "Medium", "Large", "XLarge"]

print("\n" + "="*100)
print("WAREHOUSE SIZE COMPARISON - Same Workload")
print("="*100)
print(f"\nWorkload: {config.simulation_days} days, {config.genie.total_users} users, "
      f"{config.genie.avg_queries_per_user_per_hour} queries/user/hour")
print("\n" + "-"*100)
print(f"{'Size':<10} {'DBUs/hr':<10} {'Monthly Cost':<15} {'Genie P95 Wait':<18} {'Performance':<20} {'Trade-off'}")
print("-"*100)

results = []

for size in sizes_to_test:
    # Update warehouse size
    config.warehouse.size = size
    
    # Run simulation
    metrics = run_simulation(config)
    
    monthly_cost = metrics.total_cost / config.simulation_days * 30
    p95_wait = metrics.genie_p95_wait_time
    dbus_per_hour = config.warehouse.dbus_per_hour
    
    # Performance assessment
    if p95_wait < 2:
        perf_rating = "⭐ Excellent"
    elif p95_wait < 5:
        perf_rating = "✓ Good"
    elif p95_wait < 10:
        perf_rating = "⚠ Acceptable"
    else:
        perf_rating = "✗ Poor"
    
    # Calculate performance multiplier
    baseline_dbus = 24.0
    perf_mult = (baseline_dbus / dbus_per_hour) ** 0.5
    if perf_mult < 1.0:
        perf_desc = f"{1/perf_mult:.2f}x faster"
    elif perf_mult > 1.0:
        perf_desc = f"{perf_mult:.2f}x slower"
    else:
        perf_desc = "baseline"
    
    results.append({
        'size': size,
        'dbus_per_hour': dbus_per_hour,
        'monthly_cost': monthly_cost,
        'p95_wait': p95_wait,
        'perf_rating': perf_rating,
        'perf_desc': perf_desc
    })
    
    # Determine trade-off
    if monthly_cost < 1500 and p95_wait > 10:
        tradeoff = "Cheap but slow"
    elif monthly_cost > 5000 and p95_wait < 2:
        tradeoff = "Fast but expensive"
    elif p95_wait < 5:
        tradeoff = "Good balance ✓"
    else:
        tradeoff = "Consider adjusting"
    
    print(f"{size:<10} {dbus_per_hour:<10.1f} ${monthly_cost:<14,.2f} "
          f"{p95_wait:<17.2f}s {perf_desc:<20} {tradeoff}")

print("-"*100)
print("\n💡 KEY INSIGHTS:")
print("   - Larger warehouses execute queries faster (square root scaling)")
print("   - This reduces wait times and improves user experience")
print("   - But they also cost more per hour")
print("   - The best choice depends on your performance requirements vs budget")
print("\n" + "="*100 + "\n")

