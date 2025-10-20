#!/usr/bin/env python3
"""
Simple script to run simulation from config.yaml

Just edit config.yaml and run:
    python run_simulation.py
"""

import logging
import json
from pathlib import Path
from src import load_config_from_yaml, print_config_summary, run_simulation, generate_report

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

print("\n" + "="*80)
print("DATABRICKS SERVERLESS SQL WAREHOUSE COST SIMULATOR")
print("="*80 + "\n")

# Load configuration from YAML
print("Loading configuration from config.yaml...\n")
config = load_config_from_yaml("config.yaml")

# Print summary
print_config_summary(config)

print("\nRunning simulation...\n")

# Run simulation
metrics = run_simulation(config)

# Generate report and visualizations
generate_report(config, metrics, output_dir="results")

# Save metrics summary as JSON for dashboard
output_path = Path("results")
metrics_summary = {
    "simulation_days": config.simulation_days,
    "warehouse_size": config.warehouse.size,
    "total_dbus": metrics.total_dbus,
    "total_cost": metrics.total_cost,
    "daily_cost": metrics.total_cost / config.simulation_days,
    "monthly_cost": metrics.total_cost / config.simulation_days * 30,
    "annual_cost": metrics.total_cost / config.simulation_days * 365,
    "total_queries": metrics.total_queries,
    "dashboard_queries": metrics.dashboard_queries,
    "genie_queries": metrics.genie_queries,
    "genie_avg_wait_time": metrics.genie_avg_wait_time,
    "genie_p50_wait_time": metrics.genie_p50_wait_time,
    "genie_p95_wait_time": metrics.genie_p95_wait_time,
    "genie_p99_wait_time": metrics.genie_p99_wait_time,
    "dashboard_avg_wait_time": metrics.dashboard_avg_wait_time,
    "dashboard_p95_wait_time": metrics.dashboard_p95_wait_time,
    "avg_clusters": metrics.avg_clusters,
    "max_clusters": metrics.max_clusters,
    "avg_utilization": metrics.avg_utilization,
    "max_queue_depth": metrics.max_queue_depth,
}

with open(output_path / "metrics_summary.json", 'w') as f:
    json.dump(metrics_summary, f, indent=2)

# Generate dashboards
print("\n📊 Creating dashboards...")
try:
    import subprocess
    
    # Charts dashboard
    subprocess.run(["python", "src/create_dashboard_charts.py"], check=True, capture_output=True)
    print("✅ Charts dashboard created: results/dashboard_charts.png")
    
    # Summary dashboard
    subprocess.run(["python", "src/create_dashboard_summary.py"], check=True, capture_output=True)
    print("✅ Summary dashboard created: results/dashboard_summary.png")
    
except Exception as e:
    print(f"⚠️  Could not create dashboards: {e}")

# Print quick summary
print("\n" + "="*80)
print("QUICK SUMMARY")
print("="*80)
print(f"✓ Simulation complete!")
print(f"\nTotal Cost ({config.simulation_days} days): ${metrics.total_cost:,.2f}")
print(f"Daily Cost: ${metrics.total_cost / config.simulation_days:,.2f}")
print(f"Monthly Projection (30 days): ${metrics.total_cost / config.simulation_days * 30:,.2f}")
print(f"Annual Projection (365 days): ${metrics.total_cost / config.simulation_days * 365:,.2f}")
print(f"\nQueries Executed: {metrics.total_queries:,}")
print(f"  - Dashboards: {metrics.dashboard_queries:,}")
print(f"  - Genie: {metrics.genie_queries:,}")
print(f"\nUser Experience (Genie P95 Wait Time): {metrics.genie_p95_wait_time:.2f}s")
if metrics.genie_p95_wait_time < 2:
    print("  ✓ Excellent!")
elif metrics.genie_p95_wait_time < 5:
    print("  ✓ Good")
elif metrics.genie_p95_wait_time < 10:
    print("  ⚠️  Acceptable, but consider optimizing")
else:
    print("  ⚠️  Poor - consider scaling up warehouse or min_clusters")

print(f"\nWarehouse Behavior:")
print(f"  - Average clusters: {metrics.avg_clusters:.2f}")
print(f"  - Runs {metrics.avg_clusters*100:.0f}% of the time")
if metrics.avg_clusters < 0.5:
    print(f"  - Scales to zero frequently (great for cost savings!)")
elif metrics.avg_clusters < 1.0:
    print(f"  - Good balance of cost vs performance")
else:
    print(f"  - Running most of the time (consider cost optimization)")

print(f"\n📊 Detailed results saved to 'results/' directory")
print("="*80 + "\n")

