"""
Create the summary statistics view of the simulation dashboard.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np
import yaml
from pathlib import Path

# Set style
plt.style.use('seaborn-v0_8-whitegrid')

def create_summary_dashboard():
    """Create a clean summary statistics dashboard."""
    
    # Load data
    df = pd.read_csv('results/warehouse_state_history.csv')
    
    # Load config for inputs
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        config_loaded = True
    except:
        config_loaded = False
        print("Warning: Could not load config.yaml")
    
    # Load metrics for results
    try:
        import json
        with open('results/metrics_summary.json', 'r') as f:
            metrics = json.load(f)
        metrics_loaded = True
    except:
        metrics_loaded = False
        print("Warning: Could not load metrics_summary.json")
    
    # Create figure with 3 columns
    fig = plt.figure(figsize=(24, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3)
    
    # ========================================================================
    # COLUMN 1: SIMULATION INPUTS & CONFIGURATION
    # ========================================================================
    ax_inputs = fig.add_subplot(gs[:, 0])
    ax_inputs.axis('off')
    
    if config_loaded:
        # DBU mapping for warehouse sizes
        dbu_mapping = {
            "2XSmall": 4.0, "XSmall": 6.0, "Small": 12.0, "Medium": 24.0,
            "Large": 40.0, "XLarge": 80.0, "2XLarge": 144.0, 
            "3XLarge": 272.0, "4XLarge": 528.0
        }
        warehouse_size = config['warehouse']['size']
        dbus_per_hour = dbu_mapping.get(warehouse_size, 0.0)
        min_clusters = config['warehouse']['min_clusters']
        
        inputs_data = [
            ['SIMULATION CONFIGURATION', ''],
            ['', ''],
            ['Duration', f"{config['simulation']['days']} days"],
            ['', ''],
            ['WAREHOUSE SETTINGS', ''],
            ['', ''],
            ['Size', warehouse_size],
            ['DBUs per Hour', f"{dbus_per_hour:.1f} per cluster"],
            ['Min Clusters', f"{min_clusters} {'(auto-suspend)' if min_clusters == 0 else '(always-on)'}"],
            ['Max Clusters', f"{config['warehouse']['max_clusters']}"],
            ['Target Concurrency', f"{config['warehouse']['target_concurrency_per_cluster']} queries/cluster"],
            ['Idle Shutdown', f"{config['warehouse']['idle_shutdown_seconds']:.0f} seconds"],
            ['', ''],
            ['WORKLOAD SETTINGS', ''],
            ['', ''],
            ['Dashboards', f"{config['dashboard']['num_dashboards']}"],
            ['Refreshes per Day', f"{config['dashboard']['refreshes_per_day']}"],
            ['', ''],
            ['Peak Concurrent Users', f"{config['genie']['peak_concurrent_users_min']}-{config['genie']['peak_concurrent_users_max']}"],
            ['Queries per User/Hour', f"{config['genie']['avg_queries_per_user_per_hour']}"],
            ['Cache Hit Rate', f"{config['genie']['cache_hit_rate']*100:.0f}%"],
            ['Business Hours', f"{config['genie']['business_hours_start']}-{config['genie']['business_hours_end']}"],
            ['', ''],
            ['PRICING', ''],
            ['', ''],
            ['SQL Serverless Rate', f"${config['pricing']['sql_serverless_dbu_rate']:.3f} / DBU"],
            ['GenAI Inference Rate', f"${config['pricing']['serverless_realtime_inference_dbu_rate']:.3f} / DBU"],
        ]
    else:
        inputs_data = [
            ['SIMULATION CONFIGURATION', ''],
            ['', ''],
            ['Status', 'Config file not found'],
        ]
    
    table_inputs = ax_inputs.table(cellText=inputs_data, cellLoc='left', loc='center',
                                   colWidths=[0.55, 0.45])
    table_inputs.auto_set_font_size(False)
    table_inputs.set_fontsize(11)
    table_inputs.scale(1, 2.2)
    
    # Style header rows
    header_color = '#2E86AB'
    for i, row in enumerate(inputs_data):
        if row[0] in ['SIMULATION CONFIGURATION', 'WAREHOUSE SETTINGS', 'WORKLOAD SETTINGS', 'PRICING']:
            table_inputs[(i, 0)].set_facecolor(header_color)
            table_inputs[(i, 0)].set_text_props(weight='bold', color='white', fontsize=12)
            table_inputs[(i, 1)].set_facecolor(header_color)
        elif row[0] == '':
            table_inputs[(i, 0)].set_facecolor('#F0F0F0')
            table_inputs[(i, 1)].set_facecolor('#F0F0F0')
        else:
            table_inputs[(i, 0)].set_text_props(weight='bold')
    
    ax_inputs.set_title('Configuration & Inputs', fontsize=16, fontweight='bold', pad=30)
    
    # ========================================================================
    # COLUMN 2: COST ANALYSIS
    # ========================================================================
    ax_costs = fig.add_subplot(gs[:, 1])
    ax_costs.axis('off')
    
    if metrics_loaded:
        # Calculate additional metrics
        sim_hours = metrics['simulation_days'] * 24
        dbus_per_hour = metrics['total_dbus'] / sim_hours
        hourly_cost = metrics['total_cost'] / sim_hours
        
        costs_data = [
            ['COST SUMMARY', ''],
            ['', ''],
            ['Total DBUs Consumed', f"{metrics['total_dbus']:,.1f}"],
            ['Total Cost', f"${metrics['total_cost']:,.2f}"],
            ['', ''],
            ['RATES', ''],
            ['', ''],
            ['DBUs per Hour', f"{dbus_per_hour:.2f}"],
            ['Cost per Hour', f"${hourly_cost:.2f}"],
            ['Cost per Day', f"${metrics['daily_cost']:,.2f}"],
            ['', ''],
            ['PROJECTIONS', ''],
            ['', ''],
            ['Weekly Cost (7 days)', f"${metrics['daily_cost'] * 7:,.2f}"],
            ['Monthly Cost (30 days)', f"${metrics['monthly_cost']:,.2f}"],
            ['Quarterly (90 days)', f"${metrics['monthly_cost'] * 3:,.2f}"],
            ['Annual Cost (365 days)', f"${metrics['annual_cost']:,.2f}"],
            ['', ''],
            ['EFFICIENCY METRICS', ''],
            ['', ''],
            ['Total Queries', f"{metrics['total_queries']:,}"],
            ['DBUs per Query', f"{metrics['total_dbus'] / metrics['total_queries']:.4f}"],
            ['Cost per Query', f"${metrics['total_cost'] / metrics['total_queries']:.4f}"],
            ['', ''],
            ['Queries per Day', f"{metrics['total_queries'] / metrics['simulation_days']:,.0f}"],
            ['Queries per Hour', f"{metrics['total_queries'] / sim_hours:,.0f}"],
        ]
    else:
        costs_data = [
            ['COST SUMMARY', ''],
            ['', ''],
            ['Status', 'Run simulation first'],
        ]
    
    table_costs = ax_costs.table(cellText=costs_data, cellLoc='left', loc='center',
                                 colWidths=[0.55, 0.45])
    table_costs.auto_set_font_size(False)
    table_costs.set_fontsize(11)
    table_costs.scale(1, 2.2)
    
    # Style header rows
    for i, row in enumerate(costs_data):
        if row[0] in ['COST SUMMARY', 'RATES', 'PROJECTIONS', 'EFFICIENCY METRICS']:
            table_costs[(i, 0)].set_facecolor('#A23B72')
            table_costs[(i, 0)].set_text_props(weight='bold', color='white', fontsize=12)
            table_costs[(i, 1)].set_facecolor('#A23B72')
        elif row[0] == '':
            table_costs[(i, 0)].set_facecolor('#F0F0F0')
            table_costs[(i, 1)].set_facecolor('#F0F0F0')
        else:
            table_costs[(i, 0)].set_text_props(weight='bold')
            # Highlight key projections
            if row[0] in ['Monthly Cost (30 days)', 'Annual Cost (365 days)']:
                table_costs[(i, 0)].set_facecolor('#FFF3CD')
                table_costs[(i, 1)].set_facecolor('#FFF3CD')
                table_costs[(i, 1)].set_text_props(weight='bold', color='#856404')
    
    ax_costs.set_title('Cost Analysis & Projections', fontsize=16, fontweight='bold', pad=30)
    
    # ========================================================================
    # COLUMN 3: PERFORMANCE & WAIT TIMES
    # ========================================================================
    ax_perf = fig.add_subplot(gs[:, 2])
    ax_perf.axis('off')
    
    if metrics_loaded:
        # Calculate cluster statistics
        zero_mask = df['Clusters'] == 0
        zero_pct = (zero_mask.sum() / len(df)) * 100
        
        # Find zero periods
        zero_periods = []
        in_zero = False
        for i, row in df.iterrows():
            if row['Clusters'] == 0 and not in_zero:
                in_zero = True
            elif row['Clusters'] > 0 and in_zero:
                zero_periods.append(1)
                in_zero = False
        num_zero_periods = len(zero_periods)
        
        # Performance assessment
        p95_wait = metrics['genie_p95_wait_time']
        if p95_wait < 2:
            p95_status = '✓ Excellent'
            p95_color = '#28A745'
        elif p95_wait < 5:
            p95_status = '✓ Good'
            p95_color = '#17A2B8'
        elif p95_wait < 10:
            p95_status = '⚠ Acceptable'
            p95_color = '#FFC107'
        else:
            p95_status = '⚠ Poor - Scale Up'
            p95_color = '#DC3545'
        
        perf_data = [
            ['GENIE WAIT TIMES', ''],
            ['', ''],
            ['Average Wait', f"{metrics['genie_avg_wait_time']:.2f} seconds"],
            ['P50 (Median)', f"{metrics['genie_p50_wait_time']:.2f} seconds"],
            ['P95 (95th percentile)', f"{metrics['genie_p95_wait_time']:.2f} seconds"],
            ['P99 (99th percentile)', f"{metrics['genie_p99_wait_time']:.2f} seconds"],
            ['', ''],
            ['P95 Assessment', p95_status],
            ['', ''],
            ['WAREHOUSE BEHAVIOR', ''],
            ['', ''],
            ['Average Active Clusters', f"{metrics['avg_clusters']:.2f}"],
            ['Peak Clusters', f"{metrics['max_clusters']}"],
            ['Average Utilization', f"{metrics['avg_utilization']*100:.1f}%"],
            ['', ''],
            ['SCALE-TO-ZERO ANALYSIS', ''],
            ['', ''],
            ['Time at Zero', f"{zero_pct:.1f}%"],
            ['Zero-Scale Events', f"{num_zero_periods}"],
            ['Cost Savings', f"~{(1 - metrics['avg_clusters'])*100:.0f}% vs always-on"],
            ['', ''],
            ['QUEUE STATISTICS', ''],
            ['', ''],
            ['Max Queue Depth', f"{metrics['max_queue_depth']}"],
            ['Queries Queued', f"{(df['Queued Queries'] > 0).sum()} events"],
        ]
        
        # Add row for P95 color
        p95_row_idx = None
        for i, row in enumerate(perf_data):
            if row[0] == 'P95 Assessment':
                p95_row_idx = i
                break
    else:
        perf_data = [
            ['PERFORMANCE METRICS', ''],
            ['', ''],
            ['Status', 'Run simulation first'],
        ]
        p95_row_idx = None
    
    table_perf = ax_perf.table(cellText=perf_data, cellLoc='left', loc='center',
                               colWidths=[0.55, 0.45])
    table_perf.auto_set_font_size(False)
    table_perf.set_fontsize(11)
    table_perf.scale(1, 2.2)
    
    # Style header rows
    for i, row in enumerate(perf_data):
        if row[0] in ['GENIE WAIT TIMES', 'WAREHOUSE BEHAVIOR', 
                      'SCALE-TO-ZERO ANALYSIS', 'QUEUE STATISTICS']:
            table_perf[(i, 0)].set_facecolor('#06A77D')
            table_perf[(i, 0)].set_text_props(weight='bold', color='white', fontsize=12)
            table_perf[(i, 1)].set_facecolor('#06A77D')
        elif row[0] == '':
            table_perf[(i, 0)].set_facecolor('#F0F0F0')
            table_perf[(i, 1)].set_facecolor('#F0F0F0')
        else:
            table_perf[(i, 0)].set_text_props(weight='bold')
            # Highlight P95
            if row[0] in ['P95 (95th percentile)']:
                table_perf[(i, 0)].set_facecolor('#FFF9E6')
                table_perf[(i, 1)].set_facecolor('#FFF9E6')
                table_perf[(i, 1)].set_text_props(weight='bold')
        
        # Color the P95 assessment
        if i == p95_row_idx and metrics_loaded:
            table_perf[(i, 1)].set_text_props(weight='bold', color='white')
            table_perf[(i, 1)].set_facecolor(p95_color)
    
    ax_perf.set_title('Performance & Wait Time Analysis', fontsize=16, fontweight='bold', pad=30)
    
    # ========================================================================
    # Overall title and footer
    # ========================================================================
    fig.suptitle('Databricks Serverless SQL Warehouse - Simulation Summary', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fig.text(0.5, 0.01, f'Generated: {timestamp}', 
             ha='center', fontsize=10, style='italic', color='gray')
    
    # Save
    plt.savefig('results/dashboard_summary.png', dpi=200, bbox_inches='tight')
    print('✅ Created summary dashboard: results/dashboard_summary.png')
    
    return fig

if __name__ == '__main__':
    create_summary_dashboard()
    plt.close()

