"""
Create the charts-only view of the simulation dashboard.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
colors = {
    'primary': '#2E86AB',
    'secondary': '#A23B72', 
    'success': '#06A77D',
    'warning': '#F18F01',
    'danger': '#C73E1D',
    'info': '#4ECDC4'
}

def create_charts_dashboard():
    """Create a clean charts-only dashboard."""
    
    # Load data
    df = pd.read_csv('results/warehouse_state_history.csv')
    
    # Create figure
    fig = plt.figure(figsize=(24, 14))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    times = df['Time (hours)']
    
    # ========================================================================
    # PANEL 1: Cluster Count with Zero Highlighting
    # ========================================================================
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(times, df['Clusters'], linewidth=2, color=colors['primary'], label='Active Clusters')
    ax1.fill_between(times, df['Clusters'], alpha=0.3, color=colors['primary'])
    
    # Highlight zero periods
    zero_mask = df['Clusters'] == 0
    if zero_mask.any():
        ax1.fill_between(times, 0, df['Clusters'].max() + 0.5, 
                        where=zero_mask, alpha=0.2, color='red', label='Scaled to Zero')
    
    ax1.axhline(y=0, color='red', linestyle='--', linewidth=2, alpha=0.7)
    ax1.set_xlabel('Time (hours)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Number of Clusters', fontsize=12, fontweight='bold')
    ax1.set_title('Warehouse Scaling: Auto-Suspend & Auto-Resume Behavior', 
                  fontsize=15, fontweight='bold', pad=15)
    ax1.legend(loc='upper right', fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(-0.3, df['Clusters'].max() + 0.5)
    
    # Add statistics box
    zero_pct = (zero_mask.sum() / len(df)) * 100
    avg_clusters = df['Clusters'].mean()
    textstr = f'Avg Clusters: {avg_clusters:.2f}\nZero Time: {zero_pct:.1f}%'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
    ax1.text(0.02, 0.98, textstr, transform=ax1.transAxes, fontsize=11,
            verticalalignment='top', bbox=props)
    
    # ========================================================================
    # PANEL 2: Utilization with Thresholds
    # ========================================================================
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.plot(times, df['Utilization (%)'], linewidth=2, color=colors['success'], 
             label='Utilization')
    ax2.fill_between(times, df['Utilization (%)'], alpha=0.3, color=colors['success'])
    
    # Add threshold lines
    ax2.axhline(y=80, color='orange', linestyle='--', linewidth=1.5, 
                alpha=0.7, label='Scale-Up (80%)')
    ax2.axhline(y=30, color='blue', linestyle='--', linewidth=1.5, 
                alpha=0.7, label='Scale-Down (30%)')
    
    ax2.set_xlabel('Time (hours)', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Utilization (%)', fontsize=11, fontweight='bold')
    ax2.set_title('Warehouse Utilization', fontsize=13, fontweight='bold', pad=10)
    ax2.legend(loc='upper right', fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(-5, 105)
    
    # ========================================================================
    # PANEL 3: Active Queries vs Capacity
    # ========================================================================
    ax3 = fig.add_subplot(gs[1, 1])
    ax3.plot(times, df['Active Queries'], linewidth=2, color=colors['primary'], 
             label='Active Queries')
    ax3.plot(times, df['Capacity'], linewidth=2, linestyle='--', 
             color=colors['success'], label='Total Capacity', alpha=0.8)
    ax3.fill_between(times, df['Active Queries'], alpha=0.3, color=colors['primary'])
    
    ax3.set_xlabel('Time (hours)', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Query Count', fontsize=11, fontweight='bold')
    ax3.set_title('Active Queries vs Capacity', fontsize=13, fontweight='bold', pad=10)
    ax3.legend(loc='upper right', fontsize=9)
    ax3.grid(True, alpha=0.3)
    
    # ========================================================================
    # PANEL 4: Queue Depth
    # ========================================================================
    ax4 = fig.add_subplot(gs[1, 2])
    ax4.plot(times, df['Queued Queries'], linewidth=2, color=colors['warning'])
    ax4.fill_between(times, df['Queued Queries'], alpha=0.3, color=colors['warning'])
    
    if df['Queued Queries'].max() > 10:
        ax4.axhline(y=10, color='red', linestyle='--', linewidth=1.5, 
                    alpha=0.7, label='High Queue Warning')
        ax4.legend(loc='upper right', fontsize=9)
    
    ax4.set_xlabel('Time (hours)', fontsize=11, fontweight='bold')
    ax4.set_ylabel('Queued Queries', fontsize=11, fontweight='bold')
    ax4.set_title('Query Queue Depth', fontsize=13, fontweight='bold', pad=10)
    ax4.grid(True, alpha=0.3)
    
    # ========================================================================
    # PANEL 5: DBU Consumption
    # ========================================================================
    ax5 = fig.add_subplot(gs[2, 0])
    ax5.plot(times, df['Cumulative DBUs'], linewidth=2.5, color=colors['secondary'])
    ax5.fill_between(times, df['Cumulative DBUs'], alpha=0.3, color=colors['secondary'])
    
    ax5.set_xlabel('Time (hours)', fontsize=11, fontweight='bold')
    ax5.set_ylabel('Cumulative DBUs', fontsize=11, fontweight='bold')
    ax5.set_title('DBU Consumption Over Time', fontsize=13, fontweight='bold', pad=10)
    ax5.grid(True, alpha=0.3)
    
    # ========================================================================
    # PANEL 6: Cost Accumulation
    # ========================================================================
    ax6 = fig.add_subplot(gs[2, 1])
    ax6.plot(times, df['Cumulative Cost ($)'], linewidth=2.5, color=colors['danger'])
    ax6.fill_between(times, df['Cumulative Cost ($)'], alpha=0.3, color=colors['danger'])
    
    ax6.set_xlabel('Time (hours)', fontsize=11, fontweight='bold')
    ax6.set_ylabel('Cumulative Cost ($)', fontsize=11, fontweight='bold')
    ax6.set_title('Cost Accumulation', fontsize=13, fontweight='bold', pad=10)
    ax6.grid(True, alpha=0.3)
    
    # ========================================================================
    # PANEL 7: Wait Time Distribution
    # ========================================================================
    ax7 = fig.add_subplot(gs[2, 2])
    
    try:
        import json
        with open('results/metrics_summary.json', 'r') as f:
            metrics = json.load(f)
        
        genie_avg = metrics['genie_avg_wait_time']
        genie_p50 = metrics['genie_p50_wait_time']
        genie_p95 = metrics['genie_p95_wait_time']
        genie_p99 = metrics['genie_p99_wait_time']
        
        categories = ['Avg', 'P50', 'P95', 'P99']
        values = [genie_avg, genie_p50, genie_p95, genie_p99]
        
        # Color bars based on performance
        bar_colors = []
        for val in values:
            if val < 2:
                bar_colors.append(colors['success'])
            elif val < 5:
                bar_colors.append(colors['info'])
            elif val < 10:
                bar_colors.append(colors['warning'])
            else:
                bar_colors.append(colors['danger'])
        
        bars = ax7.bar(categories, values, color=bar_colors, alpha=0.8, 
                      edgecolor='black', linewidth=1.5)
        
        # Add value labels
        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax7.text(bar.get_x() + bar.get_width()/2., height,
                    f'{val:.2f}s', ha='center', va='bottom', 
                    fontsize=10, fontweight='bold')
        
        ax7.set_ylabel('Wait Time (seconds)', fontsize=11, fontweight='bold')
        ax7.set_title('Genie Wait Time Percentiles', fontsize=13, fontweight='bold', pad=10)
        ax7.grid(True, alpha=0.3, axis='y')
        
        # Add SLA reference lines
        ax7.axhline(y=2.0, color='green', linestyle='--', linewidth=1, 
                   alpha=0.4, label='Excellent')
        ax7.axhline(y=5.0, color='orange', linestyle='--', linewidth=1, 
                   alpha=0.4, label='Good')
        ax7.axhline(y=10.0, color='red', linestyle='--', linewidth=1, 
                   alpha=0.4, label='Acceptable')
        ax7.legend(loc='upper left', fontsize=8)
        
    except Exception as e:
        ax7.text(0.5, 0.5, 'Wait Time Data\n(Run simulation first)', 
                ha='center', va='center', transform=ax7.transAxes, fontsize=11)
        ax7.set_title('Genie Wait Time Percentiles', fontsize=13, fontweight='bold', pad=10)
    
    # ========================================================================
    # Overall title and footer
    # ========================================================================
    fig.suptitle('Databricks Serverless SQL Warehouse - Simulation Charts', 
                 fontsize=18, fontweight='bold', y=0.995)
    
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fig.text(0.5, 0.005, f'Generated: {timestamp}', 
             ha='center', fontsize=9, style='italic', color='gray')
    
    # Save
    plt.savefig('results/dashboard_charts.png', dpi=200, bbox_inches='tight')
    print('✅ Created charts dashboard: results/dashboard_charts.png')
    
    return fig

if __name__ == '__main__':
    create_charts_dashboard()
    plt.close()

