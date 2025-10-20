"""
Visualization and reporting for simulation results.
"""

import matplotlib.pyplot as plt
import numpy as np
from typing import List, Optional
import logging
from pathlib import Path

from .simulator import SimulationMetrics
from .config import SimulationConfig


class SimulationReporter:
    """Generate reports and visualizations from simulation results."""
    
    def __init__(self, config: SimulationConfig, metrics: SimulationMetrics):
        self.config = config
        self.metrics = metrics
        self.logger = logging.getLogger(__name__)
    
    def print_summary(self):
        """Print a text summary of simulation results."""
        print("\n" + "=" * 80)
        print("DATABRICKS SERVERLESS SQL WAREHOUSE - COST SIMULATION RESULTS")
        print("=" * 80)
        
        print(f"\n{'SIMULATION PARAMETERS':-^80}")
        print(f"  Duration: {self.config.simulation_days} days")
        print(f"  Warehouse Size: {self.config.warehouse.size} ({self.config.warehouse.dbus_per_hour} DBUs/hour/cluster)")
        print(f"  Target Concurrency: {self.config.warehouse.target_concurrency_per_cluster} queries/cluster")
        print(f"  Dashboards: {self.config.dashboard.num_dashboards}")
        print(f"  Dashboard Refreshes/Day: {self.config.dashboard.refreshes_per_day}")
        print(f"  Peak Concurrent Users: {self.config.genie.peak_concurrent_users_min}-{self.config.genie.peak_concurrent_users_max}")
        print(f"  Queries/User/Hour: {self.config.genie.avg_queries_per_user_per_hour}")
        
        print(f"\n{'COST ANALYSIS':-^80}")
        print(f"  Total DBUs Consumed: {self.metrics.total_dbus:,.2f}")
        print(f"    - SQL Compute DBUs: {self.metrics.sql_dbus:,.2f}")
        print(f"    - GenAI Inference DBUs: {self.metrics.genai_dbus:,.2f}")
        print(f"  ")
        print(f"  Total Cost: ${self.metrics.total_cost:,.2f}")
        print(f"    - SQL Compute Cost: ${self.metrics.sql_cost:,.2f} (@ ${self.config.pricing.sql_serverless_dbu_rate}/DBU)")
        print(f"    - GenAI Inference Cost: ${self.metrics.genai_cost:,.2f} (@ ${self.config.pricing.serverless_realtime_inference_dbu_rate}/DBU)")
        print(f"  ")
        print(f"  Daily Average Cost: ${self.metrics.total_cost / self.config.simulation_days:,.2f}")
        print(f"  Monthly Projected Cost (30 days): ${self.metrics.total_cost / self.config.simulation_days * 30:,.2f}")
        print(f"  Annual Projected Cost (365 days): ${self.metrics.total_cost / self.config.simulation_days * 365:,.2f}")
        
        print(f"\n{'WORKLOAD ANALYSIS':-^80}")
        print(f"  Total Queries Executed: {self.metrics.total_queries:,}")
        
        if self.metrics.total_queries > 0:
            print(f"    - Dashboard Queries: {self.metrics.dashboard_queries:,} ({self.metrics.dashboard_queries/self.metrics.total_queries*100:.1f}%)")
            print(f"    - Genie Queries: {self.metrics.genie_queries:,} ({self.metrics.genie_queries/self.metrics.total_queries*100:.1f}%)")
            print(f"    - GenAI-enabled Queries: {self.metrics.genai_queries:,} ({self.metrics.genai_queries/self.metrics.total_queries*100:.1f}%)")
            print(f"  ")
            print(f"  Queries per Day: {self.metrics.total_queries / self.config.simulation_days:,.0f}")
            print(f"  Average DBUs per Query: {self.metrics.total_dbus / self.metrics.total_queries:.4f}")
            print(f"  Average Cost per Query: ${self.metrics.total_cost / self.metrics.total_queries:.4f}")
        else:
            print(f"    ⚠️  WARNING: No queries were executed!")
            print(f"    This may indicate a simulation configuration issue.")
        
        print(f"\n{'PERFORMANCE METRICS (GENIE QUERIES - USER EXPERIENCE)':-^80}")
        print(f"  Wait Time Statistics:")
        print(f"    - Average Wait: {self.metrics.genie_avg_wait_time:.2f} seconds")
        print(f"    - P50 Wait: {self.metrics.genie_p50_wait_time:.2f} seconds")
        print(f"    - P95 Wait: {self.metrics.genie_p95_wait_time:.2f} seconds")
        print(f"    - P99 Wait: {self.metrics.genie_p99_wait_time:.2f} seconds")
        
        if self.metrics.genie_p95_wait_time > 5.0:
            print(f"  ⚠️  WARNING: P95 wait time exceeds 5 seconds - consider scaling up")
        elif self.metrics.genie_p95_wait_time > 2.0:
            print(f"  ⚠️  NOTICE: P95 wait time exceeds 2 seconds - monitor user experience")
        else:
            print(f"  ✓ P95 wait time is acceptable for interactive queries")
        
        print(f"\n{'PERFORMANCE METRICS (DASHBOARD REFRESHES)':-^80}")
        print(f"  Wait Time Statistics:")
        print(f"    - Average Wait: {self.metrics.dashboard_avg_wait_time:.2f} seconds")
        print(f"    - P95 Wait: {self.metrics.dashboard_p95_wait_time:.2f} seconds")
        
        print(f"\n{'WAREHOUSE SCALING BEHAVIOR':-^80}")
        print(f"  Average Active Clusters: {self.metrics.avg_clusters:.2f}")
        print(f"  Peak Clusters: {self.metrics.max_clusters}")
        print(f"  Average Utilization: {self.metrics.avg_utilization*100:.1f}%")
        print(f"  Max Queue Depth: {self.metrics.max_queue_depth}")
        
        if self.metrics.avg_utilization < 0.3:
            print(f"  💡 TIP: Low utilization - consider smaller warehouse size")
        elif self.metrics.avg_utilization > 0.8:
            print(f"  💡 TIP: High utilization - warehouse is right-sized or may benefit from larger size")
        
        if self.metrics.max_queue_depth > 10:
            print(f"  ⚠️  WARNING: Significant queuing detected - consider larger warehouse or more clusters")
        
        print("\n" + "=" * 80 + "\n")
    
    def create_visualizations(self, output_dir: str = "results"):
        """
        Create comprehensive visualization charts.
        
        Args:
            output_dir: Directory to save charts
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        self.logger.info(f"Creating visualizations in {output_path}")
        
        # Create a comprehensive multi-panel figure
        fig = plt.figure(figsize=(20, 12))
        
        # 1. Warehouse scaling over time
        ax1 = plt.subplot(3, 3, 1)
        self._plot_warehouse_scaling(ax1)
        
        # 2. DBU consumption over time
        ax2 = plt.subplot(3, 3, 2)
        self._plot_dbu_consumption(ax2)
        
        # 3. Queue depth over time
        ax3 = plt.subplot(3, 3, 3)
        self._plot_queue_depth(ax3)
        
        # 4. Utilization over time
        ax4 = plt.subplot(3, 3, 4)
        self._plot_utilization(ax4)
        
        # 5. Genie wait time distribution
        ax5 = plt.subplot(3, 3, 5)
        self._plot_genie_wait_distribution(ax5)
        
        # 6. Cost breakdown
        ax6 = plt.subplot(3, 3, 6)
        self._plot_cost_breakdown(ax6)
        
        # 7. Query volume by hour of day
        ax7 = plt.subplot(3, 3, 7)
        self._plot_query_volume_by_hour(ax7)
        
        # 8. Wait time percentiles comparison
        ax8 = plt.subplot(3, 3, 8)
        self._plot_wait_time_comparison(ax8)
        
        # 9. Active queries over time
        ax9 = plt.subplot(3, 3, 9)
        self._plot_active_queries(ax9)
        
        plt.tight_layout()
        output_file = output_path / "simulation_results.png"
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        self.logger.info(f"Saved comprehensive visualization to {output_file}")
        plt.close()
        
        # Create additional detailed charts
        self._create_detailed_wait_time_chart(output_path)
        self._create_cost_projection_chart(output_path)
    
    def _plot_warehouse_scaling(self, ax):
        """Plot warehouse cluster count over time."""
        if not self.metrics.state_history:
            return
        
        times = [s.time / 3600 for s in self.metrics.state_history]  # Convert to hours
        clusters = [s.num_clusters for s in self.metrics.state_history]
        
        ax.plot(times, clusters, linewidth=2, color='#2E86AB')
        ax.fill_between(times, clusters, alpha=0.3, color='#2E86AB')
        ax.set_xlabel('Time (hours)')
        ax.set_ylabel('Number of Clusters')
        ax.set_title('Warehouse Scaling Behavior')
        ax.grid(True, alpha=0.3)
    
    def _plot_dbu_consumption(self, ax):
        """Plot cumulative DBU consumption over time."""
        if not self.metrics.state_history:
            return
        
        times = [s.time / 3600 for s in self.metrics.state_history]
        dbus = [s.dbu_consumption for s in self.metrics.state_history]
        
        ax.plot(times, dbus, linewidth=2, color='#A23B72')
        ax.fill_between(times, dbus, alpha=0.3, color='#A23B72')
        ax.set_xlabel('Time (hours)')
        ax.set_ylabel('Cumulative DBUs')
        ax.set_title('DBU Consumption Over Time')
        ax.grid(True, alpha=0.3)
        
        # Add cost annotation
        total_cost = dbus[-1] * self.config.pricing.sql_serverless_dbu_rate if dbus else 0
        ax.text(0.98, 0.98, f'Total: ${total_cost:,.2f}', 
                transform=ax.transAxes, ha='right', va='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    def _plot_queue_depth(self, ax):
        """Plot query queue depth over time."""
        if not self.metrics.state_history:
            return
        
        times = [s.time / 3600 for s in self.metrics.state_history]
        queue = [s.queued_queries for s in self.metrics.state_history]
        
        ax.plot(times, queue, linewidth=2, color='#F18F01')
        ax.fill_between(times, queue, alpha=0.3, color='#F18F01')
        ax.set_xlabel('Time (hours)')
        ax.set_ylabel('Queued Queries')
        ax.set_title('Query Queue Depth')
        ax.grid(True, alpha=0.3)
        
        if max(queue) > 0:
            ax.axhline(y=10, color='r', linestyle='--', alpha=0.5, label='High Queue Threshold')
            ax.legend()
    
    def _plot_utilization(self, ax):
        """Plot warehouse utilization over time."""
        if not self.metrics.state_history:
            return
        
        times = [s.time / 3600 for s in self.metrics.state_history]
        utilization = [s.utilization() * 100 for s in self.metrics.state_history]
        
        ax.plot(times, utilization, linewidth=2, color='#06A77D')
        ax.fill_between(times, utilization, alpha=0.3, color='#06A77D')
        ax.set_xlabel('Time (hours)')
        ax.set_ylabel('Utilization (%)')
        ax.set_title('Warehouse Utilization')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 100)
        
        # Add reference lines
        ax.axhline(y=80, color='orange', linestyle='--', alpha=0.5, label='Scale-up threshold')
        ax.axhline(y=30, color='blue', linestyle='--', alpha=0.5, label='Scale-down threshold')
        ax.legend(fontsize='small')
    
    def _plot_genie_wait_distribution(self, ax):
        """Plot distribution of Genie query wait times."""
        if not self.metrics.genie_wait_times:
            return
        
        wait_times = np.array(self.metrics.genie_wait_times)
        
        ax.hist(wait_times, bins=50, color='#4ECDC4', alpha=0.7, edgecolor='black')
        ax.axvline(self.metrics.genie_p95_wait_time, color='r', linestyle='--', 
                   linewidth=2, label=f'P95: {self.metrics.genie_p95_wait_time:.2f}s')
        ax.axvline(self.metrics.genie_avg_wait_time, color='green', linestyle='--', 
                   linewidth=2, label=f'Avg: {self.metrics.genie_avg_wait_time:.2f}s')
        ax.set_xlabel('Wait Time (seconds)')
        ax.set_ylabel('Frequency')
        ax.set_title('Genie Query Wait Time Distribution')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    def _plot_cost_breakdown(self, ax):
        """Plot cost breakdown by component."""
        labels = ['SQL Compute', 'GenAI Inference']
        costs = [self.metrics.sql_cost, self.metrics.genai_cost]
        colors = ['#2E86AB', '#A23B72']
        
        # Filter out zero costs
        filtered_data = [(l, c, col) for l, c, col in zip(labels, costs, colors) if c > 0]
        if not filtered_data:
            return
        
        labels, costs, colors = zip(*filtered_data)
        
        wedges, texts, autotexts = ax.pie(costs, labels=labels, colors=colors, autopct='%1.1f%%',
                                            startangle=90)
        ax.set_title(f'Cost Breakdown\nTotal: ${sum(costs):,.2f}')
        
        # Make percentage text more readable
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
    
    def _plot_query_volume_by_hour(self, ax):
        """Plot query volume by hour of day."""
        # This requires tracking queries by hour - simplified version
        ax.text(0.5, 0.5, 'Query Volume by Hour\n(Aggregation in progress)', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title('Query Volume by Hour of Day')
    
    def _plot_wait_time_comparison(self, ax):
        """Compare wait time percentiles for different query types."""
        categories = ['Avg', 'P50', 'P95', 'P99']
        genie_times = [
            self.metrics.genie_avg_wait_time,
            self.metrics.genie_p50_wait_time,
            self.metrics.genie_p95_wait_time,
            self.metrics.genie_p99_wait_time
        ]
        dashboard_times = [
            self.metrics.dashboard_avg_wait_time,
            0,  # P50 not tracked
            self.metrics.dashboard_p95_wait_time,
            0   # P99 not tracked
        ]
        
        x = np.arange(len(categories))
        width = 0.35
        
        ax.bar(x - width/2, genie_times, width, label='Genie', color='#4ECDC4')
        ax.bar(x + width/2, dashboard_times, width, label='Dashboard', color='#FF6B6B')
        
        ax.set_xlabel('Percentile')
        ax.set_ylabel('Wait Time (seconds)')
        ax.set_title('Wait Time Comparison')
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
    
    def _plot_active_queries(self, ax):
        """Plot number of active queries over time."""
        if not self.metrics.state_history:
            return
        
        times = [s.time / 3600 for s in self.metrics.state_history]
        active = [s.active_queries for s in self.metrics.state_history]
        capacity = [s.total_capacity for s in self.metrics.state_history]
        
        ax.plot(times, active, linewidth=2, label='Active Queries', color='#2E86AB')
        ax.plot(times, capacity, linewidth=2, linestyle='--', label='Total Capacity', color='#06A77D')
        ax.fill_between(times, active, alpha=0.3, color='#2E86AB')
        
        ax.set_xlabel('Time (hours)')
        ax.set_ylabel('Query Count')
        ax.set_title('Active Queries vs Capacity')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    def _create_detailed_wait_time_chart(self, output_path: Path):
        """Create detailed wait time analysis chart."""
        if not self.metrics.genie_wait_times:
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Histogram
        axes[0, 0].hist(self.metrics.genie_wait_times, bins=50, color='#4ECDC4', 
                        alpha=0.7, edgecolor='black')
        axes[0, 0].axvline(self.metrics.genie_p95_wait_time, color='r', 
                          linestyle='--', linewidth=2, label=f'P95: {self.metrics.genie_p95_wait_time:.2f}s')
        axes[0, 0].set_xlabel('Wait Time (seconds)')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].set_title('Genie Wait Time Distribution')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # CDF
        sorted_times = np.sort(self.metrics.genie_wait_times)
        cdf = np.arange(1, len(sorted_times) + 1) / len(sorted_times)
        axes[0, 1].plot(sorted_times, cdf * 100, linewidth=2, color='#2E86AB')
        axes[0, 1].axhline(y=95, color='r', linestyle='--', alpha=0.5, label='P95')
        axes[0, 1].axvline(x=self.metrics.genie_p95_wait_time, color='r', 
                          linestyle='--', alpha=0.5)
        axes[0, 1].set_xlabel('Wait Time (seconds)')
        axes[0, 1].set_ylabel('Cumulative Probability (%)')
        axes[0, 1].set_title('Cumulative Distribution Function')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # Box plot
        axes[1, 0].boxplot([self.metrics.genie_wait_times], vert=True, patch_artist=True,
                           boxprops=dict(facecolor='#4ECDC4', alpha=0.7))
        axes[1, 0].set_ylabel('Wait Time (seconds)')
        axes[1, 0].set_title('Genie Wait Time Box Plot')
        axes[1, 0].set_xticklabels(['Genie Queries'])
        axes[1, 0].grid(True, alpha=0.3, axis='y')
        
        # Percentile table
        axes[1, 1].axis('off')
        percentiles = [50, 75, 90, 95, 99, 99.9]
        values = [np.percentile(self.metrics.genie_wait_times, p) for p in percentiles]
        
        table_data = [['Percentile', 'Wait Time (s)']]
        for p, v in zip(percentiles, values):
            table_data.append([f'P{p}', f'{v:.3f}'])
        
        table = axes[1, 1].table(cellText=table_data, cellLoc='center', loc='center',
                                colWidths=[0.4, 0.4])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # Style header row
        for i in range(2):
            table[(0, i)].set_facecolor('#2E86AB')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        axes[1, 1].set_title('Wait Time Percentiles', pad=20)
        
        plt.tight_layout()
        output_file = output_path / "wait_time_analysis.png"
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        self.logger.info(f"Saved wait time analysis to {output_file}")
        plt.close()
    
    def _create_cost_projection_chart(self, output_path: Path):
        """Create cost projection chart."""
        fig, axes = plt.subplots(1, 2, figsize=(15, 5))
        
        # Monthly cost projection
        months = np.arange(1, 13)
        monthly_cost = (self.metrics.total_cost / self.config.simulation_days) * 30
        
        axes[0].bar(months, [monthly_cost] * 12, color='#2E86AB', alpha=0.7)
        axes[0].set_xlabel('Month')
        axes[0].set_ylabel('Cost ($)')
        axes[0].set_title(f'Monthly Cost Projection: ${monthly_cost:,.2f}/month')
        axes[0].grid(True, alpha=0.3, axis='y')
        axes[0].set_xticks(months)
        
        # Cost by warehouse size comparison (based on actual DBU rates)
        sizes = ['2XSmall', 'XSmall', 'Small', 'Medium', 'Large', 'XLarge']
        size_multipliers = [0.167, 0.25, 0.5, 1.0, 1.67, 3.33]  # Relative to Medium (24 DBUs/hour)
        costs = [monthly_cost * m for m in size_multipliers]
        
        colors = ['#06A77D' if s == self.config.warehouse.size else '#CCCCCC' for s in sizes]
        axes[1].bar(sizes, costs, color=colors, alpha=0.7)
        axes[1].set_xlabel('Warehouse Size')
        axes[1].set_ylabel('Monthly Cost ($)')
        axes[1].set_title('Cost by Warehouse Size (Estimated)')
        axes[1].grid(True, alpha=0.3, axis='y')
        axes[1].tick_params(axis='x', rotation=45)
        
        # Highlight current size if in list
        if self.config.warehouse.size in sizes:
            current_idx = sizes.index(self.config.warehouse.size)
            axes[1].text(current_idx, costs[current_idx], 'Current', 
                        ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        output_file = output_path / "cost_projections.png"
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        self.logger.info(f"Saved cost projections to {output_file}")
        plt.close()
    
    def save_csv_reports(self, output_dir: str = "results"):
        """Save detailed CSV reports."""
        import csv
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # State history CSV
        if self.metrics.state_history:
            csv_file = output_path / "warehouse_state_history.csv"
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Time (s)', 'Time (hours)', 'Clusters', 'Active Queries', 
                               'Queued Queries', 'Capacity', 'Utilization (%)', 
                               'Cumulative DBUs', 'Cumulative Cost ($)'])
                
                for state in self.metrics.state_history:
                    writer.writerow([
                        state.time,
                        state.time / 3600,
                        state.num_clusters,
                        state.active_queries,
                        state.queued_queries,
                        state.total_capacity,
                        state.utilization() * 100,
                        state.dbu_consumption,
                        state.dbu_consumption * self.config.pricing.sql_serverless_dbu_rate
                    ])
            
            self.logger.info(f"Saved state history to {csv_file}")


def generate_report(config: SimulationConfig, metrics: SimulationMetrics, 
                   output_dir: str = "results"):
    """
    Generate complete report with visualizations.
    
    Args:
        config: Simulation configuration
        metrics: Simulation metrics
        output_dir: Output directory for reports
    """
    reporter = SimulationReporter(config, metrics)
    reporter.print_summary()
    reporter.create_visualizations(output_dir)
    reporter.save_csv_reports(output_dir)

