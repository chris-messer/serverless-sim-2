"""
Event generation for dashboard refreshes and Genie queries.
"""

import numpy as np
from typing import List, Tuple
from dataclasses import dataclass
from .config import SimulationConfig


@dataclass
class Query:
    """Represents a single query (dashboard or Genie)."""
    
    query_id: int
    query_type: str  # "dashboard" or "genie"
    start_time: float  # seconds from simulation start
    duration: float  # seconds
    uses_genai: bool = False
    
    @property
    def end_time(self) -> float:
        """When the query completes."""
        return self.start_time + self.duration


class EventGenerator:
    """Generates query events for the simulation."""
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.query_counter = 0
        # Calculate performance multiplier based on warehouse size
        self._performance_multiplier = self._calculate_performance_multiplier()
    
    def _calculate_performance_multiplier(self) -> float:
        """
        Calculate query execution speed based on warehouse size.
        Larger warehouses execute queries faster.
        
        Returns:
            Performance multiplier (< 1.0 = faster, > 1.0 = slower)
            Baseline is Medium (24 DBUs) = 1.0x
        """
        dbus_per_hour = self.config.warehouse.dbus_per_hour
        baseline_dbus = 24.0  # Medium warehouse baseline
        
        # Use square root scaling for realistic performance gains
        # This means doubling DBUs gives ~1.4x performance improvement
        # Following typical compute scaling laws (sub-linear)
        multiplier = (baseline_dbus / dbus_per_hour) ** 0.5
        
        # Examples:
        # 2XSmall (4 DBU): (24/4)^0.5 = 2.45x slower
        # XSmall (6 DBU): (24/6)^0.5 = 2.0x slower  
        # Small (12 DBU): (24/12)^0.5 = 1.41x slower
        # Medium (24 DBU): 1.0x (baseline)
        # Large (40 DBU): (24/40)^0.5 = 0.77x (30% faster)
        # XLarge (80 DBU): (24/80)^0.5 = 0.55x (45% faster)
        # 2XLarge (160 DBU): (24/160)^0.5 = 0.39x (61% faster)
        
        return multiplier
    
    def generate_dashboard_queries(self) -> List[Query]:
        """
        Generate all dashboard refresh queries for the simulation period.
        
        Returns:
            List of Query objects representing dashboard refreshes
        """
        queries = []
        total_seconds = self.config.total_seconds
        seconds_per_refresh = (24 * 3600) / self.config.dashboard.refreshes_per_day
        
        for dashboard_id in range(self.config.dashboard.num_dashboards):
            # Calculate refresh times for this dashboard
            # Add some jitter based on overlap factor
            base_offset = dashboard_id * (seconds_per_refresh / self.config.dashboard.num_dashboards)
            jitter_scale = seconds_per_refresh * self.config.dashboard.refresh_overlap_factor
            
            for refresh_num in range(self.config.dashboard.refreshes_per_day * self.config.simulation_days):
                # Base refresh time
                refresh_time = refresh_num * seconds_per_refresh + base_offset
                
                # Add jitter to create overlap
                if self.config.dashboard.refresh_overlap_factor > 0:
                    jitter = np.random.normal(0, jitter_scale)
                    refresh_time += jitter
                
                # Skip if outside simulation window
                if refresh_time < 0 or refresh_time >= total_seconds:
                    continue
                
                # Generate runtime with variability
                runtime = np.random.normal(
                    self.config.dashboard.avg_refresh_runtime,
                    self.config.dashboard.refresh_runtime_std
                )
                runtime = np.clip(
                    runtime,
                    self.config.dashboard.min_refresh_runtime,
                    self.config.dashboard.max_refresh_runtime
                )
                
                # Apply warehouse performance scaling
                # Larger warehouses execute queries faster
                runtime = runtime * self._performance_multiplier
                
                query = Query(
                    query_id=self.query_counter,
                    query_type="dashboard",
                    start_time=refresh_time,
                    duration=runtime,
                    uses_genai=False
                )
                queries.append(query)
                self.query_counter += 1
        
        return sorted(queries, key=lambda q: q.start_time)
    
    def generate_genie_queries(self) -> List[Query]:
        """
        Generate Genie interactive queries based on user activity patterns.
        
        Returns:
            List of Query objects representing Genie queries
        """
        queries = []
        total_seconds = self.config.total_seconds
        
        # Simulate user activity throughout the day
        current_time = 0.0
        
        while current_time < total_seconds:
            # Determine if we're in business hours
            hour_of_day = (current_time / 3600) % 24
            is_business_hours = (
                self.config.genie.business_hours_start <= hour_of_day < self.config.genie.business_hours_end
            )
            
            if is_business_hours:
                # Determine concurrent users (varies during business hours)
                # Peak in middle of day
                hours_into_business_day = hour_of_day - self.config.genie.business_hours_start
                business_day_length = self.config.genie.business_hours_end - self.config.genie.business_hours_start
                
                # Bell curve for user activity
                peak_position = business_day_length / 2
                activity_factor = np.exp(-((hours_into_business_day - peak_position) ** 2) / (2 * (business_day_length / 4) ** 2))
                
                concurrent_users = int(
                    self.config.genie.peak_concurrent_users_min +
                    activity_factor * (self.config.genie.peak_concurrent_users_max - self.config.genie.peak_concurrent_users_min)
                )
            else:
                # Off hours: minimal activity
                concurrent_users = max(1, int(self.config.genie.peak_concurrent_users_min * 0.2))
            
            # Generate queries for active users in this time window
            # Each user generates queries according to avg_queries_per_user_per_hour
            queries_per_second = (concurrent_users * self.config.genie.avg_queries_per_user_per_hour) / 3600
            
            # Use Poisson process to generate query arrivals in next time window
            time_window = 60.0  # Generate queries for next minute
            expected_queries = queries_per_second * time_window
            num_queries = np.random.poisson(expected_queries)
            
            # Generate individual query times within this window
            for _ in range(num_queries):
                query_time = current_time + np.random.uniform(0, time_window)
                
                if query_time >= total_seconds:
                    break
                
                # Determine query duration (cache hit vs miss)
                is_cache_hit = np.random.random() < self.config.genie.cache_hit_rate
                
                if is_cache_hit:
                    duration = max(0.1, np.random.normal(
                        self.config.genie.cache_hit_avg_time,
                        self.config.genie.cache_hit_std
                    ))
                else:
                    duration = max(0.1, np.random.normal(
                        self.config.genie.cache_miss_avg_time,
                        self.config.genie.cache_miss_std
                    ))
                
                # Apply warehouse performance scaling
                # Larger warehouses execute queries faster
                duration = duration * self._performance_multiplier
                
                # Determine if query uses GenAI
                uses_genai = np.random.random() < self.config.genie.fraction_using_genai
                
                query = Query(
                    query_id=self.query_counter,
                    query_type="genie",
                    start_time=query_time,
                    duration=duration,
                    uses_genai=uses_genai
                )
                queries.append(query)
                self.query_counter += 1
            
            current_time += time_window
        
        return sorted(queries, key=lambda q: q.start_time)
    
    def generate_all_queries(self) -> Tuple[List[Query], List[Query]]:
        """
        Generate all queries for the simulation.
        
        Returns:
            Tuple of (dashboard_queries, genie_queries)
        """
        dashboard_queries = self.generate_dashboard_queries()
        genie_queries = self.generate_genie_queries()
        
        return dashboard_queries, genie_queries
    
    def merge_queries(self, dashboard_queries: List[Query], genie_queries: List[Query]) -> List[Query]:
        """
        Merge and sort all queries by start time.
        
        Args:
            dashboard_queries: List of dashboard queries
            genie_queries: List of Genie queries
        
        Returns:
            Combined and sorted list of all queries
        """
        all_queries = dashboard_queries + genie_queries
        return sorted(all_queries, key=lambda q: q.start_time)

