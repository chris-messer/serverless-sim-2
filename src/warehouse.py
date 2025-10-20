"""
Serverless SQL Warehouse autoscaling and resource management.
"""

import numpy as np
from typing import List, Tuple
from dataclasses import dataclass, field
from .config import SimulationConfig


@dataclass
class ClusterState:
    """State of a single cluster."""
    
    cluster_id: int
    active_queries: int = 0
    last_query_end_time: float = 0.0
    startup_time: float = 0.0
    shutdown_time: float = None
    
    def is_active(self, current_time: float, idle_timeout: float) -> bool:
        """Check if cluster is still active and available for queries."""
        # Check if cluster has been shut down
        if self.shutdown_time is not None and current_time >= self.shutdown_time:
            return False
        
        # Cluster with active queries is always active
        if self.active_queries > 0:
            return True
        
        # For idle clusters, check how long they've been idle
        # If never processed a query, check idle time from startup
        if self.last_query_end_time == 0.0:
            idle_time = current_time - self.startup_time
        else:
            idle_time = current_time - self.last_query_end_time
        
        # Cluster is active if idle time is within timeout
        return idle_time < idle_timeout
    
    def utilization(self, target_concurrency: int) -> float:
        """Calculate current utilization as fraction of target concurrency."""
        if target_concurrency == 0:
            return 0.0
        return self.active_queries / target_concurrency


@dataclass
class WarehouseState:
    """State of the entire warehouse at a point in time."""
    
    time: float
    num_clusters: int
    active_queries: int
    queued_queries: int
    total_capacity: int
    dbu_consumption: float = 0.0
    genai_dbu_consumption: float = 0.0
    
    def utilization(self) -> float:
        """Calculate overall utilization."""
        if self.total_capacity == 0:
            return 0.0
        return self.active_queries / self.total_capacity


class ServerlessWarehouse:
    """
    Simulates a Databricks Serverless SQL Warehouse with autoscaling.
    """
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.clusters: List[ClusterState] = []
        self.cluster_counter = 0
        
        # Start with minimum clusters
        for _ in range(self.config.warehouse.min_clusters):
            self._add_cluster(startup_time=0.0)
        
        # Scaling state
        self.last_scale_up_time = 0.0
        self.last_scale_down_time = 0.0
        
        # Metrics tracking
        self.state_history: List[WarehouseState] = []
    
    def _add_cluster(self, startup_time: float) -> ClusterState:
        """Add a new cluster to the warehouse."""
        cluster = ClusterState(
            cluster_id=self.cluster_counter,
            startup_time=startup_time
        )
        self.clusters.append(cluster)
        self.cluster_counter += 1
        return cluster
    
    def _remove_idle_clusters(self, current_time: float):
        """Remove clusters that have been idle too long, but keep min_clusters."""
        active_clusters = []
        inactive_clusters = []
        
        for cluster in self.clusters:
            if cluster.is_active(current_time, self.config.warehouse.idle_shutdown_seconds):
                active_clusters.append(cluster)
            else:
                inactive_clusters.append(cluster)
        
        # Always keep at least min_clusters, even if idle
        clusters_to_keep = active_clusters
        if len(active_clusters) < self.config.warehouse.min_clusters:
            needed = self.config.warehouse.min_clusters - len(active_clusters)
            clusters_to_keep.extend(inactive_clusters[:needed])
        
        self.clusters = clusters_to_keep
    
    def _should_scale_up(self, current_time: float) -> bool:
        """
        Determine if we should scale up based on current load.
        
        Scales up if:
        - No clusters exist (auto-resume from zero for Serverless)
        - Current utilization exceeds threshold
        - Haven't scaled up too recently
        """
        if len(self.clusters) >= self.config.warehouse.max_clusters:
            return False
        
        # Auto-resume: if no clusters exist, scale up immediately (Serverless behavior)
        if len(self.clusters) == 0:
            return True
        
        if current_time - self.last_scale_up_time < self.config.warehouse.scale_up_delay_seconds:
            return False
        
        # Check overall utilization
        total_capacity = len(self.clusters) * self.config.warehouse.effective_concurrency_per_cluster
        active_queries = sum(c.active_queries for c in self.clusters)
        
        if total_capacity == 0:
            return False
        
        utilization = active_queries / total_capacity
        return utilization >= self.config.warehouse.scale_up_threshold
    
    def _should_scale_down(self, current_time: float) -> bool:
        """
        Determine if we should scale down based on current load.
        
        Scales down if:
        - Current utilization is below threshold
        - Haven't scaled down too recently
        - Have more than minimum clusters
        """
        if len(self.clusters) <= self.config.warehouse.min_clusters:
            return False
        
        if current_time - self.last_scale_down_time < self.config.warehouse.scale_down_delay_seconds:
            return False
        
        # Check overall utilization
        total_capacity = len(self.clusters) * self.config.warehouse.effective_concurrency_per_cluster
        active_queries = sum(c.active_queries for c in self.clusters)
        
        if total_capacity == 0:
            return False
        
        utilization = active_queries / total_capacity
        return utilization <= self.config.warehouse.scale_down_threshold
    
    def _scale_up(self, current_time: float):
        """Add a new cluster."""
        self._add_cluster(startup_time=current_time)
        self.last_scale_up_time = current_time
    
    def _scale_down(self, current_time: float):
        """Remove an idle cluster."""
        # Find cluster with fewest active queries
        if not self.clusters:
            return
        
        least_busy_cluster = min(self.clusters, key=lambda c: c.active_queries)
        
        # Only scale down if the cluster has no active queries
        if least_busy_cluster.active_queries == 0:
            least_busy_cluster.shutdown_time = current_time
            self.last_scale_down_time = current_time
    
    def assign_query(self, current_time: float) -> Tuple[bool, ClusterState]:
        """
        Assign a query to a cluster.
        
        Returns:
            Tuple of (success, cluster) where success indicates if query was assigned
        """
        # Check for scaling needs
        if self._should_scale_up(current_time):
            self._scale_up(current_time)
        
        # Try to assign to existing cluster with capacity
        # Any cluster that's not shut down and has capacity can accept queries
        available_clusters = [
            c for c in self.clusters
            if c.active_queries < self.config.warehouse.effective_concurrency_per_cluster
            and (c.shutdown_time is None or current_time < c.shutdown_time)
        ]
        
        if available_clusters:
            # Assign to cluster with fewest active queries (load balance)
            cluster = min(available_clusters, key=lambda c: c.active_queries)
            cluster.active_queries += 1
            return True, cluster
        
        # All clusters at capacity - query will queue
        return False, None
    
    def release_query(self, cluster: ClusterState, end_time: float):
        """Release a query from a cluster."""
        if cluster and cluster in self.clusters:
            cluster.active_queries = max(0, cluster.active_queries - 1)
            cluster.last_query_end_time = end_time
    
    def update_state(self, current_time: float):
        """Update warehouse state and perform scaling decisions."""
        # Remove idle clusters
        self._remove_idle_clusters(current_time)
        
        # Check for scale down
        if self._should_scale_down(current_time):
            self._scale_down(current_time)
    
    def calculate_dbu_consumption(self, time_delta_seconds: float) -> float:
        """
        Calculate DBU consumption for the given time period.
        
        Serverless billing: only pay for active compute time.
        
        Args:
            time_delta_seconds: Time period to calculate for
        
        Returns:
            DBU consumption for this period
        """
        # Count active clusters
        active_clusters = len(self.clusters)
        
        # Calculate DBUs
        # DBUs = (DBUs per hour per cluster) * (number of clusters) * (hours)
        hours = time_delta_seconds / 3600.0
        dbus = self.config.warehouse.dbus_per_hour * active_clusters * hours
        
        return dbus
    
    def get_state(self, current_time: float, queued_queries: int = 0) -> WarehouseState:
        """
        Get current warehouse state.
        
        Args:
            current_time: Current simulation time
            queued_queries: Number of queries waiting for capacity
        
        Returns:
            WarehouseState object
        """
        active_queries = sum(c.active_queries for c in self.clusters)
        total_capacity = len(self.clusters) * self.config.warehouse.effective_concurrency_per_cluster
        
        return WarehouseState(
            time=current_time,
            num_clusters=len(self.clusters),
            active_queries=active_queries,
            queued_queries=queued_queries,
            total_capacity=total_capacity
        )
    
    def record_state(self, current_time: float, queued_queries: int = 0, 
                     dbu_consumption: float = 0.0, genai_dbu_consumption: float = 0.0):
        """Record current state for metrics."""
        state = self.get_state(current_time, queued_queries)
        state.dbu_consumption = dbu_consumption
        state.genai_dbu_consumption = genai_dbu_consumption
        self.state_history.append(state)

