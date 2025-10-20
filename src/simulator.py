"""
Main simulation engine for Databricks Serverless SQL Warehouse cost modeling.
"""

import numpy as np
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
import logging
from collections import defaultdict

from .config import SimulationConfig
from .events import EventGenerator, Query
from .warehouse import ServerlessWarehouse, WarehouseState


@dataclass
class QueryExecution:
    """Tracks a query's execution through the system."""
    
    query: Query
    assigned_time: float  # When query was assigned to a cluster
    completed_time: float = None  # When query completed
    cluster_id: int = None  # Which cluster executed it
    
    @property
    def wait_time(self) -> float:
        """Time spent waiting before execution."""
        return self.assigned_time - self.query.start_time
    
    @property
    def total_time(self) -> float:
        """Total time from submission to completion."""
        if self.completed_time is None:
            return 0.0
        return self.completed_time - self.query.start_time


@dataclass
class SimulationMetrics:
    """Aggregated metrics from simulation run."""
    
    # Cost metrics
    total_dbus: float = 0.0
    sql_dbus: float = 0.0
    genai_dbus: float = 0.0
    total_cost: float = 0.0
    sql_cost: float = 0.0
    genai_cost: float = 0.0
    
    # Query metrics
    total_queries: int = 0
    dashboard_queries: int = 0
    genie_queries: int = 0
    genai_queries: int = 0
    
    # Performance metrics
    avg_wait_time: float = 0.0
    p50_wait_time: float = 0.0
    p95_wait_time: float = 0.0
    p99_wait_time: float = 0.0
    max_wait_time: float = 0.0
    
    # Wait times by query type
    genie_avg_wait_time: float = 0.0
    genie_p50_wait_time: float = 0.0
    genie_p95_wait_time: float = 0.0
    genie_p99_wait_time: float = 0.0
    
    dashboard_avg_wait_time: float = 0.0
    dashboard_p95_wait_time: float = 0.0
    
    # Warehouse metrics
    avg_clusters: float = 0.0
    max_clusters: int = 0
    avg_utilization: float = 0.0
    max_queue_depth: int = 0
    
    # Time series data
    state_history: List[WarehouseState] = field(default_factory=list)
    wait_times: List[float] = field(default_factory=list)
    genie_wait_times: List[float] = field(default_factory=list)
    dashboard_wait_times: List[float] = field(default_factory=list)


class Simulator:
    """
    Main simulation engine for Databricks Serverless SQL Warehouse.
    """
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.warehouse = ServerlessWarehouse(config)
        self.event_generator = EventGenerator(config)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Tracking structures
        self.query_executions: List[QueryExecution] = []
        self.active_queries: Dict[int, Tuple[Query, object, float]] = {}  # query_id -> (query, cluster, start)
        self.query_queue: List[Query] = []
        
        # Metrics
        self.total_dbus = 0.0
        self.genai_dbus = 0.0
    
    def run(self) -> SimulationMetrics:
        """
        Run the simulation.
        
        Returns:
            SimulationMetrics with all results
        """
        self.logger.info(f"Starting simulation for {self.config.simulation_days} days")
        self.logger.info(f"Warehouse size: {self.config.warehouse.size} "
                        f"({self.config.warehouse.dbus_per_hour} DBUs/hour/cluster)")
        
        # Show performance multiplier
        performance_multiplier = self.event_generator._performance_multiplier
        if performance_multiplier < 1.0:
            self.logger.info(f"Performance: {(1/performance_multiplier):.2f}x faster than Medium warehouse")
        elif performance_multiplier > 1.0:
            self.logger.info(f"Performance: {performance_multiplier:.2f}x slower than Medium warehouse")
        else:
            self.logger.info(f"Performance: Baseline (Medium warehouse)")
        
        # Generate all queries
        self.logger.info("Generating queries...")
        dashboard_queries, genie_queries = self.event_generator.generate_all_queries()
        all_queries = self.event_generator.merge_queries(dashboard_queries, genie_queries)
        
        self.logger.info(f"Generated {len(dashboard_queries)} dashboard queries")
        self.logger.info(f"Generated {len(genie_queries)} Genie queries")
        self.logger.info(f"Total queries: {len(all_queries)}")
        
        # Run discrete event simulation
        self.logger.info("Running simulation...")
        query_index = 0
        current_time = 0.0
        last_state_record = 0.0
        state_record_interval = 60.0  # Record state every minute
        
        # Continue until all queries processed and no active queries
        max_time = self.config.total_seconds + 3600  # Add 1 hour buffer for completion
        
        while (query_index < len(all_queries) or self.active_queries or self.query_queue) and current_time < max_time:
            # Process new query arrivals at current time
            while query_index < len(all_queries):
                query = all_queries[query_index]
                if query.start_time > current_time:
                    break
                
                # Try to assign query to warehouse
                success, cluster = self.warehouse.assign_query(current_time)
                
                if success:
                    # Query assigned immediately
                    self.active_queries[query.query_id] = (query, cluster, current_time)
                    self.query_executions.append(QueryExecution(
                        query=query,
                        assigned_time=current_time,
                        cluster_id=cluster.cluster_id if cluster else None
                    ))
                else:
                    # Query must wait in queue
                    self.query_queue.append(query)
                
                query_index += 1
            
            # Process query completions
            completed_queries = []
            for query_id, (query, cluster, start) in list(self.active_queries.items()):
                if current_time >= start + query.duration:
                    # Query completed
                    self.warehouse.release_query(cluster, current_time)
                    completed_queries.append(query_id)
                    
                    # Update execution record
                    for exec_record in self.query_executions:
                        if exec_record.query.query_id == query_id:
                            exec_record.completed_time = current_time
                            break
                    
                    # Track GenAI DBU usage
                    if query.uses_genai:
                        self.genai_dbus += self.config.genie.genai_dbu_per_call
            
            for query_id in completed_queries:
                del self.active_queries[query_id]
            
            # Process query queue - try to assign waiting queries
            remaining_queue = []
            for query in self.query_queue:
                success, cluster = self.warehouse.assign_query(current_time)
                if success:
                    self.active_queries[query.query_id] = (query, cluster, current_time)
                    self.query_executions.append(QueryExecution(
                        query=query,
                        assigned_time=current_time,
                        cluster_id=cluster.cluster_id if cluster else None
                    ))
                else:
                    remaining_queue.append(query)
            
            self.query_queue = remaining_queue
            
            # Update warehouse state and scaling
            self.warehouse.update_state(current_time)
            
            # Calculate DBU consumption for this time step
            if current_time > 0:
                time_delta = self.config.time_step_seconds
                dbus = self.warehouse.calculate_dbu_consumption(time_delta)
                self.total_dbus += dbus
            
            # Record state periodically
            if current_time - last_state_record >= state_record_interval:
                self.warehouse.record_state(
                    current_time,
                    queued_queries=len(self.query_queue),
                    dbu_consumption=self.total_dbus,
                    genai_dbu_consumption=self.genai_dbus
                )
                last_state_record = current_time
            
            # Advance time
            current_time += self.config.time_step_seconds
            
            # Progress logging
            if int(current_time) % 86400 == 0:  # Every day
                day = int(current_time / 86400)
                self.logger.info(f"Completed day {day}/{self.config.simulation_days}")
        
        self.logger.info("Simulation complete, calculating metrics...")
        
        # Calculate final metrics
        metrics = self._calculate_metrics()
        
        return metrics
    
    def _calculate_metrics(self) -> SimulationMetrics:
        """Calculate aggregated metrics from simulation results."""
        metrics = SimulationMetrics()
        
        # Cost metrics
        metrics.total_dbus = self.total_dbus
        metrics.sql_dbus = self.total_dbus
        metrics.genai_dbus = self.genai_dbus
        
        metrics.sql_cost = self.total_dbus * self.config.pricing.sql_serverless_dbu_rate
        metrics.genai_cost = self.genai_dbus * self.config.pricing.serverless_realtime_inference_dbu_rate
        metrics.total_cost = metrics.sql_cost + metrics.genai_cost
        
        # Query counts
        metrics.total_queries = len(self.query_executions)
        metrics.dashboard_queries = sum(1 for e in self.query_executions if e.query.query_type == "dashboard")
        metrics.genie_queries = sum(1 for e in self.query_executions if e.query.query_type == "genie")
        metrics.genai_queries = sum(1 for e in self.query_executions if e.query.uses_genai)
        
        # Wait time metrics
        wait_times = [e.wait_time for e in self.query_executions if e.completed_time is not None]
        genie_wait_times = [e.wait_time for e in self.query_executions 
                           if e.query.query_type == "genie" and e.completed_time is not None]
        dashboard_wait_times = [e.wait_time for e in self.query_executions 
                               if e.query.query_type == "dashboard" and e.completed_time is not None]
        
        if wait_times:
            metrics.avg_wait_time = np.mean(wait_times)
            metrics.p50_wait_time = np.percentile(wait_times, 50)
            metrics.p95_wait_time = np.percentile(wait_times, 95)
            metrics.p99_wait_time = np.percentile(wait_times, 99)
            metrics.max_wait_time = np.max(wait_times)
            metrics.wait_times = wait_times
        
        if genie_wait_times:
            metrics.genie_avg_wait_time = np.mean(genie_wait_times)
            metrics.genie_p50_wait_time = np.percentile(genie_wait_times, 50)
            metrics.genie_p95_wait_time = np.percentile(genie_wait_times, 95)
            metrics.genie_p99_wait_time = np.percentile(genie_wait_times, 99)
            metrics.genie_wait_times = genie_wait_times
        
        if dashboard_wait_times:
            metrics.dashboard_avg_wait_time = np.mean(dashboard_wait_times)
            metrics.dashboard_p95_wait_time = np.percentile(dashboard_wait_times, 95)
            metrics.dashboard_wait_times = dashboard_wait_times
        
        # Warehouse metrics
        state_history = self.warehouse.state_history
        if state_history:
            metrics.avg_clusters = np.mean([s.num_clusters for s in state_history])
            metrics.max_clusters = max(s.num_clusters for s in state_history)
            
            utilizations = [s.utilization() for s in state_history if s.total_capacity > 0]
            if utilizations:
                metrics.avg_utilization = np.mean(utilizations)
            
            metrics.max_queue_depth = max(s.queued_queries for s in state_history)
            metrics.state_history = state_history
        
        return metrics


def run_simulation(config: SimulationConfig = None) -> SimulationMetrics:
    """
    Convenience function to run a simulation.
    
    Args:
        config: Simulation configuration (uses default if None)
    
    Returns:
        SimulationMetrics with all results
    """
    if config is None:
        from .config import create_default_config
        config = create_default_config()
    
    simulator = Simulator(config)
    return simulator.run()

