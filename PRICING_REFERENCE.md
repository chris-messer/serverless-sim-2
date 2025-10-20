# Databricks Serverless SQL Warehouse Pricing Reference

**Source:** [Databricks Instance Types Pricing](https://www.databricks.com/product/pricing/product-pricing/instance-types)

**Last Updated:** Based on AWS Enterprise pricing as of October 2024

## Serverless SQL Warehouse Sizes (AWS Enterprise)

| Size | DBUs/hour/cluster | Cost @ $0.70/DBU | Notes |
|------|-------------------|------------------|-------|
| **2X-Small** | 4.0 | $2.80/hour | Minimum size, testing/dev |
| **X-Small** | 6.0 | $4.20/hour | Light workloads |
| **Small** | 12.0 | $8.40/hour | Small production |
| **Medium** | 24.0 | $16.80/hour | **Default - Standard workloads** |
| **Large** | 40.0 | $28.00/hour | Heavy workloads |
| **X-Large** | 80.0 | $56.00/hour | Very large datasets |
| **2X-Large** | 160.0 | $112.00/hour | Enterprise scale |
| **3X-Large** | 320.0 | $224.00/hour | Massive scale |
| **4X-Large** | 640.0 | $448.00/hour | Maximum throughput |

## Important Notes

### DBU Rates by Cloud Provider

The simulator uses **$0.70/DBU** as the default, which applies to:
- ✅ **AWS** (US East - N. Virginia): $0.70/DBU
- ✅ **Azure** (US East): $0.70/DBU  
- ⚠️ **GCP** (Preview): $0.88/DBU

**To adjust for your region/provider:** Edit the `dbu_rate` parameter in your simulation config.

### Pricing Model

**Databricks Serverless SQL Warehouse includes:**
- ✅ Compute costs (bundled into DBU rate)
- ✅ Infrastructure costs (bundled)
- ✅ Management overhead (bundled)

**You pay ONLY for active compute time:**
- No charges during idle periods
- Automatic scale-down after inactivity
- Billed per second of usage

### Cost Calculation

```
Hourly Cost = (Active Clusters) × (DBUs/hour/cluster) × ($/DBU)
```

**Example:**
- Medium warehouse (24 DBUs/hour)
- Running for 1 hour continuously
- Cost = 1 cluster × 24 DBUs × $0.70 = **$16.80/hour**

### Warehouse Selection Guide

**2X-Small / X-Small:**
- Development and testing
- Single user queries
- Low concurrency (1-2 queries)

**Small:**
- Small teams (< 10 users)
- Light production workloads
- Low to moderate concurrency

**Medium (Default):**
- Standard teams (10-50 users)
- Typical dashboards and BI
- Moderate concurrency (10-20 queries)
- **Most common starting point**

**Large:**
- Larger teams (50-100 users)
- Heavy dashboards
- High concurrency (20-40 queries)

**X-Large and above:**
- Enterprise deployments (100+ users)
- Mission-critical workloads
- Very high concurrency (40+ queries)
- Large-scale data processing

## Autoscaling Impact

The simulator models **intelligent autoscaling** where:
- Additional clusters spin up when load increases
- Clusters shut down when idle
- You pay for total active clusters

**Example:**
- Medium warehouse with 3 active clusters
- Cost = 3 × 24 DBUs × $0.70 = **$50.40/hour**

## Regional Variations

DBU rates may vary by:
- ☁️ Cloud provider (AWS, Azure, GCP)
- 🌍 Geographic region
- 📋 Enterprise agreement terms
- 🎫 Subscription plan (Standard, Premium, Enterprise)

**Always check your specific pricing** in the Databricks console or with your account team.

## Updating the Simulator

To adjust for your specific pricing:

```python
from src import create_custom_config

config = create_custom_config(
    warehouse_size="Large",      # Your size
    dbu_rate=0.65,              # Your contract rate
    simulation_days=30
)
```

Or for custom DBU mapping (non-standard sizes):

```python
config = create_default_config()
config.warehouse.size_dbu_mapping["Custom"] = 50.0  # 50 DBUs/hour
config.warehouse.size = "Custom"
```

## References

- **Official Pricing:** https://www.databricks.com/product/pricing/product-pricing/instance-types
- **SQL Serverless Docs:** https://docs.databricks.com/sql/admin/serverless.html
- **DBU Pricing:** Contact your Databricks account team for contract-specific rates

---

**Note:** This simulator uses the values in `src/config.py`. The DBU mapping is based on actual Databricks Serverless SQL Warehouse specifications for AWS Enterprise as of October 2024.

