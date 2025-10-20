# Validation & Testing Scripts

This folder contains optional validation and testing scripts.

## Scripts

### `validate_simulation.py`
Runs a comprehensive validation suite to ensure the simulation logic works correctly:
- Always-on warehouse cost validation
- Warehouse size scaling tests
- Query volume vs cost analysis
- Auto-suspend vs always-on comparison
- Max clusters scaling effects
- DBU rate scaling validation

**Usage:**
```bash
python scripts/validate_simulation.py
```

### `validate_scale_to_zero.py`
Tests and validates the scale-to-zero (auto-suspend) behavior:
- Verifies warehouse scales to 0 clusters when idle
- Analyzes idle period distribution
- Calculates cost savings from auto-suspend
- Tests overnight scaling behavior

**Usage:**
```bash
python scripts/validate_scale_to_zero.py
```

## When to Use These

- **After code changes**: Run validation to ensure nothing broke
- **Debugging behavior**: Use these to understand how parameters affect results
- **Testing new configurations**: Verify your settings produce expected behavior

## Note

These are **optional** - you don't need to run them for normal simulation usage. 
Just use `python run_simulation.py` for regular cost analysis.

