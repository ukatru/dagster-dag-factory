from dagster import __version__ as dagster_version
import packaging.version

# 1. Handle FreshnessPolicy rename
try:
    from dagster import LegacyFreshnessPolicy as FreshnessPolicy
    HAS_LEGACY_FP = True
except ImportError:
    try:
        from dagster import FreshnessPolicy
        HAS_LEGACY_FP = False
    except ImportError:
        FreshnessPolicy = None
        HAS_LEGACY_FP = False

# 2. Determine correct keyword for @asset/@asset_out
# Dagster 1.11.x uses 'legacy_freshness_policy' for LegacyFreshnessPolicy
# in both @asset and AssetOut.
v = packaging.version.parse(dagster_version)
if v.major == 1 and v.minor == 11 and HAS_LEGACY_FP:
    FRESHNESS_POLICY_KEY = "legacy_freshness_policy"
else:
    # 1.10 and earlier, or 1.12+ (where they might have reconciled)
    FRESHNESS_POLICY_KEY = "freshness_policy"

__all__ = ["FreshnessPolicy", "FRESHNESS_POLICY_KEY"]
