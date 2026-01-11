"""Trade-in orphans (trade-ins that didn't form pricing_groups).

This module keeps a separate collection for trade-ins that failed grouping
(`pricing_bad_tradein_skus` with grouping-related reason codes).

They can still be priced via the existing pricing algorithm, but require a
manual sell anchor.
"""
