# Depreciation API (seed curve + multipliers)

## Collections

### depreciation_models
Per (user_id, market, brand, model, storage_gb):
- release_date
- msrp_amount
- currency
- segment (optional override)

### depreciation_multipliers
Per (user_id, market, key):
- scope
- log_m
- n
- updated_at

## Integrate into app

1) Include router (wherever you register routers):

```py
from app.features.depreciation.depreciation_api import router as depreciation_router
app.include_router(depreciation_router)
```

2) Add indexes in `app/db/mongo.py`:

```py
await _ensure_index(
    db["depreciation_models"],
    [("user_id", 1), ("market", 1), ("brand", 1), ("model", 1), ("storage_gb", 1)],
    unique=True,
    name="uniq_depr_models_user_market_device",
)
await _ensure_index(
    db["depreciation_models"],
    [("user_id", 1), ("market", 1), ("brand", 1), ("model", 1)],
    unique=False,
    name="idx_depr_models_user_market_brand_model",
)
await _ensure_index(
    db["depreciation_multipliers"],
    [("user_id", 1), ("market", 1), ("key", 1)],
    unique=True,
    name="uniq_depr_mult_user_market_key",
)
await _ensure_index(
    db["depreciation_multipliers"],
    [("user_id", 1), ("market", 1), ("scope", 1)],
    unique=False,
    name="idx_depr_mult_user_market_scope",
)
```

3) Seed model catalog via Swagger:
- `PUT /depreciation/models`

4) Compute depreciation into a pricing group doc:
- `POST /depreciation/pricing-groups/{pricing_group_id}/compute`
  - Writes `pricing_groups.depreciation_anchor`

5) Calibrate multipliers:
- `POST /depreciation/observe` (manual)
- `POST /depreciation/observe/from-orderline/{orderline_id}` (from bm_orderlines)
