# Back Market Pricer Backend (FastAPI)

This service provides the API used by the “BM Pricer” frontend.

Core responsibilities:
- Store **users** and per-user Back Market credentials / settings
- Sync sell listings + trade-in listings from Back Market
- Build **pricing groups** (the backbone for pricing)
- Compute **sell anchors**, **trade pricing scenarios**, and final offer prices
- Apply trade-in offers back to Back Market (dry-run supported)
- Manage **Trade-in Orphans** (trade-in listings that could not be grouped)

## Requirements

- Python 3.11+ recommended
- MongoDB

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

The backend reads environment variables (and loads a local `.env` file automatically).

Common settings:

| Variable | Purpose | Example |
|---|---|---|
| `MONGO_URI` | Mongo connection string | `mongodb://localhost:27017` |
| `MONGO_DB` | Database name | `test_pricer` |
| `BM_API_BASE_URL` | Back Market base URL | `https://www.backmarket.co.uk` |
| `BM_TIMEOUT_SECONDS` | HTTP timeout for BM calls | `15` |
| `PROXY_URL` | Optional global proxy URL | `http://user:pass@host:port` |

See `app/core/config.py` for the full list.

## Run locally

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Then open:
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

## Relevant API Areas

### Users

Routes under `/users`:
- Create/list/update users
- Initialize a user with BM credentials (`POST /users/init`)
- Read/update user settings

### Pricing Groups

Routes under `/pricing-groups`:
- `POST /pricing-groups/build/{user_id}` builds groups from stored BM sell + trade-in listings
- `GET /pricing-groups/user/{user_id}` lists groups

### Sell pricing / anchors

Routes under `/bm/sell`:
- Sell listing sync
- Sell pricing cycle
- Manual sell anchor overrides for groups

### Trade-in pricing

Routes under `/tradein` and `/bm/pricing/trade`:
- Competitor refresh
- Trade pricing recompute
- Offer application

### Trade-in Orphans

Routes under `/tradein/orphans` handle trade-in listings that could not be grouped into pricing groups.

Common workflow:
1. `POST /tradein/orphans/sync/{user_id}` to materialize orphans from issue records
2. Per orphan:
   - set `manual_trade_sku` if the SKU parse failed
   - set `manual_sell_anchor_gross` (required when there are no sell children)
   - enable the orphan
3. `POST /tradein/orphans/competitors/{user_id}/refresh` to fetch competitor data
4. `POST /tradein/orphans/recompute/{user_id}` to compute trade pricing
5. `POST /tradein/orphans/offers/{user_id}/apply` (dry-run first, then live)

Important:
- The frontend expects these endpoints to exist under the **exact prefix** `/tradein/orphans`.
- Ensure `app.features.backmarket.tradein_orphans.router` is included in `app/main.py`.

## Notes on typing

Some Mongo documents are deliberately schemaless (BM payload snapshots, pricing group blobs, etc.).
For those endpoints, the API returns JSON objects/dicts rather than strict Pydantic response models.

If you want to enforce “no Any” across the Python codebase, the recommended approach is:
- Introduce `TypedDict` models for the most common document shapes
- Use narrower `dict[str, object]` and helper validators instead of `typing.Any`
- Add Pydantic response models only where the schema is truly stable
