# Quick Fix: Extend /store/products response + block costPrice
Generated: 2026-03-25

## Change Made
- File: `backend/services/api-gateway/src/store/store.controller.ts`
- Lines: 36-78 (`mapVariants` function)
- Change: Destructure `costPrice` out and discard it before spreading product fields; updated all internal references from `product` to `safeProduct`

## What Changed

Before, `mapVariants` spread `...product` directly — `costPrice` would leak if the product service included it.

After:
```ts
const { costPrice: _costPrice, ...safeProduct } = product;
```
- `costPrice` is stripped for ALL code paths (with variants and without)
- `sku`, `isPhysicalProduct`, `metaTitle`, `metaDescription`, `collections[]` pass through automatically via `...safeProduct` spread — no additional mapping needed
- `getProductAvailability` endpoint already uses an explicit field whitelist; not affected

## Affected Endpoints
All three product-returning endpoints share `mapVariants`:
- `GET /store/products` (list)
- `GET /store/products/search`
- `GET /store/products/:id`

## Verification
- Pattern followed: existing spread-based passthrough, minimal change
- Security: costPrice excluded in all paths
- New fields (sku, isPhysicalProduct, metaTitle, metaDescription, collections): pass through via `...safeProduct`
- No deploy requested

## Files Modified
1. `backend/services/api-gateway/src/store/store.controller.ts` - strip costPrice in mapVariants, use safeProduct throughout

## Notes
No DTO or mapper file exists that whiteLists product fields — the controller uses a spread. The destructure approach is the correct minimal fix here.
