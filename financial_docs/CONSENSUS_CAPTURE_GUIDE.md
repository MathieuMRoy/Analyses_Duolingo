# Consensus Capture Guide

This file defines the minimum label set needed to move from explainable proxy signals
to supervised quarterly nowcasting.

## Priority 1

Fill these fields first for each quarter:

- `quarter`
- `earnings_release_date`
- `actual_revenue_musd`
- `consensus_revenue_musd`
- `revenue_surprise_pct`
- `actual_eps`
- `consensus_eps`
- `eps_surprise_pct`
- `source_actuals`
- `source_consensus`

These are enough to start a first `revenue beat / miss` and `EPS beat / miss` backtest.

## Priority 2

Add these when available:

- `actual_adjusted_ebitda_musd`
- `consensus_adjusted_ebitda_musd`
- `actual_paid_subscribers_m`
- `consensus_paid_subscribers_m`
- `actual_subscription_revenue_musd`
- `consensus_subscription_revenue_musd`

These improve the future EBITDA and monetization nowcast.

## Priority 3

Add guidance fields when possible:

- `guidance_next_q_revenue_musd`
- `guidance_fy_revenue_musd`
- `guidance_signal`

Recommended `guidance_signal` values:

- `raise`
- `maintain`
- `cut`

## Source rules

- `source_actuals`: 10-Q, 10-K, shareholder letter, or earnings release
- `source_consensus`: Nasdaq, Zacks, school terminal export, or other source
- Keep the source URL or document name in `notes` when helpful

## Important modeling note

Until this file is populated, the project should treat:

- `revenue_beat_probability`
- `ebitda_beat_probability`
- `guidance_raise_probability`

as unavailable or proxy-only, never as fully supervised model outputs.
