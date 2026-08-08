# Changelog

## v0.2.0

### Breaking changes

- Require Peven 0.6.
- Require worker protocol 2 and reject protocol 1 handshakes.

### Changes

- Decode nonnegative transition retry counts, defaulting omitted values to zero.
- Bound executor calls with a configurable 900-second default timeout.
- Resolve Python executors within their gateway instead of Peven's global
  registry.
- Harden malformed, stale, disconnected, and unroutable peer handling.
- Reject duplicate place and transition ids in decoded nets.
- Replace oversized terminal fire error text with a correlated bounded fallback.

## v0.1.0

- Initial Julia transport bridge release.
