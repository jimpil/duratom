# Change Log

## [Unreleased]
### Fixed
- `swap!` is atomic: Duratom implements glimmer's `IAtomicSwap` (glimmer
  bumped to v0.1.7), so concurrent swaps no longer lose writes.
- A stored `false` or `nil` is read back as the value instead of being replaced
  by `init`.
### Changed
- Rewritten for jolt. A duratom is now a durable atom over a pluggable
  `duratom.backends/StorageBackend` protocol, with file and sqlite/postgres
  (via jolt-lang/db) backends. It implements `glimmer.ratom/IReactiveCell`, so
  it composes with glimmer's `@`/`reset!`/`swap!`/`cursor`/`reaction`.
