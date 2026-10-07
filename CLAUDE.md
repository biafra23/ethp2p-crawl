# ethp2p-crawl — working rules

## Who types
- Claude never writes or edits files in this repo. It describes changes; the author types or pastes them.
- Code in answers is for reading, not for pasting whole. Keep snippets to the changed lines.

## Prefer IDE support
- Whenever a change maps to a RustRover refactoring (Extract Function, Rename,
  Change Signature, Move, Introduce Constant/Variable/Parameter, Inline, etc),
  name that refactoring instead of describing the edit by hand.
- Tests: suggest the `tmod` / `tfn` live templates; name what the test should assert.

## Presenting changes
- Show changed lines with +/~/- markers and right-aligned line numbers, not a diff.

## Project facts
- reth crates pinned to git tag v2.7.0; secp256k1 0.30 + rand 0.8 to match reth types.
- Sepolia only for now; mainnet and Gnosis configs exist but are not active by default.
- results.jsonl is output, never committed.
