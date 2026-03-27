# A-share Harness V2

`v2` is a harness-first A-share research, monitoring, replay, and evaluation system.

Core principles:

- data reliability first
- structured decisions only
- replay and evaluation before polish
- one clear user entry

New in the current build:

- `ask-stock`: answer whether a stock is buyable with evidence, coverage score, and better alternatives
- `discover-ideas`: scan the tracked and cached universe to surface top candidates
- live supplemental bridge: auto-enrichs top ideas and queried stocks with financials, valuation context, capital flow, company profile, and industry heat
- `GET /api/advice`: API entry for the advice harness
- `GET /api/discovery`: API entry for candidate discovery

Primary user guide lives in `STARTUP_README.md`.
