# ARCHITECTURE_BASELINE

This repo is a standalone sibling project built alongside older repos until it stands on its own.

Build this repo as an object-ledger system.

Every meaningful entity must have:
1. canonical object
2. current state
3. append-only events
4. derived summaries
5. render shell

Shell types:
- static
- dynamic
- calculator
- share

Hard rules:
- one object, one canonical URL
- page is the payload
- summaries reveal patterns, not inventory
- front = action
- back = evidence
- no duplicate identities
- no UI-only critical truth
- no page-first implementations for persistent entities
- no legacy schema assumptions unless explicitly mapped

Current priority order:
1. World Cup group object
2. World Cup match object
3. card URL plus object page
4. licensing parent-child object model
