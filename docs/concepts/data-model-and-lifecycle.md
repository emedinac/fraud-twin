# Data model and lifecycle

**Level:** Beginner<br><br>
**You will:** understand the main records in a run and how a payment moves
through its lifecycle.<br><br>
**Before you start:** the [Quickstart](../quickstart.md).<br><br>
**Services:** None.

FraudTwin creates a small payment world with stable identities and a clear
timeline. That makes it possible to follow one payment from the first event
to the final financial result.

## Stable identities and reproducible runs

A configuration and seed produce the same logical identities and output
fingerprints. Customers, accounts, cards, merchants, payments, and graph
relationships can therefore be compared across runs.

FraudTwin uses named random streams for different parts of the simulation.
Changing one part of a configuration should not silently replace every other
identity. Worker scheduling can change the order in which work is completed,
but it should not change the logical result.

## A payment has a lifecycle

A payment is a business identity. Its lifecycle is described by events such
as authorization, capture, clearing, settlement, reversal, refund, return,
and chargeback. These events explain when something happened and when it
became visible.

A lifecycle event is not the same as a ledger entry. The ledger records the
financial movement and keeps the accounting postings balanced. Keeping these
ideas separate makes it easier to investigate both operational behavior and
financial outcomes.

## What to inspect first

When you open a generated run, start with the manifest. It records the
configuration, seed, schema versions, counts, and fingerprints. Then follow a
payment through its events and inspect the matching ledger entries.

This order gives you the business story first and the accounting detail
second.

## Next

Continue with [Point-in-time data and labels](point-in-time-data-and-labels.md)
when you want to build features or evaluate fraud detection.

## Related

- [Data contracts](../data-contracts.rst)
- [Configuration](../configuration.md)
- [Glossary](../glossary.md)
