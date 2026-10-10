# Transactional outbox verification

This run verifies the Aggregator outbox change on a working tree based on `c08e3130ebf4e3bce8954bf6d55aaa5d56478ec2`. The generated demo metadata reports that base commit with uncommitted changes. `source-sha256.txt` records the exact contents of all twelve changed or added implementation, test, and documentation files in the outbox PR. Unchanged source and test configuration come from the base commit.

## Results

| Command | Result | Evidence |
|---|---|---|
| `make ci` | Lint, race tests, builds, dependency verification, and vulnerability checks passed across all eight modules | `ci.log` |
| `make integration` | All integration suites passed using real PostgreSQL and RabbitMQ | `integration.log` |
| Aggregator `golangci-lint run ./...` | Zero issues | `aggregator-lint.log` |
| Aggregator `go test -race -count=1 ./...` | Passed | `aggregator-race.log` |
| `DEMO_PROJECT=atlas-outbox-20261010 make demo LOAD=1` | 24/24 checks passed, no skips | `demo.log`, `../demo.md`, `../demo.json`, `../P2/` |

The final worker-restart unit regression was added after the full CI and integration runs, then verified with the Aggregator lint and race commands above. Production code remained identical throughout those checks and the full demo. Test stacks were removed after their runs. Captured logs omit trailing whitespace.

Outbox-specific coverage includes:

- A child process exits with code 77 immediately after the canonical/checkpoint/outbox transaction commits. A restarted store recovers the original snapshot and correlation ID.
- Publication confirmed before deletion can be retried with the same message identity, allowing consumer deduplication.
- An older pending revision retains its original payload after the latest canonical revision changes.
- A failed transaction rolls back canonical and outbox writes together.
- An empty exchange returns the mandatory publication. The pending row is delivered and removed after a subscriber binds.
- Three failed broker attempts retain SQL work; a recovered relay delivers it without RAM hints.
- A full hint channel, an exhausted worker, and a failed outbox deletion retain pending work.

## Load measurements

The sustained run used 60 VUs for 90 seconds with PVMBG responses fixed at 3 seconds. BMKG p95 was 54.1 ms, with 0% unexpected errors and no 429 responses. The burst used 400 VUs for 20 seconds and counted 226,769 controlled 429 responses with 0% unexpected errors. Raw metrics and connection samples are in `../P2/`.

These results verify the catalogued runtime behaviors and the outbox recovery cases. The design still permits duplicate delivery after confirmation, disk growth while publication is blocked, and subscriber delivery bounded by the existing queue TTL/capacity. The relay matches the deployment's single Aggregator instance; previously lost revisions are not reconstructed.

To compare the recorded source with a checkout of the PR, run this from the repository root:

```sh
sha256sum -c evidence/2026-10-10_22-26-43_WIB/verification/source-sha256.txt
```
