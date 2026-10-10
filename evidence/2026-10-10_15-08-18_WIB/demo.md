# M1 Demo

Commit `280416e` with uncommitted changes. Run from 2026-10-10_15-08-18_WIB to 2026-10-10_15-12-04_WIB on windows/amd64, 12 CPUs. Docker `27.1.1 12 7899430912`.

Settings within the specification are PVMBG fixed at 3s, polling every 3s, a new source event every 2s, and a 60s access token.

## Checks

| Check | Criteria | Result | Seconds | Logic also covered by |
|---|---|---|---|---|
| R3 every service runs in its own container from one compose up | R3 | not run | 0.0 |  |
| R1 services share no code | R1 | not run | 0.0 |  |
| S1 source mocks keep their seed, event rate, delay, and since contract | S1 | not run | 0.0 | TestSeedMeetsContract, TestSinceQuery, TestDelayAppliesOnlyToAuthorizedDataRequests, TestRandomDelayStaysWithinBounds, TestRunGeneratorAddsEventsUntilCancelled |
| R4 a restarted source keeps its history and schema version | R4, S1 | not run | 0.0 | TestIntegrationStorePersistsReportsAndSchemaVersion, TestIntegrationStorePersistsSeedAndFiltersSince |
| R7 every service answers its health check | R7 | not run | 0.0 | TestQueryValidationAndHealth, TestRoutesAndCorrelation |
| P1.1 both sources map to canonical HazardEvents | P1.1 | not run | 0.0 | TestSeismicBoundariesAndWarnings, TestVolcanicAdditiveValuesAndStableIdentity, TestRequiredFieldsRejectMissingNullAndWrongType |
| P1.2-P1.4 schema change applies at runtime and stays valid | P1.2, P1.3, P1.4 | not run | 0.0 | TestSchemaChangeAtRuntime, TestIntegrationLivePollingMappingFreshnessAndReads |
| P2.1-P2.2 BMKG reads stay fast under sustained load | P2.1, P2.2 | ✅ pass | 106.3 | TestIndependentPollingDuringSlowSourceAndOutage, TestAPIAdmissionAndDeadline |
| P2.2 overload is rejected with counted 429 responses | P2.2 | ✅ pass | 25.2 | TestAdmissionLimitDoesNotCallDependenciesAndReleasesCapacity, TestUpstream429PreservesStatusAndSafeRetryAfter |
| P2.3 outage serves stale volcanic data and fresh seismic data | P2.3, R4 | ✅ pass | 12.9 | TestOutageErrorModeAndRecovery, TestIntegrationLivePollingMappingFreshnessAndReads |
| P2.3 hanging source never makes reads hang | P2.3, R4 | ✅ pass | 15.0 | TestOutageHangModeSendsNoResponse, TestSourceBoundsRetriesAndCancellation |
| P3.1 agencies reject each other's credentials | P3.1, R5 | not run | 0.0 | TestCredentialDomains |
| P3.2 Media gets summary fields and raw requests get 403 | P3.2 | not run | 0.0 | TestMediaRawRequestForbiddenAndRawClientGetsAllFields, TestSummaryFiltersRawFieldsAndPreservesFreshnessAndCorrelation |
| P3.3 expired token refreshes without login and the old token is rejected | P3.3 | not run | 0.0 | TestNaturalExpiryThenRefreshWithoutLogin, TestLoginScopesAndRefreshSupersedesOldSessionVersion, TestIntegrationConcurrentRefreshRevocationAndExpiry |
| P3.4 no credentials are committed and .env.example lists every variable | P3.4, R6 | not run | 0.0 |  |
| P4.2 old and new attribute shapes coexist without migration | P4.2 | not run | 0.0 | TestIntegrationStoreCommitRollbackRestartAndOwnership |
| P4.3 only the Aggregator reaches the Canonical Store and every store has one owner | P4.3, R2 | not run | 0.0 | TestIntegrationStoreCommitRollbackRestartAndOwnership |
| P5.1-P5.2 events fan out to independent consumers | P5.1, P5.2 | not run | 0.0 | TestIntegrationRabbitFanoutConfirmAndReconnect, TestIntegrationIndependentQueuesAndRestartCatchup |
| P5.3 a stopped consumer blocks nothing and catches up | P5.3, R4 | not run | 0.0 | TestIntegrationIndependentQueuesAndRestartCatchup, TestIntegrationRedeliveryAfterPersistBeforeAck |
| P5.4 a new consumer joins without producer changes | P5.4 | not run | 0.0 |  |
| R7 correlation IDs cross every service in a flow | R7 | not run | 0.0 | TestAPIRouteMethodsAndCorrelation, TestSourceCallLogsDependencyAndSafeFailure |
| R7 every outbound call logs its latency | R7 | not run | 0.0 | TestSourceCallLogsDependencyAndSafeFailure |
| P4.1 one service rebuilds while the others keep serving | P4.1, R4 | not run | 0.0 |  |
| P2.3 failed source without stored data is explicitly unavailable | P2.3 | ✅ pass | 15.7 | TestAPIAuthenticationQueriesAndFreshness |

## Specification Coverage

| Criterion | Requirement | Result |
|---|---|---|
| S1 | Mocks seed 20+ records, add 1+ per 10s, keep their delays, filter by since, and keep data across restarts | not run |
| P1.1 | Both sources map to HazardEvent by the mapping rules | not run |
| P1.2 | PVMBG schema change runs without restarting any service | not run |
| P1.3 | HazardEvents stay valid after the schema change | not run |
| P1.4 | Unknown fields follow the declared attributes policy | not run |
| P1.5 | Schema evolution mechanism and supported changes | report section |
| P2.1 | BMKG-only p95 below 300 ms while PVMBG takes 3 s | ✅ pass |
| P2.2 | 50+ connections for 60+ s with full metrics, errors below 1%, 429 counted | ✅ pass |
| P2.3 | Outage serves stale data or explicit unavailability and recovers without restart | ✅ pass |
| P2.4 | Concurrency model and load test configuration | report section |
| P3.1 | Each agency rejects the other agency's credentials with 401 or 403 | not run |
| P3.2 | Media gets summary fields and explicit raw requests get 401 or 403 | not run |
| P3.3 | Expired Field Team token refreshes without login and the old token is rejected | not run |
| P3.4 | No hard-coded or committed credentials and .env.example exists | not run |
| P3.5 | Authentication mechanism and why it opens no new gap | report section |
| P4.1 | One service rebuilds and restarts while independent requests keep working | not run |
| P4.2 | Old and new attribute shapes stored side by side without migration | not run |
| P4.3 | Only the Aggregator accesses the Canonical Store | not run |
| P4.4 | Service boundaries and two storage alternatives compared | report section |
| P5.1 | New HazardEvents are published to a broker | not run |
| P5.2 | Two or more independent consumers receive the same stream | not run |
| P5.3 | A stopped consumer blocks neither producer nor others and catches up | not run |
| P5.4 | A new consumer joins without producer code changes | not run |
| P5.5 | Broker, delivery semantics, and idempotency | report section |
| R1 | Services talk only over the network and share no business logic | not run |
| R2 | Each storage has exactly one owning service | not run |
| R3 | Each service has its own container and one command starts all | not run |
| R4 | Services start and stop independently under real failures | not run |
| R5 | Credentials are valid in one agency only | not run |
| R6 | Configuration and secrets come from the environment | not run |
| R7 | Health checks, correlated structured logs, and outbound latency | not run |

Evidence for each check is in `demo.json`.
