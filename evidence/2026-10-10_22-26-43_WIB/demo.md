# M1 Demo

Commit `c08e313` with uncommitted changes. Run from 2026-10-10_22-26-43_WIB to 2026-10-10_22-33-12_WIB on linux/amd64, 16 CPUs. Docker `29.7.2 16 41376174080`.

Settings within the specification are PVMBG fixed at 3s, polling every 3s, a new source event every 2s, and a 60s access token.

## Checks

| Check | Criteria | Result | Seconds | Logic also covered by |
|---|---|---|---|---|
| R3 every service runs in its own container from one compose up | R3 | ✅ pass | 0.1 |  |
| R1 services share no code | R1 | ✅ pass | 0.4 |  |
| S1 source mocks keep their seed, event rate, delay, and since contract | S1 | ✅ pass | 11.2 | TestSeedMeetsContract, TestSinceQuery, TestDelayAppliesOnlyToAuthorizedDataRequests, TestRandomDelayStaysWithinBounds, TestRunGeneratorAddsEventsUntilCancelled |
| R4 a restarted source keeps its history and schema version | R4, S1 | ✅ pass | 16.3 | TestIntegrationStorePersistsReportsAndSchemaVersion, TestIntegrationStorePersistsSeedAndFiltersSince |
| R7 every service answers its health check | R7 | ✅ pass | 0.0 | TestQueryValidationAndHealth, TestRoutesAndCorrelation |
| P1.1 both sources map to canonical HazardEvents | P1.1 | ✅ pass | 0.8 | TestSeismicBoundariesAndWarnings, TestVolcanicAdditiveValuesAndStableIdentity, TestRequiredFieldsRejectMissingNullAndWrongType |
| P1.2-P1.4 schema change applies at runtime and stays valid | P1.2, P1.3, P1.4 | ✅ pass | 2.0 | TestSchemaChangeAtRuntime, TestIntegrationLivePollingMappingFreshnessAndReads |
| P2.1-P2.2 BMKG reads stay fast under sustained load | P2.1, P2.2 | ✅ pass | 98.4 | TestIndependentPollingDuringSlowSourceAndOutage, TestAPIAdmissionAndDeadline |
| P2.2 overload is rejected with counted 429 responses | P2.2 | ✅ pass | 26.4 | TestAdmissionLimitDoesNotCallDependenciesAndReleasesCapacity, TestUpstream429PreservesStatusAndSafeRetryAfter |
| P2.3 outage serves stale volcanic data and fresh seismic data | P2.3, R4 | ✅ pass | 10.4 | TestOutageErrorModeAndRecovery, TestIntegrationLivePollingMappingFreshnessAndReads |
| P2.3 hanging source never makes reads hang | P2.3, R4 | ✅ pass | 15.7 | TestOutageHangModeSendsNoResponse, TestSourceBoundsRetriesAndCancellation |
| P3.1 agencies reject each other's credentials | P3.1, R5 | ✅ pass | 3.1 | TestCredentialDomains |
| P3.2 Media gets summary fields and raw requests get 403 | P3.2 | ✅ pass | 2.6 | TestMediaRawRequestForbiddenAndRawClientGetsAllFields, TestSummaryFiltersRawFieldsAndPreservesFreshnessAndCorrelation |
| P3.3 expired token refreshes without login and the old token is rejected | P3.3 | ✅ pass | 62.6 | TestNaturalExpiryThenRefreshWithoutLogin, TestLoginScopesAndRefreshSupersedesOldSessionVersion, TestIntegrationConcurrentRefreshRevocationAndExpiry |
| P3.4 no credentials are committed and .env.example lists every variable | P3.4, R6 | ✅ pass | 0.0 |  |
| P4.2 old and new attribute shapes coexist without migration | P4.2 | ✅ pass | 4.5 | TestIntegrationStoreCommitRollbackRestartAndOwnership |
| P4.3 only the Aggregator reaches the Canonical Store and every store has one owner | P4.3, R2 | ✅ pass | 0.6 | TestIntegrationStoreCommitRollbackRestartAndOwnership |
| P5.1-P5.2 events fan out to independent consumers | P5.1, P5.2 | ✅ pass | 1.3 | TestIntegrationRabbitFanoutConfirmAndReconnect, TestIntegrationIndependentQueuesAndRestartCatchup |
| P5.3 a stopped consumer blocks nothing and catches up | P5.3, R4 | ✅ pass | 5.7 | TestIntegrationIndependentQueuesAndRestartCatchup, TestIntegrationRedeliveryAfterPersistBeforeAck |
| P5.4 a new consumer joins without producer changes | P5.4 | ✅ pass | 14.4 |  |
| R7 correlation IDs cross every service in a flow | R7 | ✅ pass | 2.9 | TestAPIRouteMethodsAndCorrelation, TestSourceCallLogsDependencyAndSafeFailure |
| R7 every outbound call logs its latency | R7 | ✅ pass | 6.7 | TestSourceCallLogsDependencyAndSafeFailure |
| P4.1 one service rebuilds while the others keep serving | P4.1, R4 | ✅ pass | 15.9 |  |
| P2.3 failed source without stored data is explicitly unavailable | P2.3 | ✅ pass | 12.5 | TestAPIAuthenticationQueriesAndFreshness |

## Specification Coverage

| Criterion | Requirement | Result |
|---|---|---|
| S1 | Mocks seed 20+ records, add 1+ per 10s, keep their delays, filter by since, and keep data across restarts | ✅ pass |
| P1.1 | Both sources map to HazardEvent by the mapping rules | ✅ pass |
| P1.2 | PVMBG schema change runs without restarting any service | ✅ pass |
| P1.3 | HazardEvents stay valid after the schema change | ✅ pass |
| P1.4 | Unknown fields follow the declared attributes policy | ✅ pass |
| P1.5 | Schema evolution mechanism and supported changes | report section |
| P2.1 | BMKG-only p95 below 300 ms while PVMBG takes 3 s | ✅ pass |
| P2.2 | 50+ connections for 60+ s with full metrics, errors below 1%, 429 counted | ✅ pass |
| P2.3 | Outage serves stale data or explicit unavailability and recovers without restart | ✅ pass |
| P2.4 | Concurrency model and load test configuration | report section |
| P3.1 | Each agency rejects the other agency's credentials with 401 or 403 | ✅ pass |
| P3.2 | Media gets summary fields and explicit raw requests get 401 or 403 | ✅ pass |
| P3.3 | Expired Field Team token refreshes without login and the old token is rejected | ✅ pass |
| P3.4 | No hard-coded or committed credentials and .env.example exists | ✅ pass |
| P3.5 | Authentication mechanism and why it opens no new gap | report section |
| P4.1 | One service rebuilds and restarts while independent requests keep working | ✅ pass |
| P4.2 | Old and new attribute shapes stored side by side without migration | ✅ pass |
| P4.3 | Only the Aggregator accesses the Canonical Store | ✅ pass |
| P4.4 | Service boundaries and two storage alternatives compared | report section |
| P5.1 | New HazardEvents are published to a broker | ✅ pass |
| P5.2 | Two or more independent consumers receive the same stream | ✅ pass |
| P5.3 | A stopped consumer blocks neither producer nor others and catches up | ✅ pass |
| P5.4 | A new consumer joins without producer code changes | ✅ pass |
| P5.5 | Broker, delivery semantics, and idempotency | report section |
| R1 | Services talk only over the network and share no business logic | ✅ pass |
| R2 | Each storage has exactly one owning service | ✅ pass |
| R3 | Each service has its own container and one command starts all | ✅ pass |
| R4 | Services start and stop independently under real failures | ✅ pass |
| R5 | Credentials are valid in one agency only | ✅ pass |
| R6 | Configuration and secrets come from the environment | ✅ pass |
| R7 | Health checks, correlated structured logs, and outbound latency | ✅ pass |

Evidence for each check is in `demo.json`.
