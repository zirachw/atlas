#!/usr/bin/env python3
"""Run full-stack checks in Compose."""
import argparse
import base64
import json
import os
import shlex
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--p2', action='store_true', help='include load testing (Linux only)')
args = parser.parse_args()
ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'services/bnpb/aggregator/evidence'
EVIDENCE.mkdir(exist_ok=True)
LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
report = {'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
          'working_tree_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip()),
          'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
          'host': {'kernel': os.uname().release, 'architecture': os.uname().machine,
                   'cpus': os.cpu_count(), 'memory_bytes': os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')},
          'configuration': {'pvmbg_delay': '3s', 'event_interval': '2s', 'poll_interval': '1s',
                            'access_token_ttl': '60s', 'host_ports': 'ephemeral loopback'}, 'checks': {}}


def check(name, value):
    report['checks'][name] = value
    print('PASS ' + name, flush=True)


def await_condition(name, predicate, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(1)
    raise RuntimeError('timed out: ' + name)


def http(url, token=None, body=None, headers=None):
    h = dict(headers or {})
    if token:
        h['Authorization'] = 'Bearer ' + token
    payload = None if body is None else json.dumps(body).encode()
    if payload:
        h['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=payload, headers=h)
    try:
        response = LOCAL_HTTP.open(req, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.loads(response.read())


with tempfile.TemporaryDirectory(prefix='atlas-b-live-') as tmp:
    tmp = Path(tmp)
    env = dict(line.split('=', 1) for line in (ROOT / '.env.example').read_text().splitlines()
               if line and not line.startswith('#'))
    private = subprocess.check_output(['openssl', 'genpkey', '-algorithm', 'Ed25519'])
    public = subprocess.check_output(['openssl', 'pkey', '-pubout'], input=private)
    env.update(AUTH_ED25519_PRIVATE_KEY_B64=base64.b64encode(private).decode(),
               AUTH_ED25519_PUBLIC_KEY_B64=base64.b64encode(public).decode(),
               BMKG_PORT='0', PVMBG_PORT='0', CLIENT_API_PORT='0', AUTH_PORT='0',
               PVMBG_MIN_RESPONSE_DELAY='3s', PVMBG_MAX_RESPONSE_DELAY='3s',
               SOURCE_EVENT_INTERVAL='2s', POLL_INTERVAL='1s', ACCESS_TOKEN_TTL='60s')
    envfile = tmp / 'example.env'
    envfile.write_text(''.join(k + '=' + v + '\n' for k, v in env.items()))
    envfile.chmod(0o600)
    override = tmp / 'ports.yaml'
    override.write_text('services:\n  aggregator:\n    ports: ["127.0.0.1:0:8083"]\n')
    project = 'atlas-b-live-' + str(os.getpid())
    archived_logs = {}
    compose = ['docker', 'compose', '--env-file', str(envfile), '-p', project,
               '-f', str(ROOT / 'docker-compose.yml'), '-f', str(override)]

    def dc(*args, allow_failure=False):
        result = subprocess.run(compose + list(args), cwd=ROOT, capture_output=True, text=True)
        if result.returncode and not allow_failure:
            raise RuntimeError('compose ' + ' '.join(args) + ': ' + result.stderr[-4000:])
        return result.stdout

    def sql(statement):
        return dc('exec', '-T', 'postgres', 'psql', '-U', 'atlas_admin', '-d', 'canonical', '-Atc', statement).strip()

    def records(service):
        lines = (archived_logs.get(service, '') + dc('logs', '--no-log-prefix', '--no-color', service)).splitlines()
        return [json.loads(line) for line in lines if line.startswith('{')]

    def processed(service):
        return {entry['hazard_id'] for entry in records(service) if entry.get('msg') == 'event_processed'}

    def url(service, port):
        return 'http://' + dc('port', service, str(port)).strip()

    try:
        print('Starting test stack...', flush=True)
        dc('build')
        # Bind consumers before publishing.
        dc('up', '-d', '--wait', '--wait-timeout', '120', 'postgres', 'rabbitmq', 'bmkg', 'pvmbg', 'auth', 'consumer-a', 'consumer-b')
        dc('up', '-d', '--wait', '--wait-timeout', '120', 'aggregator', 'client-api')
        agg, client, auth = url('aggregator', 8083), url('client-api', 8080), url('auth', 8084)
        bmkg, pvmbg = url('bmkg', 8081), url('pvmbg', 8082)
        internal = env['AGGREGATOR_INTERNAL_TOKEN']

        def login_access():
            status, login = http(auth + '/auth/login', body={'client_id': env['INTERNAL_OPS_CLIENT_ID'], 'client_secret': env['INTERNAL_OPS_CLIENT_SECRET']})
            assert status == 200
            return login['access_token']

        def hazards(source='', raw=False):
            params = 'limit=500' + ('&source=' + source if source else '')
            if raw:
                return http(client + '/hazards?' + params + '&view=raw', access)
            return http(agg + '/internal/hazards?' + params, internal)

        await_condition('both sources ingested', lambda: all(f['status'] == 'healthy' for f in hazards()[1]['sources']))
        baseline = hazards()[1]
        assert {h['source'] for h in baseline['data']} == {'BMKG', 'PVMBG'}
        baseline_ids = {h['source_ref_id']: h['hazard_id'] for h in baseline['data']}
        check('canonical_mapping', {'count': len(baseline_ids), 'sample': baseline['data'][:3]})
        assert sql('SELECT count(*) = count(DISTINCT (source,source_ref_id)) FROM hazard_events') == 't'
        check('unique_source_identity', True)
        columns = sql("SELECT column_name FROM information_schema.columns WHERE table_name='hazard_events' ORDER BY ordinal_position")
        before_container = dc('ps', '-q', 'aggregator').strip()
        before_pvmbg = dc('ps', '-q', 'pvmbg').strip()
        assert http(pvmbg + '/admin/schema-version', body={'version': 2})[0] == 200
        changed = await_condition('confidence ingestion', lambda: next((h for h in hazards('PVMBG')[1]['data'] if 'confidence_level' in h['attributes']), None))
        old = next(h for h in hazards('PVMBG')[1]['data'] if h['source_ref_id'] in baseline_ids)
        assert 'confidence_level' not in old['attributes']
        assert old['hazard_id'] == baseline_ids[old['source_ref_id']]
        assert columns == sql("SELECT column_name FROM information_schema.columns WHERE table_name='hazard_events' ORDER BY ordinal_position")
        assert before_container == dc('ps', '-q', 'aggregator').strip() and before_pvmbg == dc('ps', '-q', 'pvmbg').strip()
        check('live_additive_schema_without_restart_or_column', {'old': old, 'new': changed, 'columns': columns.splitlines()})
        access = login_access()
        status, data = hazards(raw=True)
        assert status == 200 and data['data']
        settings = json.loads(dc('config', '--format', 'json'))
        assert not any('DATABASE' in k for k in settings['services']['client-api']['environment'])
        check('client_reads_over_http_without_database_credentials', {'count': len(data['data']), 'sources': data['sources']})
        timings = []
        for _ in range(50):
            start = time.monotonic()
            assert hazards('BMKG', raw=True)[0] == 200
            timings.append((time.monotonic() - start) * 1000)
        assert any(e.get('msg') == 'source_call' and e.get('source') == 'PVMBG' and e.get('status') == 200 and e.get('latency_ms', 0) >= 3000 for e in records('aggregator'))
        check('three_second_pvmbg_isolated_from_reads', {'requests': 50, 'p95_ms': sorted(timings)[47], 'max_ms': max(timings), 'note': 'sequential smoke measurement, not the sustained P2 load demonstration'})
        if args.p2:
            # Preserve publisher logs before rotation.
            archived_logs['aggregator'] = dc('logs', '--no-log-prefix', '--no-color', 'aggregator')
            image = 'grafana/k6:1.3.0'
            subprocess.run(['docker', 'pull', image], check=True, stdout=subprocess.DEVNULL)
            load_env = {'BASE_URL': client, 'AUTH_URL': auth, 'DURATION': '90s',
                        'BMKG_VUS': '40', 'VOLCANIC_VUS': '20',
                        'CLIENT_ID': env['INTERNAL_OPS_CLIENT_ID'],
                        'CLIENT_SECRET': env['INTERNAL_OPS_CLIENT_SECRET'],
                        'SUMMARY_JSON': '/results/p2-k6.json', 'SUMMARY_TEXT': '/results/p2-output.txt'}
            command = ['docker', 'run', '--rm', '--name', project + '-k6', '--network', 'host',
                       '--user', str(os.getuid()) + ':' + str(os.getgid()),
                       '-v', str(ROOT / 'loadtest') + ':/scripts:ro', '-v', str(EVIDENCE) + ':/results']
            for key, value in load_env.items():
                command.extend(['-e', key + '=' + value])
            command.extend([image, 'run', '--quiet', '/scripts/p2.js'])
            if freeze := os.environ.get('ATLAS_EVIDENCE_FREEZE'):
                screenshot = ROOT / 'docs/evidence/p2-summary.png'
                command = [freeze, '--execute', shlex.join(command), '--execute.timeout', '130s',
                           '--font.size', '16', '--padding', '24', '--margin', '16',
                           '--border.radius', '12', '--window', '-o', str(screenshot)]
            samples = []
            print('Running load test...', flush=True)
            with (tmp / 'load-runner.log').open('w') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                started = time.monotonic()
                try:
                    while process.poll() is None:
                        if time.monotonic() - started > 130:
                            raise RuntimeError('P2 runner exceeded 130-second deadline')
                        # Count established Client API sockets.
                        tcp = dc('exec', '-T', 'client-api', 'cat', '/proc/net/tcp', '/proc/net/tcp6')
                        connections = sum(1 for line in tcp.splitlines()
                                          if len(line.split()) > 3 and line.split()[1].endswith(':1F90')
                                          and line.split()[3] == '01')
                        samples.append({'elapsed_seconds': round(time.monotonic() - started, 3),
                                        'established_client_connections': connections})
                        time.sleep(2)
                finally:
                    if process.poll() is None:
                        subprocess.run(['docker', 'stop', project + '-k6'], capture_output=True)
                        process.wait(timeout=15)
            (EVIDENCE / 'p2-connections.json').write_text(json.dumps(samples, indent=2) + '\n')
            assert process.returncode == 0, (tmp / 'load-runner.log').read_text()
            sustained = [sample for sample in samples if 10 <= sample['elapsed_seconds'] <= 85]
            assert sustained and sustained[-1]['elapsed_seconds'] - sustained[0]['elapsed_seconds'] >= 60
            assert min(sample['established_client_connections'] for sample in sustained) >= 50
            metrics = json.loads((EVIDENCE / 'p2-k6.json').read_text())
            values = metrics['metrics']
            assert values['http_req_duration{kind:bmkg}']['values']['p(95)'] < 300
            assert values['unexpected_errors']['values']['rate'] < 0.01
            assert values.get('auth_failures', {'values': {'count': 0}})['values']['count'] == 0
            check('p2_sustained_compose_load', {'runner': image, 'duration_seconds': metrics['state']['testRunDurationMs'] / 1000,
                  'observed_connection_min': min(sample['established_client_connections'] for sample in sustained),
                  'observed_span_seconds': sustained[-1]['elapsed_seconds'] - sustained[0]['elapsed_seconds'],
                  'bmkg_p95_ms': values['http_req_duration{kind:bmkg}']['values']['p(95)'],
                  'unexpected_error_rate': values['unexpected_errors']['values']['rate'],
                  'controlled_rejections': values.get('controlled_rejections', {'values': {'count': 0}})['values']['count'],
                  'auth_failures': 0, 'samples': 'p2-connections.json', 'metrics': 'p2-k6.json'})
            access = login_access()
        assert http(pvmbg + '/admin/outage', body={'enabled': True, 'mode': 'error'})[0] == 200
        before_calls = len([e for e in records('aggregator') if e.get('msg') == 'poll_committed' and e.get('source') == 'BMKG'])
        def stale_response():
            status, response = hazards('PVMBG')
            return response if status == 200 and response['sources'][0]['status'] == 'stale' else None

        stale = await_condition('stale response', stale_response)
        assert stale['data'] and stale['sources'][0]['stale_since']
        await_condition('BMKG poll progress during outage', lambda: len([e for e in records('aggregator') if e.get('msg') == 'poll_committed' and e.get('source') == 'BMKG']) > before_calls + 2)
        access = login_access()
        assert hazards('BMKG', raw=True)[0] == 200
        check('outage_stale_reads_and_independent_bmkg', {'sources': stale['sources'], 'stored_count': len(stale['data'])})
        assert http(pvmbg + '/admin/outage', body={'enabled': False})[0] == 200
        await_condition('automatic recovery', lambda: hazards('PVMBG')[1]['sources'][0]['status'] == 'healthy')
        assert before_container == dc('ps', '-q', 'aggregator').strip()
        check('automatic_recovery_without_restart', True)
        assert http(pvmbg + '/admin/outage', body={'enabled': True, 'mode': 'hang'})[0] == 200
        hang_started = time.monotonic()
        hanging = await_condition('hanging source marked stale after bounded retries', stale_response, seconds=45)
        access = login_access()
        assert hazards('BMKG', raw=True)[0] == 200
        check('hanging_source_deadlines_and_isolation', {'failure_detected_seconds': time.monotonic() - hang_started, 'sources': hanging['sources']})
        assert http(pvmbg + '/admin/outage', body={'enabled': False})[0] == 200
        await_condition('recovery from hanging source', lambda: hazards('PVMBG')[1]['sources'][0]['status'] == 'healthy')
        matching = await_condition('both consumers receive sample', lambda: {changed['hazard_id']} & processed('consumer-a') & processed('consumer-b'))
        confirms = [e for e in records('aggregator') if e.get('msg') == 'broker_call' and e.get('confirmed') and e.get('hazard_id') == changed['hazard_id']]
        assert confirms
        check('confirmed_publication_to_independent_queues', {'hazard_ids': sorted(matching), 'confirm': confirms[0]})
        ids_before = processed('consumer-a')
        dc('stop', 'consumer-a')
        new_b = await_condition('B continues while A stopped', lambda: processed('consumer-b') - ids_before)
        dc('start', 'consumer-a')
        await_condition('A catches up', lambda: new_b <= processed('consumer-a'))
        check('consumer_a_offline_b_progress_and_catchup', {'hazard_ids': sorted(new_b)})
        future_before = processed('consumer-b')
        dc('--profile', 'consumer-c-demo', 'up', '-d', '--wait', '--wait-timeout', '90', 'consumer-c-demo')
        future = await_condition('new subscriber receives future publication', lambda: processed('consumer-c-demo') & (processed('consumer-b') - future_before))
        assert before_container == dc('ps', '-q', 'aggregator').strip()
        check('subscriber_c_added_without_producer_changes', {'future_hazard_ids': sorted(future)})
        checkpoint = sql('SELECT source,checkpoint FROM source_progress ORDER BY source')
        peer_ids = {s: dc('ps', '-q', s).strip() for s in ['auth', 'client-api', 'bmkg', 'pvmbg', 'consumer-a', 'consumer-b']}
        archived_logs['aggregator'] = archived_logs.get('aggregator', '') + dc('logs', '--no-log-prefix', '--no-color', 'aggregator')
        dc('up', '-d', '--no-deps', '--build', '--force-recreate', '--wait', '--wait-timeout', '90', 'aggregator')
        agg = url('aggregator', 8083)
        await_condition('restart source recovery', lambda: all(f['status'] == 'healthy' for f in hazards()[1]['sources']))
        after_ids = {h['source_ref_id']: h['hazard_id'] for h in hazards()[1]['data']}
        assert all(after_ids.get(ref) == ident for ref, ident in baseline_ids.items())
        assert all(dc('ps', '-q', s).strip() == ident for s, ident in peer_ids.items())
        assert sql('SELECT count(*) = count(DISTINCT (source,source_ref_id)) FROM hazard_events') == 't'
        check('independent_aggregator_rebuild_and_restart', {'checkpoint_before': checkpoint, 'checkpoint_after': sql('SELECT source,checkpoint FROM source_progress ORDER BY source'), 'stable_ids': len(baseline_ids), 'peer_containers_unchanged': True})
        denied = subprocess.run(compose + ['exec', '-T', '-e', 'PGPASSWORD=' + env['AUTH_DB_PASSWORD'], 'postgres', 'psql', '-h', '127.0.0.1', '-U', 'auth', '-d', 'canonical', '-c', 'SELECT 1'], capture_output=True, text=True)
        assert denied.returncode != 0 and 'permission denied' in denied.stderr
        check('non_owner_canonical_access_denied', True)
        access = login_access()
        correlation = 'b-live-protected-read'
        assert http(client + '/hazards?source=BMKG', access, headers={'X-Correlation-ID': correlation})[0] == 200
        for s in ['client-api', 'auth', 'aggregator']:
            assert any(e.get('correlation_id') == correlation for e in records(s))
        check('client_auth_aggregator_correlation', correlation)
        # Check an unavailable source with no stored data last.
        assert http(pvmbg + '/admin/outage', body={'enabled': True, 'mode': 'error'})[0] == 200
        await_condition('last outage marked', lambda: hazards('PVMBG')[1]['sources'][0]['status'] == 'stale')
        sql("DELETE FROM hazard_events WHERE source='PVMBG'")
        assert hazards('PVMBG')[0] == 503 and hazards()[0] == 206 and hazards('BMKG')[0] == 200
        check('empty_failed_source_explicitly_unavailable', {'PVMBG': 503, 'mixed': 206, 'BMKG': 200})
        report['status'] = 'passed'
    except Exception as error:
        report['status'] = 'failed'
        report['failure'] = str(error)
        raise
    finally:
        for service in ['aggregator', 'bmkg', 'pvmbg', 'client-api', 'auth', 'consumer-a', 'consumer-b', 'consumer-c-demo']:
            (EVIDENCE / (service + '.jsonl')).write_text(archived_logs.get(service, '') + dc('logs', '--no-log-prefix', '--no-color', service, allow_failure=True))
        report['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        (EVIDENCE / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
        dc('--profile', 'consumer-c-demo', 'down', '-v', '--remove-orphans')
        print('Results: ' + str(EVIDENCE.relative_to(ROOT)), flush=True)
