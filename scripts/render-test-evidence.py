#!/usr/bin/env python3
"""Run and capture full-stack tests."""
import argparse
import os
from pathlib import Path
import shlex
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--freeze', default='freeze', help='executable path')
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
output = root / 'docs/evidence'
output.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, ATLAS_EVIDENCE_FREEZE=args.freeze)
subprocess.run([args.freeze, '--execute', shlex.join(['python3', 'scripts/verify-aggregator-stack.py', '--p2']),
                '--execute.timeout', '15m', '--font.size', '16', '--padding', '24',
                '--margin', '16', '--border.radius', '12', '--window', '--wrap', '100',
                '-o', str(output / 'e2e-summary.png')],
               check=True, cwd=root, env=env, stdin=subprocess.DEVNULL)
