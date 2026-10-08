# Test results

`summary.json` records full-stack checks and the run configuration. `p2-k6.json` contains load metrics, `p2-connections.json` contains TCP samples, and `p2-output.txt` contains the load-test summary.

`save-publish-crash.txt` records the known publication gap. Service logs stay local and are ignored by Git.

Run `make e2e-p2` to refresh the results. `make evidence-images` runs the tests and captures their terminal output directly for the README.
