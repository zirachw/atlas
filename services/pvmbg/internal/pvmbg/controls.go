package pvmbg

import "sync"

type Outage struct {
	Enabled bool   `json:"enabled"`
	Mode    string `json:"mode,omitempty"`
}

type Controls struct {
	mu            sync.RWMutex
	schemaVersion int
	outage        Outage
}

const (
	OutageError = "error" // answer 503 at once
	OutageHang  = "hang"  // never answer, like a dead host
)

// The schema version is cached here, while the store keeps it across restarts.
func NewControls(schemaVersion int) *Controls { return &Controls{schemaVersion: schemaVersion} }

func (c *Controls) SchemaVersion() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.schemaVersion
}

func (c *Controls) SetSchemaVersion(v int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.schemaVersion = v
}

func (c *Controls) Outage() Outage {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.outage
}

func (c *Controls) SetOutage(o Outage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.outage = o
}
