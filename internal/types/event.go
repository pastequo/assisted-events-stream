package types

type EventEnvelope struct {
	Event Event
	Key   []byte
}

type Event struct {
	Name     string                 `json:"name"`
	Payload  interface{}            `json:"payload"`
	Metadata map[string]interface{} `json:"metadata"`
}

type State struct {
	Metadata map[string]interface{} `json:"metadata"`
	Payload  map[string]interface{} `json:"payload"`
}

type EnrichedEvent struct {
	// we'll be using a UUID based on event fields ID until we'll receive a proper UUID from the service
	ID             string                   `json:"id"`
	Message        string                   `json:"message"`
	Category       string                   `json:"category"`
	ClusterID      string                   `json:"cluster_id"`
	EventTime      string                   `json:"event_time"`
	HostsSummary   *HostsSummary            `json:"host_summary"`
	Name           string                   `json:"name"`
	RequestID      string                   `json:"request_id"`
	Severity       string                   `json:"severity"`
	Event          EmbeddedEvent            `json:"event,omitempty"`
	Cluster        map[string]interface{}   `json:"cluster"`
	InfraEnvs      []map[string]interface{} `json:"infra_envs,omitempty"`
	Versions       map[string]interface{}   `json:"versions"`
	ReleaseTag     *string                  `json:"release_tag,omitempty"`
	DeploymentType *string                  `json:"deployment_type,omitempty"`
}

type EmbeddedEvent struct {
	Properties map[string]interface{} `json:"props,omitempty"`
}
type ClusterState State

type HostState State

type InfraEnvState State
