package process

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/openshift-assisted/assisted-events-streams/internal/types"
	"github.com/openshift-assisted/assisted-events-streams/pkg/jsonedit"
	"github.com/openshift/assisted-service/pkg/uploader/models"
	"github.com/sirupsen/logrus"
)

type EventEnricher struct {
	logger                 *logrus.Logger
	fieldsToUnpack         []string
	fieldsMapToList        []string
	fieldsMapToListDropKey []string
	fieldsToDelete         []string
	fieldsToAnonymize      map[string]string
}

func NewEventEnricher(logger *logrus.Logger) *EventEnricher {
	return &EventEnricher{
		logger:                 logger,
		fieldsToUnpack:         getDefaultFieldsToUnpack(),
		fieldsMapToList:        getDefaultFieldsMapToList(),
		fieldsMapToListDropKey: getDefaultFieldsMapToListDropKey(),
		fieldsToDelete:         getDefaultFieldsToDelete(),
		fieldsToAnonymize:      getDefaultFieldsToAnonymize(),
	}
}

func (e *EventEnricher) GetEnrichedEvent(event *types.Event, cluster map[string]interface{}, hosts []map[string]interface{}, infraEnvs []map[string]interface{}) *types.EnrichedEvent {
	enrichedEvent, err := e.GetBaseEnrichedEvent(event, cluster, hosts, infraEnvs)
	if err != nil {
		e.logger.WithError(err).Debug("Error creating base enriched event")
	}
	return e.getTransformedEvent(enrichedEvent)
}

// Get event after having applied all required transformations
func (e *EventEnricher) getTransformedEvent(enrichedEvent *types.EnrichedEvent) *types.EnrichedEvent {
	originalJson, err := json.Marshal(enrichedEvent)
	if err != nil {
		e.logger.WithError(err).Debug("error marshaling enriched event")
		return enrichedEvent
	}

	unpackedJson, err := unpackJson(originalJson, e.fieldsToUnpack)
	if err != nil {
		e.logger.WithError(err).Debug("error unpacking json")
	}
	transformedJson, err := mapToListJsonDropKey(unpackedJson, e.fieldsMapToListDropKey)
	if err != nil {
		e.logger.WithError(err).Debug("error transforming json")
	}
	transformedJson, err = mapToListJson(transformedJson, e.fieldsMapToList)
	if err != nil {
		e.logger.WithError(err).Debug("error transforming json")
	}
	anonymizedJson, err := anonymizeJson(transformedJson, e.fieldsToAnonymize)
	if err != nil {
		e.logger.WithError(err).Debug("error anonymizing json")
	}
	deletedJson, err := jsonedit.Delete(anonymizedJson, e.fieldsToDelete)
	if err != nil {
		e.logger.WithError(err).Debug("error deleting json")
	}
	withHostsSummaryJson, err := e.addHostsSummaryJson(deletedJson)
	if err != nil {
		e.logger.WithError(err).Debug("error adding summary json")
	}

	return e.getEnrichedEventFromJson(withHostsSummaryJson)
}

func (e *EventEnricher) addHostsSummaryJson(eventJson []byte) ([]byte, error) {
	event := e.getEnrichedEventFromJson(eventJson)
	err := AddHostsSummary(event)
	if err != nil {
		return eventJson, err
	}
	outJson, err := json.Marshal(event)
	if err != nil {
		return outJson, err
	}
	return outJson, nil
}

// Get enriched event before applying any transformation
func (e *EventEnricher) GetBaseEnrichedEvent(event *types.Event, cluster map[string]interface{}, hosts []map[string]interface{}, infraEnvs []map[string]interface{}) (*types.EnrichedEvent, error) {
	namespace, err := uuid.FromBytes([]byte("abcdefghilmnopqr"))
	if err != nil {
		return nil, fmt.Errorf("Error getting uuid namespace, most likely seed is not 16 characters")
	}
	enrichedEvent := &types.EnrichedEvent{}
	props, err := getProps(event.Payload)
	if err != nil {
		e.logger.WithError(err).Debug("error while extracting event props")
	}
	enrichedEvent.Event.Properties = props

	// remove ID, as it's passed as INT (only from onprem data)
	payload, ok := event.Payload.(map[string]interface{})
	if ok {
		delete(payload, "ID")
	}

	eventBytes, err := json.Marshal(payload)
	if err != nil {
		return enrichedEvent, err
	}
	err = json.Unmarshal(eventBytes, enrichedEvent)
	cluster["hosts"] = getHostsWithEmbeddedInfraEnv(hosts, infraEnvs)

	enrichedEvent.ID = uuid.NewSHA1(namespace, []byte(enrichedEvent.Message+enrichedEvent.EventTime)).String()

	eventMetadata := e.getEventMetadata(event.Metadata, enrichedEvent.Name)
	if eventMetadata != nil && eventMetadata.DeploymentType != "" {
		enrichedEvent.DeploymentType = &eventMetadata.DeploymentType
	}

	enrichedEvent.Versions = e.getVersionsFromMetadata(event.Metadata, eventMetadata, enrichedEvent.Name)
	enrichedEvent.ReleaseTag = e.getReleaseTagFromMetadata(event.Metadata, enrichedEvent.Name)

	enrichedEvent.Cluster = cluster
	enrichedEvent.InfraEnvs = infraEnvs

	return enrichedEvent, err
}

func (e *EventEnricher) getEventMetadata(metadata map[string]interface{}, eventName string) *models.Metadata {
	eventMetadata, ok := metadata["eventmetadata"]
	if !ok {
		return nil
	}

	ret, ok := eventMetadata.(models.Metadata)
	if !ok {
		e.logger.Warnf("eventmetadata type in %s is not models.Metadata", eventName)
		return nil
	}

	return &ret
}

func (e *EventEnricher) getVersionsFromMetadata(metadata map[string]interface{}, eventMetadata *models.Metadata, eventName string) map[string]interface{} {
	ret := e.extractVersionFromMetadata(metadata, eventName)
	if ret == nil {
		return nil
	}

	if eventMetadata == nil {
		return ret
	}

	ret["git_ref"] = eventMetadata.GitRef
	ret["deployment_version"] = eventMetadata.DeploymentVersion

	return ret
}

func (e *EventEnricher) extractVersionFromMetadata(metadata map[string]interface{}, eventName string) map[string]interface{} {
	var (
		versionsMapInterface interface{}
		versionsMap          map[string]interface{}
		versionsInterface    interface{}
		innerVersionsMap     map[string]interface{}
		ok                   bool
	)
	if versionsMapInterface, ok = metadata["versions"]; !ok {
		e.logger.Debugf("No versions found in event %s", eventName)
		return nil
	}
	if versionsMap, ok = versionsMapInterface.(map[string]interface{}); !ok {
		e.logger.Debugf("The found versions in %s are not a valid map", eventName)
		return nil
	}
	if versionsInterface, ok = versionsMap["versions"]; !ok {
		return versionsMap
	}
	if innerVersionsMap, ok = versionsInterface.(map[string]interface{}); !ok {
		e.logger.Debugf("The found versions in %s are not a valid map", eventName)
		return versionsMap
	}
	return innerVersionsMap
}

func (e *EventEnricher) getReleaseTagFromMetadata(metadata map[string]interface{}, eventName string) *string {
	var (
		versionsMapInterface interface{}
		versionsMap          map[string]interface{}
		releaseTagInterface  interface{}
		releaseTag           string
		ok                   bool
	)
	if versionsMapInterface, ok = metadata["versions"]; !ok {
		e.logger.Debugf("No versions found in event %s", eventName)
		return nil
	}
	if versionsMap, ok = versionsMapInterface.(map[string]interface{}); !ok {
		e.logger.Debugf("The found versions in %s are not a valid map", eventName)
		return nil
	}
	if releaseTagInterface, ok = versionsMap["release_tag"]; !ok {
		e.logger.Debugf("No release tag found in %s", eventName)
		return nil
	}
	if releaseTag, ok = releaseTagInterface.(string); !ok {
		e.logger.Debugf("The release tag found in %s is not a valid string", eventName)
		return nil
	}
	return &releaseTag
}

func getHostsWithEmbeddedInfraEnv(hosts []map[string]interface{}, infraEnvs []map[string]interface{}) []map[string]interface{} {
	for i, host := range hosts {
		if infraEnvID, ok := host["infra_env_id"]; ok {
			for _, infraEnv := range infraEnvs {
				if infraEnv["id"] == infraEnvID {
					hosts[i]["infra_env"] = infraEnv
				}
			}
		}
	}
	return hosts
}

func (e *EventEnricher) getEnrichedEventFromJson(eventJson []byte) *types.EnrichedEvent {
	var outEvent types.EnrichedEvent
	err := json.Unmarshal(eventJson, &outEvent)
	if err != nil {
		e.logger.WithError(err).Debug("error unmarshaling json to enriched event")
	}
	return &outEvent
}

func getProps(eventPayload interface{}) (map[string]interface{}, error) {
	defaultProps := map[string]interface{}{}
	if payload, ok := eventPayload.(map[string]interface{}); ok {
		if rawProps, ok := payload["props"]; ok {
			if p, ok := rawProps.(string); ok {
				structuredProps := map[string]interface{}{}
				err := json.Unmarshal([]byte(p), &structuredProps)
				if err != nil {
					return structuredProps, err
				}
				return structuredProps, nil
			}
		}
	}
	return defaultProps, nil
}
