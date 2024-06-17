package onprem

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kelseyhightower/envconfig"
	"github.com/openshift-assisted/assisted-events-streams/internal/types"
	"github.com/openshift/assisted-service/pkg/uploader"
	"github.com/openshift/assisted-service/pkg/uploader/models"
	"github.com/sirupsen/logrus"
)

type EventTypeMatch struct {
	glob         string
	eventName    string
	clusterIDKey string
}

//go:generate mockgen -source=event_extractor.go -package=onprem -destination=mock_event_extractor.go
type IEventExtractor interface {
	ExtractEvents(tarFilename string) (chan types.EventEnvelope, error)
}
type EventExtractor struct {
	logger           *logrus.Logger
	workDir          string
	eventsBufferSize int
}
type ExtractorConfig struct {
	WorkDir string `envconfig:"WORK_DIRECTORY" required:"true"`
}

func NewEventExtractorFromEnv(logger *logrus.Logger, channelsConfig *ChannelsConfig) (*EventExtractor, error) {
	config := &ExtractorConfig{}
	err := envconfig.Process("", config)
	if err != nil {
		return nil, err
	}

	return &EventExtractor{
		logger:           logger,
		workDir:          config.WorkDir,
		eventsBufferSize: channelsConfig.EventChannelBufferSize,
	}, nil
}

func (e *EventExtractor) ExtractEvents(tarFilename string) (chan types.EventEnvelope, error) {
	logger := e.logger.WithFields(logrus.Fields{
		"tarFilename": tarFilename,
	})
	eventChannel := make(chan types.EventEnvelope, e.eventsBufferSize)
	tmpdir, err := os.MkdirTemp("", ".untar-")
	if err != nil {
		logger.WithError(err).Error("failed to create tmpdir")
		return eventChannel, err
	}
	defer os.RemoveAll(tmpdir)
	logger = logger.WithFields(logrus.Fields{
		"tmpdir": tmpdir,
	})
	logger.Info("untarring file into tmpdir")
	e.untarFile(logger, tarFilename, tmpdir)

	logger.Info("extracting events from files in directory")
	e.extractEvents(logger, tmpdir, tarFilename, eventChannel)
	return eventChannel, nil
}

// untar target tarball into tmpdir
func (e *EventExtractor) untarFile(logger *logrus.Entry, filename string, tmpdir string) {
	gzipfile, err := os.Open(filename)
	if err != nil {
		logger.WithError(err).Error("failed to open file")
		return
	}
	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		logger.WithError(err).Error("failed to unzip file")
		return
	}
	defer reader.Close()

	tarReader := tar.NewReader(reader)
	for {
		if !e.readTarLine(logger, tarReader, tmpdir) {
			break
		}
	}
}

func (e *EventExtractor) readTarLine(logger *logrus.Entry, tarReader *tar.Reader, tmpdir string) bool {
	header, err := tarReader.Next()
	if err == io.EOF {
		return false
	} else if err != nil {
		logger.WithError(err).Error("error while reading tar file")
		return false
	}

	path := filepath.Join(tmpdir, header.Name)
	info := header.FileInfo()
	if info.IsDir() {
		if err = os.MkdirAll(path, info.Mode()); err != nil {
			logger.WithError(err).WithFields(logrus.Fields{
				"dir": path,
			}).Error("error while creating directory")
			return true
		}
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0744); err != nil {
		logger.WithError(err).WithFields(logrus.Fields{
			"filename": path,
			"dir":      dir,
		}).Error("error creating directory for file")
		return true
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{
			"dst":  path,
			"mode": info.Mode(),
		}).Error("error creating file")
		return true
	}
	defer file.Close()
	_, err = io.Copy(file, tarReader)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{
			"dst": path,
		}).Error("error copying file")
		return true
	}
	return true
}

// extract events from given target tmpdir (where extracted files would be)
func (e *EventExtractor) extractEvents(logger *logrus.Entry, tmpdir string, filename string, eventChannel chan types.EventEnvelope) {
	eventTypesMatches := []EventTypeMatch{
		{
			glob:         "/*/events/cluster.json",
			eventName:    "ClusterState",
			clusterIDKey: "id",
		},
		{
			glob:         "/*/events/hosts.json",
			eventName:    "HostState",
			clusterIDKey: "cluster_id",
		},
		{
			glob:         "/*/events/infraenv.json",
			eventName:    "InfraEnv",
			clusterIDKey: "cluster_id",
		},
		{
			glob:         "/*/events.json",
			eventName:    "Event",
			clusterIDKey: "cluster_id",
		},
	}

	versions, err := getVersionsFromFile(tmpdir, "/*/versions.json")
	if err != nil {
		logger.WithError(err).Warning("error extracting versions")
	}

	metadata, err := extractMetadataEvents(filename)
	if err != nil {
		logger.WithError(err).Warning("error extracting metadata")
	}

	for _, eventTypeMatch := range eventTypesMatches {
		e.extractEventsForEventType(logger, eventTypeMatch, tmpdir, metadata, versions, eventChannel)
	}
	close(eventChannel)
}

func getVersionsFromFile(tmpdir string, filename string) (map[string]string, error) {
	versions := map[string]string{}
	files, err := filepath.Glob(tmpdir + filename)
	if err != nil {
		return versions, fmt.Errorf("could not glob %s in %s", filename, tmpdir)
	}
	if len(files) < 1 {
		return versions, fmt.Errorf("no versions file found (%s)", filename)
	}
	jsonFile, err := os.Open(files[0])
	if err != nil {
		return versions, err
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)
	err = json.Unmarshal(byteValue, &versions)
	if err != nil {
		return versions, err
	}
	return versions, nil
}

func extractMetadataEvents(filename string) ([]models.Events, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	ret, err := uploader.ExtractEvents(context.TODO(), file)
	if err != nil {
		return nil, fmt.Errorf("failed to extract metadata events: %w", err)
	}

	return ret, nil
}

func (e *EventExtractor) extractEventsForEventType(logger *logrus.Entry, eventTypeMatch EventTypeMatch, tmpdir string, metadata []models.Events, versions map[string]string, eventChannel chan types.EventEnvelope) {
	files, err := filepath.Glob(fmt.Sprintf("%s%s", tmpdir, eventTypeMatch.glob))
	if err != nil {
		logger.WithError(err).Error("error globbing files")
		return
	}
	logger.WithFields(logrus.Fields{
		"files": files,
		"glob":  eventTypeMatch.glob,
	}).Info("extracting events from files in directory")

	for _, jFile := range files {
		resources, err := e.getResourcesFromFile(logger, jFile)
		if err != nil {
			logger.WithError(err).WithFields(logrus.Fields{
				"jsonFile": jFile,
			}).Error("Error getting resources from json file")
		}
		e.transformResourcesToEvents(logger, eventTypeMatch, resources, metadata, versions, eventChannel)
	}
}

func findEventMetadata(clusterID string, events []models.Events) *models.Metadata {
	for _, event := range events {
		if event.ClusterID == clusterID {
			return event.Metadata
		}
	}

	return nil
}

func getMetadata(clusterID string, events []models.Events, versions map[string]string) map[string]interface{} {
	ret := map[string]interface{}{
		"versions": map[string]interface{}{
			// nested value, as here would go other version related metadata
			// such as `release_tag`. See getVersionsFromMetadata method in
			// package projection/process
			"versions": versions,
		},
	}

	eventMetadata := findEventMetadata(clusterID, events)
	if eventMetadata != nil {
		ret["eventmetadata"] = *eventMetadata
	}

	return ret
}

func (e *EventExtractor) transformResourcesToEvents(logger *logrus.Entry, eventTypeMatch EventTypeMatch, resources []map[string]interface{}, metadata []models.Events, versions map[string]string, eventChannel chan types.EventEnvelope) {
	for _, resource := range resources {
		clusterID, ok := resource[eventTypeMatch.clusterIDKey]
		if !ok {
			logger.WithFields(logrus.Fields{
				"resource":       resource,
				"cluster_id_key": eventTypeMatch.clusterIDKey,
			}).Warning("could not extract cluster_id from resource")
			return
		}
		cID, ok := clusterID.(string)
		if !ok {
			logger.Warning("cluster_id not a string")
			return
		}

		if eventTypeMatch.eventName == "ClusterState" {
			resource["onprem"] = true
			// delete hosts, as message could be too big
			// hosts are covered by the HostState event type
			delete(resource, "hosts")
		}
		event := types.Event{
			Name:     eventTypeMatch.eventName,
			Payload:  resource,
			Metadata: getMetadata(cID, metadata, versions),
		}
		logger = logger.WithFields(logrus.Fields{
			"cluster_id":     clusterID,
			"cluster_id_key": eventTypeMatch.clusterIDKey,
			"event_type":     event.Name,
		})
		logger.Info("generating event for cluster")
		logger.WithFields(logrus.Fields{
			"event": event,
		}).Debug("generated event")

		eventChannel <- types.EventEnvelope{
			Key:   []byte(cID),
			Event: event,
		}
	}
}

func (e *EventExtractor) getResourcesFromFile(logger *logrus.Entry, filename string) ([]map[string]interface{}, error) {
	resources := []map[string]interface{}{}
	jsonFile, err := os.Open(filename)
	if err != nil {
		logger.WithError(err).WithFields(logrus.Fields{
			"file": filename,
		}).Error("Error opening file")
		return []map[string]interface{}{}, err
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)
	err = json.Unmarshal(byteValue, &resources)
	if err != nil {
		logger.WithError(err).Warning("Error decoding json file for resource list, trying resource")
		resource := map[string]interface{}{}
		err = json.Unmarshal(byteValue, &resource)
		if err != nil {
			return []map[string]interface{}{}, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}
