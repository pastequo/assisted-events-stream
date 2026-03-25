package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/openshift-assisted/assisted-events-streams/internal/types"
	"github.com/sirupsen/logrus"
)

var defaultDuration = 720 * time.Hour

var _ = Describe("Setting objects", func() {
	var (
		ctx          context.Context
		snapshotRepo *SnapshotRepository
		redis        *redis.Client
		mock         redismock.ClientMock
		logger       *logrus.Logger
	)
	BeforeEach(func() {
		logger = logrus.New()
		logger.Out = io.Discard
		ctx = context.Background()
		redis, mock = redismock.NewClientMock()
		snapshotRepo = &SnapshotRepository{
			logger:     logger,
			redis:      redis,
			expiration: defaultDuration,
		}
	})
	AfterEach(func() {
		mock.ClearExpect()
	})
	When("setting empty value", func() {
		It("should set it", func() {
			var event *types.Event
			mock.Regexp().ExpectSet("clusters_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", regexp.QuoteMeta("[]"), defaultDuration).SetVal("1")

			err := snapshotRepo.SetCluster(
				ctx,
				"c42cfc1d-411a-4cdb-953b-a8e0f3f82375",
				event,
			)
			Expect(err).To(BeNil())
			Expect(mock.ExpectationsWereMet()).To(BeNil())

		})
	})

	When("setting a struct", func() {
		It("should set it", func() {
			event := &types.Event{
				Name: "FooBar",
			}
			eventBytes, _ := json.Marshal(event.Payload)
			cmp := fmt.Sprintf("%v", eventBytes)

			mock.Regexp().ExpectSet("clusters_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", regexp.QuoteMeta(cmp), defaultDuration).SetVal("1")

			err := snapshotRepo.SetCluster(
				ctx,
				"c42cfc1d-411a-4cdb-953b-a8e0f3f82375",
				event,
			)
			Expect(err).To(BeNil())
			Expect(mock.ExpectationsWereMet()).To(BeNil())

		})
	})

	When("setting a struct and redis returns error", func() {
		It("should return error", func() {
			event := &types.Event{
				Name: "FooBar",
			}
			eventBytes, _ := json.Marshal(event.Payload)
			cmp := fmt.Sprintf("%v", eventBytes)

			expectedError := errors.New("FAIL")
			mock.Regexp().ExpectSet("clusters_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", regexp.QuoteMeta(cmp), defaultDuration).SetErr(expectedError)

			err := snapshotRepo.SetCluster(
				ctx,
				"c42cfc1d-411a-4cdb-953b-a8e0f3f82375",
				event,
			)
			Expect(err).To(MatchError(expectedError))
			Expect(mock.ExpectationsWereMet()).To(BeNil())

		})
	})

	When("setting host", func() {
		It("should call underlying redis", func() {
			event := &types.Event{
				Name: "FooBar",
			}
			eventBytes, _ := json.Marshal(event.Payload)
			cmp := fmt.Sprintf("%v", eventBytes)

			mock.Regexp().ExpectHSet("hosts_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", "8eb96308-9d8b-4293-a4ef-f68dfed549e4", regexp.QuoteMeta(cmp)).SetVal(1)
			mock.Regexp().ExpectExpire("hosts_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", defaultDuration).SetVal(true)

			err := snapshotRepo.SetHost(
				ctx,
				"c42cfc1d-411a-4cdb-953b-a8e0f3f82375",
				"8eb96308-9d8b-4293-a4ef-f68dfed549e4",
				event,
			)
			Expect(err).To(BeNil())
			Expect(mock.ExpectationsWereMet()).To(BeNil())
		})
	})

	When("setting infraenv", func() {
		It("should call underlying redis", func() {
			event := &types.Event{
				Name: "FooBar",
			}
			eventBytes, _ := json.Marshal(event.Payload)
			cmp := fmt.Sprintf("%v", eventBytes)

			mock.Regexp().ExpectHSet("infraenvs_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", "8eb96308-9d8b-4293-a4ef-f68dfed549e4", regexp.QuoteMeta(cmp)).SetVal(1)
			mock.Regexp().ExpectExpire("infraenvs_c42cfc1d-411a-4cdb-953b-a8e0f3f82375", defaultDuration).SetVal(true)

			err := snapshotRepo.SetInfraEnv(
				ctx,
				"c42cfc1d-411a-4cdb-953b-a8e0f3f82375",
				"8eb96308-9d8b-4293-a4ef-f68dfed549e4",
				event,
			)
			Expect(err).To(BeNil())
			Expect(mock.ExpectationsWereMet()).To(BeNil())
		})
	})

})

func TestRepositories(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test repositories")
}
