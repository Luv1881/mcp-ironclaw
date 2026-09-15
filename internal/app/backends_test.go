package app

import (
	"reflect"
	"testing"
)

func TestBrokerListsTrimWhitespace(t *testing.T) {
	tests := map[string]struct {
		raw  string
		want []string
	}{
		"empty":            {"", nil},
		"only whitespace":  {"   ", nil},
		"single":           {"kafka:9092", []string{"kafka:9092"}},
		"spaced separator": {"a:9092, b:9092", []string{"a:9092", "b:9092"}},
		"trailing comma":   {"a:9092,", []string{"a:9092"}},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			got := Backends{KafkaBrokers: testCase.raw}.brokers()
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("got %v, want %v: an untrimmed address is a broker the client can never reach", got, testCase.want)
			}
		})
	}
}

func TestTopicPrefixDefaultsAndOverrides(t *testing.T) {
	if got := (Backends{}).topic("events.raw"); got != "ironclaw.events.raw" {
		t.Fatalf("default topic %q, want ironclaw.events.raw", got)
	}
	if got := (Backends{TopicPrefix: "acme"}).topic("events.raw"); got != "acme.events.raw" {
		t.Fatalf("overridden topic %q, want acme.events.raw", got)
	}
}

func TestSecuritySettingsCarryEveryCredentialField(t *testing.T) {
	backends := Backends{
		RequireTLS:     true,
		CAFile:         "ca.pem",
		CertFile:       "cert.pem",
		KeyFile:        "key.pem",
		KafkaSASLUser:  "kafka-user",
		KafkaSASLPass:  "kafka-pass",
		RedisUsername:  "redis-user",
		RedisPassword:  "redis-pass",
		PostgresRootCA: "root.pem",
	}

	kafka := backends.kafkaSecurity()
	if !kafka.Enabled || kafka.CAFile != "ca.pem" || kafka.CertFile != "cert.pem" ||
		kafka.KeyFile != "key.pem" || kafka.SASLUser != "kafka-user" || kafka.SASLPassword != "kafka-pass" {
		t.Fatalf("kafka security mapping dropped a field: %+v", kafka)
	}

	redis := backends.redisSecurity()
	if !redis.Enabled || redis.CAFile != "ca.pem" || redis.CertFile != "cert.pem" ||
		redis.KeyFile != "key.pem" || redis.Username != "redis-user" || redis.Password != "redis-pass" {
		t.Fatalf("redis security mapping dropped a field: %+v", redis)
	}
}

func TestCodecAcceptsBothWireFormats(t *testing.T) {
	for _, format := range []string{"", "json", "protobuf"} {
		if _, err := (Backends{WireFormat: format}).codec(); err != nil {
			t.Fatalf("wire format %q: unexpected error %v", format, err)
		}
	}

	if _, err := (Backends{WireFormat: "yaml"}).codec(); err == nil {
		t.Fatal("an unknown wire format must be refused rather than silently defaulting")
	}
}
