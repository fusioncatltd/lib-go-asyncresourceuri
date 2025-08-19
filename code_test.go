package lib_go_asyncresourceuri

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseAsyncResourceReference(t *testing.T) {
	// Case 0 — invalid URI string
	result0, err0 := ParseAsyncResourceReference("async+kafka:/")
	assert.Nil(t, result0)
	assert.Contains(t, err0.Error(), "invalid resource reference format")

	// Case 1 — valid kafka URI
	result1, err1 := ParseAsyncResourceReference("async+kafka://broker1@readwrite/topic/user.profile.updated.v2")

	assert.Nil(t, err1)
	assert.Equal(t, result1.Protocol, PROTOCOL_KAFKA)
	assert.Equal(t, result1.Mode, MODE_READWRITE)
	assert.Equal(t, result1.Type, RESOURCE_TYPE_TOPIC)
	assert.Equal(t, result1.Name, "user.profile.updated.v2")

	// Case 2 — invalid kafka-like URI
	result2, err2 := ParseAsyncResourceReference("async+kaffka://broker1@readwrite/topic/user.profile.updated.v2")
	assert.Nil(t, result2)
	assert.Contains(t, err2.Error(), "invalid protocol")

	// Case 3 — invalid resource type
	result3, err3 := ParseAsyncResourceReference("async+kafka://broker1@write/queue/user.profile.updated.v2")
	assert.Nil(t, result3)
	assert.Contains(t, err3.Error(), "invalid resource type")

	// Case 4 — invalid resource name
	result4, err4 := ParseAsyncResourceReference("async+kafka://broker1@write/topic/user-profile.updated.v2")
	assert.Nil(t, result4)
	assert.Contains(t, err4.Error(), "must be alphanumeric")
}

func TestAssemble(t *testing.T) {
	// Test 1: Valid Kafka topic URI
	uri1 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_KAFKA,
		Server:   "broker1.example.com",
		Mode:     MODE_READWRITE,
		Type:     RESOURCE_TYPE_TOPIC,
		Name:     "user.profile.updated.v2",
	}
	result1, err1 := uri1.Assemble()
	assert.Nil(t, err1)
	assert.Equal(t, "async+kafka://broker1.example.com@readwrite/topic/user.profile.updated.v2", result1)

	// Test 2: Valid AMQP queue URI
	uri2 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_AMQP,
		Server:   "rabbitmq.example.com",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_QUEUE,
		Name:     "order_queue",
	}
	result2, err2 := uri2.Assemble()
	assert.Nil(t, err2)
	assert.Equal(t, "async+amqp://rabbitmq.example.com@read/queue/order_queue", result2)

	// Test 3: Valid AMQP exchange URI
	uri3 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_AMQP,
		Server:   "rabbitmq.example.com",
		Mode:     MODE_WRITE,
		Type:     RESOURCE_TYPE_EXCHANGE,
		Name:     "events_exchange",
	}
	result3, err3 := uri3.Assemble()
	assert.Nil(t, err3)
	assert.Equal(t, "async+amqp://rabbitmq.example.com@write/exchange/events_exchange", result3)

	// Test 4: Valid MQTT topic URI
	uri4 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_MQTT,
		Server:   "mqtt.broker.com",
		Mode:     MODE_WRITE,
		Type:     RESOURCE_TYPE_TOPIC,
		Name:     "sensors.temperature",
	}
	result4, err4 := uri4.Assemble()
	assert.Nil(t, err4)
	assert.Equal(t, "async+mqtt://mqtt.broker.com@write/topic/sensors.temperature", result4)

	// Test 5: Valid DB table URI
	uri5 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_DB,
		Server:   "postgres.db.com",
		Mode:     MODE_READWRITE,
		Type:     RESOURCE_TYPE_TABLE,
		Name:     "users_table",
	}
	result5, err5 := uri5.Assemble()
	assert.Nil(t, err5)
	assert.Equal(t, "async+db://postgres.db.com@readwrite/table/users_table", result5)

	// Test 6: Valid Webhook endpoint URI
	uri6 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_WEBHOOK,
		Server:   "api.example.com",
		Mode:     MODE_WRITE,
		Type:     RESOURCE_TYPE_ENDPOINT,
		Name:     "webhooks.orders",
	}
	result6, err6 := uri6.Assemble()
	assert.Nil(t, err6)
	assert.Equal(t, "async+webhook://api.example.com@write/endpoint/webhooks.orders", result6)

	// Test 7: Invalid protocol
	uri7 := &AsyncResourceReferenceURI{
		Protocol: "invalid",
		Server:   "server",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_TOPIC,
		Name:     "test",
	}
	result7, err7 := uri7.Assemble()
	assert.Empty(t, result7)
	assert.Contains(t, err7.Error(), "invalid protocol")

	// Test 8: Invalid resource type for protocol
	uri8 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_KAFKA,
		Server:   "broker1",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_QUEUE,
		Name:     "test",
	}
	result8, err8 := uri8.Assemble()
	assert.Empty(t, result8)
	assert.Contains(t, err8.Error(), "invalid resource type")

	// Test 9: Invalid mode for resource type
	uri9 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_AMQP,
		Server:   "rabbitmq",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_EXCHANGE,
		Name:     "test",
	}
	result9, err9 := uri9.Assemble()
	assert.Empty(t, result9)
	assert.Contains(t, err9.Error(), "invalid mode")

	// Test 10: Invalid resource name with hyphens
	uri10 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_KAFKA,
		Server:   "broker1",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_TOPIC,
		Name:     "test-topic",
	}
	result10, err10 := uri10.Assemble()
	assert.Empty(t, result10)
	assert.Contains(t, err10.Error(), "invalid resource name")

	// Test 11: Empty server
	uri11 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_KAFKA,
		Server:   "",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_TOPIC,
		Name:     "test_topic",
	}
	result11, err11 := uri11.Assemble()
	assert.Empty(t, result11)
	assert.Contains(t, err11.Error(), "server cannot be empty")
}

func TestValidate(t *testing.T) {
	// Test 1: Valid URI struct
	uri1 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_KAFKA,
		Server:   "broker1",
		Mode:     MODE_READWRITE,
		Type:     RESOURCE_TYPE_TOPIC,
		Name:     "test_topic",
	}
	err1 := uri1.Validate()
	assert.Nil(t, err1)

	// Test 2: Empty fields
	uri2 := &AsyncResourceReferenceURI{}
	err2 := uri2.Validate()
	assert.NotNil(t, err2)

	// Test 3: Invalid combination of protocol and resource type
	uri3 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_MQTT,
		Server:   "broker",
		Mode:     MODE_READ,
		Type:     RESOURCE_TYPE_EXCHANGE,
		Name:     "test",
	}
	err3 := uri3.Validate()
	assert.Contains(t, err3.Error(), "invalid resource type")

	// Test 4: Invalid mode for webhook (readwrite not allowed)
	uri4 := &AsyncResourceReferenceURI{
		Protocol: PROTOCOL_WEBHOOK,
		Server:   "api.example.com",
		Mode:     MODE_READWRITE,
		Type:     RESOURCE_TYPE_ENDPOINT,
		Name:     "webhook_endpoint",
	}
	err4 := uri4.Validate()
	assert.Contains(t, err4.Error(), "invalid mode")
}

func TestRoundTrip(t *testing.T) {
	// Test that parsing and assembling produce the same result
	testCases := []string{
		"async+kafka://broker1@readwrite/topic/user.profile.updated.v2",
		"async+amqp://rabbitmq.example.com@read/queue/order_queue",
		"async+amqp://rabbitmq.example.com@write/exchange/events_exchange",
		"async+mqtt://mqtt.broker.com@write/topic/sensors.temperature",
		"async+db://postgres.db.com@readwrite/table/users_table",
		"async+webhook://api.example.com@write/endpoint/webhooks.orders",
	}

	for _, original := range testCases {
		parsed, err := ParseAsyncResourceReference(original)
		assert.Nil(t, err, "Failed to parse: %s", original)

		assembled, err := parsed.Assemble()
		assert.Nil(t, err, "Failed to assemble: %s", original)

		assert.Equal(t, original, assembled, "Round trip failed for: %s", original)
	}
}
