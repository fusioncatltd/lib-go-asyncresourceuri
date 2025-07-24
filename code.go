package lib_go_asyncresourceuri

import (
	"errors"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const PROTOCOL_KAFKA = "async+kafka"
const PROTOCOL_AMQP = "async+amqp"
const PROTOCOL_MQTT = "async+mqtt"
const PROTOCOL_DB = "async+db"
const PROTOCOL_WEBHOOK = "async+webhook"

var ValidProtocols = []string{PROTOCOL_KAFKA, PROTOCOL_AMQP, PROTOCOL_MQTT, PROTOCOL_DB, PROTOCOL_WEBHOOK}

const MODE_READWRITE = "readwrite"
const MODE_READ = "read"
const MODE_WRITE = "write"

const RESOURCE_TYPE_TOPIC = "topic"
const RESOURCE_TYPE_QUEUE = "queue"
const RESOURCE_TYPE_EXCHANGE = "exchange"
const RESOURCE_TYPE_TABLE = "table"
const RESOURCE_TYPE_ENDPOINT = "endpoint"

var ValidResourceTypesAndModes = map[string]map[string][]string{
	PROTOCOL_KAFKA: {
		RESOURCE_TYPE_TOPIC: {MODE_READWRITE, MODE_READ, MODE_WRITE}, // Kafka topics can be read, written or both
	},
	PROTOCOL_AMQP: {
		RESOURCE_TYPE_QUEUE: {MODE_READ, MODE_WRITE}, // queues can be read or written.
		// If we are writing to a queue directly, we are assuming the usage of default AMQP exchange.
		RESOURCE_TYPE_EXCHANGE: {MODE_WRITE}, // exchanges can only be written
	},
	PROTOCOL_MQTT: {
		RESOURCE_TYPE_TOPIC: {MODE_READ, MODE_WRITE, MODE_READWRITE}, // mqtt topics can be read, written or both
	},
	PROTOCOL_DB: {
		RESOURCE_TYPE_TABLE: {MODE_READ, MODE_WRITE, MODE_READWRITE},
	},
	PROTOCOL_WEBHOOK: {
		RESOURCE_TYPE_ENDPOINT: {MODE_WRITE, MODE_READ}, // Webhook endpoints can't be in readwrite mode
	},
}

// Reference to an asynchronous resource
type AsyncResourceReferenceURI struct {
	Protocol string
	Server   string
	Mode     string
	Type     string
	Name     string
}

// Resource names can contain only alphanumeric (upper and lower case), underscores, and dots
func isValidResourceName(name string) bool {
	validResourceNameRegex := regexp.MustCompile(`^[a-zA-Z0-9_.]+$`)
	return validResourceNameRegex.MatchString(name)
}

// This function tries to parse an asynchronous resource URI into structure.
// The expected format is: protocol://server@mode/type/name
// If parsing fails, it returns an error with a description of what went wrong.
func ParseAsyncResourceReference(ref string) (*AsyncResourceReferenceURI, error) {
	parts := strings.Split(ref, "://")
	if len(parts) != 2 {
		return nil, errors.New("invalid resource reference format, expected 'protocol://server@mode/type/name'")
	}

	protocol := strings.ToLower(parts[0])
	rest := parts[1]

	// Validate protocol
	if !slices.Contains(ValidProtocols, protocol) {
		return nil, errors.New("invalid protocol, must be one of " + strings.Join(ValidProtocols, ", "))
	}

	// Split server and resource parts
	serverAndMode := strings.Split(rest, "@")
	if len(serverAndMode) != 2 {
		return nil, errors.New("invalid server and mode format")
	}

	server := serverAndMode[0]
	modeAndResource := strings.Split(serverAndMode[1], "/")
	if len(modeAndResource) != 3 {
		return nil, errors.New("invalid mode and resource format")
	}

	mode := strings.ToLower(modeAndResource[0])
	resourceType := strings.ToLower(modeAndResource[1])
	resourceName := modeAndResource[2]

	// Validate resource types
	protocolSpecificResourceTypesAndModes, _ := ValidResourceTypesAndModes[protocol]
	protocolSpecificModes, resourceTypeExists := protocolSpecificResourceTypesAndModes[resourceType]
	protocolSpecificResources, _ := protocolSpecificResourceList(protocol)

	// Here we check if the resource type is allowed for the given protocol
	if !resourceTypeExists {
		return nil, errors.New("invalid resource type: " + resourceType + ", must be one of " +
			strings.Join(protocolSpecificResources, ", "))
	}

	// Here we check if mode is valid for the given resource type
	if !slices.Contains(protocolSpecificModes, resourceType) {
		return nil, errors.New("invalid mode for resource type: " + resourceType + ", must be one of " + strings.Join(protocolSpecificModes, ", "))
	}

	// Validate resource name
	if !isValidResourceName(resourceName) {
		return nil, errors.New("invalid resource name: " + resourceName +
			", must be alphanumeric, underscores, or dots")
	}

	return &AsyncResourceReferenceURI{
		Protocol: protocol,
		Server:   server,
		Mode:     mode,
		Type:     resourceType,
		Name:     resourceName,
	}, nil
}

func protocolSpecificResourceList(protocol string) ([]string, error) {
	m, ok := ValidResourceTypesAndModes[protocol]
	if !ok {
		return nil, errors.New("invalid protocol: " + protocol + ", must be one of " +
			strings.Join(ValidProtocols, ", "))
	}

	resources := make([]string, 0, len(m))
	for r := range m {
		resources = append(resources, r)
	}

	sort.Strings(resources)

	return resources, nil
}
