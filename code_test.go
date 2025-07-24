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
