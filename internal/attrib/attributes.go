// Package attrib is concerned with OpenTelemetry attributes.
// It defines ExplorViz-specific attributes and provides utilities
// for efficiently retrieving attributes from OTLP data.
package attrib

import "go.opentelemetry.io/otel/attribute"

type ExplorVizAttribute struct {
	Key          attribute.Key
	DefaultValue string
}

// ExplorVizAttributes defines attributes required or written by ExplorViz itself.
var ExplorVizAttributes = struct {
	LandscapeTokenID     ExplorVizAttribute
	LandscapeTokenSecret ExplorVizAttribute
	EntityId             ExplorVizAttribute
}{
	LandscapeTokenID: ExplorVizAttribute{
		Key:          "explorviz.token.id",
		DefaultValue: "mytokenvalue",
	},
	LandscapeTokenSecret: ExplorVizAttribute{
		Key:          "explorviz.token.secret",
		DefaultValue: "mytokenvalue",
	},
	EntityId: ExplorVizAttribute{
		Key:          "explorviz.entity.id",
		DefaultValue: "unknown",
	},
}
