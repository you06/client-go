package util

const (
	// InternalRequest is the scope of internal queries
	InternalRequest = "internal_"
	// ExternalRequest is the scope of external queries
	ExternalRequest = "external_"
	// SourceUnknown keeps same with the default value(empty string)
	SourceUnknown = ""
)

// RequestSource contains the source label of the request, used for tracking resource consuming.
type RequestSource struct {
	RequestSourceInternal bool
	RequestSourceType     string
}

// SetRequestSourceInternal sets the scope of the request source.
func (r *RequestSource) SetRequestSourceInternal(internal bool) {
	r.RequestSourceInternal = internal
}

// SetRequestSourceType sets the type of the request source.
func (r *RequestSource) SetRequestSourceType(tp string) {
	r.RequestSourceType = tp
}

// GetRequestSource gets the request_source field of the request.
func (r *RequestSource) GetRequestSource() string {
	// if r.RequestSourceType is not set, it's mostly possible that r.RequestSourceInternal is not set
	// to avoid internal requests be marked as external(default value), return unknown source here.
	if r.RequestSourceType == "" {
		return SourceUnknown
	}
	if r.RequestSourceInternal {
		return InternalRequest + r.RequestSourceType
	}
	return ExternalRequest + r.RequestSourceType
}
