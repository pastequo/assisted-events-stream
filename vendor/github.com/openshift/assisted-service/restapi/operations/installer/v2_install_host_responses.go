// Code generated by go-swagger; DO NOT EDIT.

package installer

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/openshift/assisted-service/models"
)

// V2InstallHostAcceptedCode is the HTTP code returned for type V2InstallHostAccepted
const V2InstallHostAcceptedCode int = 202

/*
V2InstallHostAccepted Success.

swagger:response v2InstallHostAccepted
*/
type V2InstallHostAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.Host `json:"body,omitempty"`
}

// NewV2InstallHostAccepted creates V2InstallHostAccepted with default headers values
func NewV2InstallHostAccepted() *V2InstallHostAccepted {

	return &V2InstallHostAccepted{}
}

// WithPayload adds the payload to the v2 install host accepted response
func (o *V2InstallHostAccepted) WithPayload(payload *models.Host) *V2InstallHostAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the v2 install host accepted response
func (o *V2InstallHostAccepted) SetPayload(payload *models.Host) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *V2InstallHostAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// V2InstallHostUnauthorizedCode is the HTTP code returned for type V2InstallHostUnauthorized
const V2InstallHostUnauthorizedCode int = 401

/*
V2InstallHostUnauthorized Unauthorized.

swagger:response v2InstallHostUnauthorized
*/
type V2InstallHostUnauthorized struct {

	/*
	  In: Body
	*/
	Payload *models.InfraError `json:"body,omitempty"`
}

// NewV2InstallHostUnauthorized creates V2InstallHostUnauthorized with default headers values
func NewV2InstallHostUnauthorized() *V2InstallHostUnauthorized {

	return &V2InstallHostUnauthorized{}
}

// WithPayload adds the payload to the v2 install host unauthorized response
func (o *V2InstallHostUnauthorized) WithPayload(payload *models.InfraError) *V2InstallHostUnauthorized {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the v2 install host unauthorized response
func (o *V2InstallHostUnauthorized) SetPayload(payload *models.InfraError) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *V2InstallHostUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// V2InstallHostForbiddenCode is the HTTP code returned for type V2InstallHostForbidden
const V2InstallHostForbiddenCode int = 403

/*
V2InstallHostForbidden Forbidden.

swagger:response v2InstallHostForbidden
*/
type V2InstallHostForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.InfraError `json:"body,omitempty"`
}

// NewV2InstallHostForbidden creates V2InstallHostForbidden with default headers values
func NewV2InstallHostForbidden() *V2InstallHostForbidden {

	return &V2InstallHostForbidden{}
}

// WithPayload adds the payload to the v2 install host forbidden response
func (o *V2InstallHostForbidden) WithPayload(payload *models.InfraError) *V2InstallHostForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the v2 install host forbidden response
func (o *V2InstallHostForbidden) SetPayload(payload *models.InfraError) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *V2InstallHostForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// V2InstallHostNotFoundCode is the HTTP code returned for type V2InstallHostNotFound
const V2InstallHostNotFoundCode int = 404

/*
V2InstallHostNotFound Error.

swagger:response v2InstallHostNotFound
*/
type V2InstallHostNotFound struct {

	/*
	  In: Body
	*/
	Payload *models.Error `json:"body,omitempty"`
}

// NewV2InstallHostNotFound creates V2InstallHostNotFound with default headers values
func NewV2InstallHostNotFound() *V2InstallHostNotFound {

	return &V2InstallHostNotFound{}
}

// WithPayload adds the payload to the v2 install host not found response
func (o *V2InstallHostNotFound) WithPayload(payload *models.Error) *V2InstallHostNotFound {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the v2 install host not found response
func (o *V2InstallHostNotFound) SetPayload(payload *models.Error) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *V2InstallHostNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// V2InstallHostConflictCode is the HTTP code returned for type V2InstallHostConflict
const V2InstallHostConflictCode int = 409

/*
V2InstallHostConflict Error.

swagger:response v2InstallHostConflict
*/
type V2InstallHostConflict struct {

	/*
	  In: Body
	*/
	Payload *models.Error `json:"body,omitempty"`
}

// NewV2InstallHostConflict creates V2InstallHostConflict with default headers values
func NewV2InstallHostConflict() *V2InstallHostConflict {

	return &V2InstallHostConflict{}
}

// WithPayload adds the payload to the v2 install host conflict response
func (o *V2InstallHostConflict) WithPayload(payload *models.Error) *V2InstallHostConflict {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the v2 install host conflict response
func (o *V2InstallHostConflict) SetPayload(payload *models.Error) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *V2InstallHostConflict) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(409)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// V2InstallHostInternalServerErrorCode is the HTTP code returned for type V2InstallHostInternalServerError
const V2InstallHostInternalServerErrorCode int = 500

/*
V2InstallHostInternalServerError Error.

swagger:response v2InstallHostInternalServerError
*/
type V2InstallHostInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.Error `json:"body,omitempty"`
}

// NewV2InstallHostInternalServerError creates V2InstallHostInternalServerError with default headers values
func NewV2InstallHostInternalServerError() *V2InstallHostInternalServerError {

	return &V2InstallHostInternalServerError{}
}

// WithPayload adds the payload to the v2 install host internal server error response
func (o *V2InstallHostInternalServerError) WithPayload(payload *models.Error) *V2InstallHostInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the v2 install host internal server error response
func (o *V2InstallHostInternalServerError) SetPayload(payload *models.Error) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *V2InstallHostInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
