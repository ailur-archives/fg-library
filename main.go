package library

import (
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"io/fs"
	"time"
)

type Permissions struct {
	Authenticate              bool `validate:"required"`
	Database                  bool `validate:"required"`
	BlobStorage               bool `validate:"required"`
	InterServiceCommunication bool `validate:"required"`
	Resources                 bool `validate:"required"`
}

type Service struct {
	Name        string      `validate:"required"`
	Permissions Permissions `validate:"required"`
	ServiceID   uuid.UUID   `validate:"required"`
}

type InterServiceMessage struct {
	ServiceID    uuid.UUID `validate:"required"`
	ForServiceID uuid.UUID `validate:"required"`
	MessageType  uint64    `validate:"required"`
	SentAt       time.Time `validate:"required"`
	Message      any       `validate:"required"`
}

type ServiceInitializationInformation struct {
	Domain        string                     `validate:"required"`
	Outbox        chan<- InterServiceMessage `validate:"required"`
	Inbox         <-chan InterServiceMessage `validate:"required"`
	Router        *chi.Mux                   `validate:"required"`
	Configuration map[string]interface{}
	ResourceDir   fs.FS
}
