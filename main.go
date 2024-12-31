package library

import (
	"database/sql"
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"io/fs"
	"sync"
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
	MessageID    uuid.UUID `validate:"required"`
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

type DBType int

const (
	Sqlite DBType = iota
	Postgres
)

type Database struct {
	DB     *sql.DB
	DBType DBType
}

type ResponseCode int

const (
	Success ResponseCode = iota
	BadRequest
	InternalError
	Unauthorized
)

var buffer = make(map[uuid.UUID]InterServiceMessage)
var mutex = sync.Mutex{}
var arrived = make(chan uuid.UUID)

func ISMessageBuffer(inbox <-chan InterServiceMessage) {
	for {
		mutex.Lock()
		msg := <-inbox
		buffer[msg.MessageID] = msg
		mutex.Unlock()
		arrived <- msg.MessageID
	}
}

var (
	ErrTimeout = errors.New("timeout")
)

func AwaitISMessage(id uuid.UUID, timeout time.Duration) (InterServiceMessage, error) {
	for {
		select {
		case <-time.After(timeout):
			return InterServiceMessage{}, ErrTimeout
		case msgID := <-arrived:
			if msgID == id {
				mutex.Lock()
				msg := buffer[id]
				delete(buffer, id)
				mutex.Unlock()
				return msg, nil
			}
		}
	}
}

func (s *ServiceInitializationInformation) SendISMessage(service Service, forService uuid.UUID, messageType uint64, message any) uuid.UUID {
	id := uuid.New()
	msg := InterServiceMessage{
		MessageID:    id,
		ServiceID:    service.ServiceID,
		ForServiceID: forService,
		MessageType:  messageType,
		SentAt:       time.Now(),
		Message:      message,
	}
	s.Outbox <- msg
	return id
}

func (s *ServiceInitializationInformation) SendAndAwaitISMessage(service Service, forService uuid.UUID, messageType uint64, message any, timeout time.Duration) (InterServiceMessage, error) {
	id := s.SendISMessage(service, forService, messageType, message)
	return AwaitISMessage(id, timeout)
}
