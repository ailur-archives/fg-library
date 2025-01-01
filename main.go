package library

import (
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"io/fs"
	"math/big"
	"sync"
	"time"
)

type Permissions struct {
	// Authenticate allows the service to register with the nucleus authentication service and use OAuth2
	Authenticate bool `validate:"required"`
	// Router allows the service to serve web pages
	Router bool `validate:"required"`
	// Database allows the service to ask for a centralised database connection
	Database bool `validate:"required"`
	// BlobStorage allows the service to use the blob storage service
	BlobStorage bool `validate:"required"`
	// InterServiceCommunication allows the service to send and receive messages from other services
	InterServiceCommunication bool `validate:"required"`
	// Resources allows the service to access their resource directory
	Resources bool `validate:"required"`
}

type Service struct {
	Name        string      `validate:"required"`
	Permissions Permissions `validate:"required"`
	ServiceID   uuid.UUID   `validate:"required"`
}

type InterServiceMessage struct {
	MessageID    uuid.UUID   `validate:"required"`
	ServiceID    uuid.UUID   `validate:"required"`
	ForServiceID uuid.UUID   `validate:"required"`
	MessageType  MessageCode `validate:"required"`
	SentAt       time.Time   `validate:"required"`
	Message      any         `validate:"required"`
}

// NewServiceInitializationInformation creates a new ServiceInitializationInformation and is only ever meant to be called
// by fulgens or a compliant implementation of fulgens.
func NewServiceInitializationInformation(domain *string, outbox chan<- InterServiceMessage, inbox <-chan InterServiceMessage, router *chi.Mux, configuration map[string]interface{}, resourceDir fs.FS) ServiceInitializationInformation {
	return ServiceInitializationInformation{
		Domain:        domain,
		Outbox:        outbox,
		inbox:         inbox,
		Router:        router,
		Configuration: configuration,
		ResourceDir:   resourceDir,
	}
}

type ServiceInitializationInformation struct {
	Service       *Service `validate:"required"`
	Domain        *string
	Outbox        chan<- InterServiceMessage `validate:"required"`
	inbox         <-chan InterServiceMessage `validate:"required"`
	Router        *chi.Mux
	Configuration map[string]interface{}
	ResourceDir   fs.FS
}

// YesIAbsolutelyKnowWhatIAmDoingAndIWantToAccessTheRawInbox returns a channel that can be used to read messages from
// the inbox. This is a dangerous operation, can and will break the buffer, and you should most absolutely not use this
// unless you would like to handle the messages yourself with no outside help or synchronization.
//
// If you think you know what you're doing, **you probably don't**.
func (s *ServiceInitializationInformation) YesIAbsolutelyKnowWhatIAmDoingAndIWantToAccessTheRawInbox() <-chan InterServiceMessage {
	return s.inbox
}

type ColumnType interface{}

type (
	// String represents arbitrary sized text
	String string
	// Int32 represents a 32-bit signed integer
	Int32 int32
	// Int64 represents a 64-bit signed integer
	Int64 int64
	// IntInf represents an arbitrary sized signed integer
	IntInf big.Int
	// Float32 represents a 32-bit floating point number
	Float32 float32
	// Float64 represents a 64-bit floating point number
	Float64 float64
	// Boolean represents a boolean value
	Boolean bool
	// Blob represents an arbitrary sized binary object
	Blob []byte
	// UUID represents a UUID value
	UUID uuid.UUID
	// Time represents a time value
	Time time.Time
)

type TableSchema map[string]ColumnType
type Row map[string]any
type Rows []Row

type QueryParameters struct {
	Equal              map[string]any
	NotEqual           map[string]any
	GreaterThan        map[string]any
	LessThan           map[string]any
	GreaterThanOrEqual map[string]any
	LessThanOrEqual    map[string]any
}

type UpdateParameters map[string]any

type Database interface {
	CreateTable(name string, schema TableSchema) error
	DeleteTable(name string) error
	InsertRow(name string, row Row) error
	Delete(name string, params QueryParameters) error
	Select(name string, params QueryParameters) (Rows, error)
	Update(name string, params QueryParameters, update UpdateParameters) error
}

type MessageCode int

const (
	Success MessageCode = iota
	BadRequest
	InternalError
	Unauthorized
)

var buffer = make(map[uuid.UUID]InterServiceMessage)
var mutex = sync.Mutex{}
var arrived = make(chan uuid.UUID)
var ispStarted = false

func (s *ServiceInitializationInformation) StartISProcessor() {
	if ispStarted {
		return
	} else {
		ispStarted = true
	}
	listener := NewListener(s.inbox)
	for {
		msg := listener.AcceptMessage()
		mutex.Lock()
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
				if msg.MessageType != Success {
					return msg, msg.Message.(error)
				}
				return msg, nil
			}
		}
	}
}

type Listener interface {
	AcceptMessage() InterServiceMessage
}

type DefaultListener <-chan InterServiceMessage

func NewListener(c <-chan InterServiceMessage) Listener {
	return DefaultListener(c)
}

func (l DefaultListener) AcceptMessage() InterServiceMessage {
	return <-l
}

func (s *ServiceInitializationInformation) SendISMessage(forService uuid.UUID, messageType MessageCode, message any) uuid.UUID {
	id := uuid.New()
	msg := InterServiceMessage{
		MessageID:    id,
		ServiceID:    s.Service.ServiceID,
		ForServiceID: forService,
		MessageType:  messageType,
		SentAt:       time.Now(),
		Message:      message,
	}
	s.Outbox <- msg
	return id
}

func (s *ServiceInitializationInformation) SendAndAwaitISMessage(forService uuid.UUID, messageType MessageCode, message any, timeout time.Duration) (InterServiceMessage, error) {
	id := s.SendISMessage(forService, messageType, message)
	return AwaitISMessage(id, timeout)
}

func (s *ServiceInitializationInformation) GetDatabase() (Database, error) {
	if !ispStarted {
		go s.StartISProcessor()
	}

	response, err := s.SendAndAwaitISMessage(uuid.Nil, 0, nil, 5*time.Second)
	if err != nil {
		return nil, err
	}

	return response.Message.(Database), nil
}
