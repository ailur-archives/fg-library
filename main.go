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

type DBType int

const (
	Sqlite DBType = iota
	Postgres
)

type Database struct {
	DB     *sql.DB
	DBType DBType
}
type MessageCode int

const (
	Success MessageCode = iota
	BadRequest
	InternalError
	Unauthorized
)

var buffer = make(map[uuid.UUID]InterServiceMessage)
var waitingList = make(map[uuid.UUID]struct{})
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
				delete(waitingList, id)
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
	mutex.Lock()
	waitingList[id] = struct{}{}
	mutex.Unlock()
	s.Outbox <- msg
	return id
}

func (s *InterServiceMessage) Respond(messageType MessageCode, message any, information ServiceInitializationInformation) {
	s.ServiceID, s.ForServiceID = s.ForServiceID, s.ServiceID
	s.MessageType = messageType
	s.Message = message
	s.SentAt = time.Now()
	information.Outbox <- *s
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
		return Database{}, err
	}

	return response.Message.(Database), nil
}

func (s *ServiceInitializationInformation) AcceptMessage() InterServiceMessage {
	for {
		<-arrived
		mutex.Lock()
		for id, msg := range buffer {
			_, ok := waitingList[id]

			if !ok {
				delete(buffer, id)
				mutex.Unlock()
				return msg
			}
		}
		mutex.Unlock()
	}
}
