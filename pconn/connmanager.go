// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package pconn

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	//ErrDialNil is used to indicate that Dial cannot be nil in the configuration.
	ErrDialNil = errors.New("Config: Dial cannot be nil")

	// ErrShuttingDown signals that the persistent connection manager is
	// shutting down.
	ErrShuttingDown = errors.New("persistent connection manager is shutting down")

	// maxRetryDuration is the max duration of time retrying of a persistent
	// connection is allowed to grow to.  This is necessary since the retry
	// logic uses a backoff mechanism which increases the interval base times
	// the number of retries that have been done.
	maxRetryDuration = time.Minute * 5

	// defaultRetryDuration is the default duration of time for retrying
	// persistent connections.
	defaultRetryDuration = time.Second * 5
)

// ConnState represents the state of the requested connection.
type ConnState uint8

// ConnState can be either pending, established, disconnected or failed.  When
// a new connection is requested, it is attempted and categorized as
// established or failed depending on the connection result.  An established
// connection which was disconnected is categorized as disconnected.
const (
	ConnPending ConnState = iota
	ConnEstablished
	ConnDisconnected
	ConnFailed
)

// Req is the connection request to a network address. All requests are assumed
// to be permanent, and will be retried until success or cancellation.
type Req struct {
	// The following variables must only be used atomically.
	id uint64

	Addr net.Addr

	conn       net.Conn
	state      ConnState
	stateMtx   sync.RWMutex
	retryCount uint32
}

// updateState updates the state of the connection request.
func (c *Req) updateState(state ConnState) {
	c.stateMtx.Lock()
	c.state = state
	c.stateMtx.Unlock()
}

// ID returns a unique identifier for the connection request.
func (c *Req) ID() uint64 {
	return atomic.LoadUint64(&c.id)
}

// State is the connection state of the requested connection.
func (c *Req) State() ConnState {
	c.stateMtx.RLock()
	state := c.state
	c.stateMtx.RUnlock()
	return state
}

// String returns a human-readable string for the connection request.
func (c *Req) String() string {
	if c.Addr.String() == "" {
		return fmt.Sprintf("reqid %d", atomic.LoadUint64(&c.id))
	}
	return fmt.Sprintf("%s (reqid %d)", c.Addr, atomic.LoadUint64(&c.id))
}

// Config holds the configuration options related to the connection manager.
type Config struct {
	// Listeners defines a slice of listeners for which the connection
	// manager will take ownership of and accept connections.  When a
	// connection is accepted, the OnAccept handler will be invoked with the
	// connection.  Since the connection manager takes ownership of these
	// listeners, they will be closed when the connection manager is
	// stopped.
	//
	// This field will not have any effect if the OnAccept field is not
	// also specified.  It may be nil if the caller does not wish to listen
	// for incoming connections.
	Listeners []net.Listener

	// OnAccept is a callback that is fired when an inbound connection is
	// accepted.  It is the caller's responsibility to close the connection.
	// Failure to close the connection will result in the connection manager
	// believing the connection is still active and thus have undesirable
	// side effects such as still counting toward maximum connection limits.
	//
	// This field will not have any effect if the Listeners field is not
	// also specified since there couldn't possibly be any accepted
	// connections in that case.
	OnAccept func(net.Conn)

	// RetryDuration is the duration to wait before retrying connection
	// requests. Defaults to 5s.
	RetryDuration time.Duration

	// OnConnection is a callback that is fired when a new outbound
	// connection is established.
	OnConnection func(*Req, net.Conn)

	// OnDisconnection is a callback that is fired when an outbound
	// connection is disconnected.
	OnDisconnection func(*Req)

	// Dial connects to the address on the named network. It cannot be nil.
	Dial func(net.Addr) (net.Conn, error)
}

// registerPending is used register an outgoing, pending connection.
type registerPending struct {
	c    *Req
	done chan struct{}
}

// handleConnected is used to queue a successful connection.
type handleConnected struct {
	c    *Req
	conn net.Conn
}

// handleRemoved is used to remove a connection.
type handleRemoved struct {
	id uint64
}

// handleFailed is used to remove a pending connection.
type handleFailed struct {
	c   *Req
	err error
}

// Manager provides a manager to handle network connections.
type Manager struct {
	// The following variables must only be used atomically.
	connReqCount uint64
	start        int32
	stop         int32

	cfg      Config
	wg       sync.WaitGroup
	requests chan interface{}
	quit     chan struct{}
}

// handleFailedConn handles a connection failed that experiences a failed
// attempt to make a connection. It retries the connection after the configured
// retry duration, scaled linearly by the number of retry attempts..
func (cm *Manager) handleFailedConn(c *Req) {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	c.retryCount++
	d := time.Duration(c.retryCount) * cm.cfg.RetryDuration
	if d > maxRetryDuration {
		d = maxRetryDuration
	}
	log.Debugf("Retrying connection to %v in %v", c, d)
	time.AfterFunc(d, func() {
		cm.Connect(c)
	})
}

// connHandler handles all connection related requests.  It must be run as a
// goroutine.
//
// The connection handler makes sure that we maintain a pool of active outbound
// connections so that we remain connected to our permanent peers. Connection
// requests are processed and mapped by their assigned ids.
func (cm *Manager) connHandler() {
	defer cm.wg.Done()

	cancelled := make(map[uint64]struct{})
	pending := make(map[uint64]*Req)
	conns := make(map[uint64]*Req)

out:
	for {
		select {
		case req := <-cm.requests:
			switch msg := req.(type) {

			case registerPending:
				connReq := msg.c
				connReq.updateState(ConnPending)
				pending[connReq.id] = connReq
				close(msg.done)

			case handleConnected:
				connReq := msg.c

				// If this conn req was previously canceled, we
				// can safely ignore it.
				if _, ok := cancelled[connReq.id]; ok {
					log.Debugf("Ignoring connection for"+
						"canceled conn req: %v", connReq)
					delete(cancelled, connReq.id)
					continue
				}

				connReq.updateState(ConnEstablished)
				connReq.conn = msg.conn
				conns[connReq.id] = connReq
				log.Debugf("Connected to %v", connReq)
				connReq.retryCount = 0

				delete(pending, connReq.id)

				if cm.cfg.OnConnection != nil {
					go cm.cfg.OnConnection(connReq, msg.conn)
				}

			case handleFailed:
				connReq := msg.c

				// If this conn req was previously canceled, we
				// can safely ignore it.
				if _, ok := cancelled[connReq.id]; ok {
					log.Debugf("Ignoring retry for canceled "+
						"conn req: %v", connReq)
					delete(cancelled, connReq.id)
					continue
				}

				connReq.updateState(ConnFailed)
				log.Debugf("Failed to connect to %v: %v",
					connReq, msg.err)
				cm.handleFailedConn(connReq)

			case handleRemoved:
				connReq, ok := conns[msg.id]
				if !ok {
					connReq, ok = pending[msg.id]
					if !ok {
						log.Errorf("Unknown connection: "+
							"%d", msg.id)
						continue
					}

					// Pending connection was found, remove
					// it from pending map.
					connReq.updateState(ConnFailed)
					log.Debugf("Canceling: %v", connReq)
					delete(pending, msg.id)

					// Mark this conn req as canceled, just
					// in case the pending connection still
					// succeeds.
					cancelled[msg.id] = struct{}{}
					continue

				}

				// Connection was found, remove it from the
				// connection manager's internal state.
				connReq.updateState(ConnDisconnected)
				if connReq.conn != nil {
					connReq.conn.Close()
				}
				log.Debugf("Disconnected from %v", connReq)
				delete(conns, msg.id)

				if cm.cfg.OnDisconnection != nil {
					go cm.cfg.OnDisconnection(connReq)
				}
			}

		case <-cm.quit:
			break out
		}
	}

	log.Trace("Connection handler done")
}

// Connect assigns an id and dials a connection to the address of the
// connection request.
func (cm *Manager) Connect(c *Req) error {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return ErrShuttingDown
	}
	if atomic.LoadUint64(&c.id) == 0 {
		atomic.StoreUint64(&c.id, atomic.AddUint64(&cm.connReqCount, 1))

		// If this request has never been assigned an id, we will
		// synchronously register it as pending within the connection
		// manager. This will allow us to properly cancel it if
		// necessary.
		done := make(chan struct{})
		select {
		case cm.requests <- registerPending{c, done}:
		case <-cm.quit:
			return ErrShuttingDown
		}

		select {
		case <-done:
		case <-cm.quit:
			return ErrShuttingDown
		}
	}

	log.Debugf("Attempting to connect to %v", c)
	conn, err := cm.cfg.Dial(c.Addr)
	if err != nil {
		// Manager will make another attempt after the retry timeout.
		select {
		case cm.requests <- handleFailed{c, err}:
			return nil
		case <-cm.quit:
			return ErrShuttingDown
		}

	}

	// Connection was successful!
	select {
	case cm.requests <- handleConnected{c, conn}:
		return nil
	case <-cm.quit:
		return ErrShuttingDown
	}
}

// Remove removes the connection corresponding to the given connection
// id from known connections.
func (cm *Manager) Remove(id uint64) {
	if atomic.LoadInt32(&cm.stop) != 0 {
		return
	}

	select {
	case cm.requests <- handleRemoved{id}:
	case <-cm.quit:
	}
}

// listenHandler accepts incoming connections on a given listener.  It must be
// run as a goroutine.
func (cm *Manager) listenHandler(listener net.Listener) {
	defer cm.wg.Done()

	log.Infof("Server listening on %s", listener.Addr())
	for atomic.LoadInt32(&cm.stop) == 0 {
		conn, err := listener.Accept()
		if err != nil {
			// Only log the error if not forcibly shutting down.
			if atomic.LoadInt32(&cm.stop) == 0 {
				log.Errorf("Can't accept connection: %v", err)
			}
			continue
		}
		go cm.cfg.OnAccept(conn)
	}

	log.Tracef("Listener handler done for %s", listener.Addr())
}

// Start launches the connection manager and begins connecting to the network.
func (cm *Manager) Start() {
	// Already started?
	if atomic.AddInt32(&cm.start, 1) != 1 {
		return
	}

	log.Trace("Connection manager started")
	cm.wg.Add(1)
	go cm.connHandler()

	// Start all the listeners so long as the caller requested them and
	// provided a callback to be invoked when connections are accepted.
	if cm.cfg.OnAccept != nil {
		for _, listner := range cm.cfg.Listeners {
			cm.wg.Add(1)
			go cm.listenHandler(listner)
		}
	}

	return
}

// Wait blocks until the connection manager halts gracefully.
func (cm *Manager) Wait() {
	cm.wg.Wait()
}

// Stop gracefully shuts down the connection manager.
func (cm *Manager) Stop() {
	if atomic.AddInt32(&cm.stop, 1) != 1 {
		log.Warnf("Connection manager already stopped")
		return
	}

	// Stop all the listeners.  There will not be any listeners if
	// listening is disabled.
	for _, listener := range cm.cfg.Listeners {
		// Ignore the error since this is shutdown and there is no way
		// to recover anyways.
		_ = listener.Close()
	}

	close(cm.quit)
	log.Trace("Connection manager stopped")
}

// NewManager returns a new connection manager.
// Use Start to start connecting to the network.
func NewManager(cfg *Config) (*Manager, error) {
	if cfg.Dial == nil {
		return nil, ErrDialNil
	}
	// Default to sane values
	if cfg.RetryDuration <= 0 {
		cfg.RetryDuration = defaultRetryDuration
	}

	return &Manager{
		cfg:      *cfg, // Copy so caller can't mutate
		requests: make(chan interface{}),
		quit:     make(chan struct{}),
	}, nil
}
