//go:build linux

package ingress

import (
	"context"
	"errors"
	"net"
	"time"

	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
)

// ReaderAcceptPlan is supplied by the platform endpoint owner AFTER native
// admission. Accept cannot create another listener, launch a consumer or infer
// grants from request data. Bytes includes transient accept/native/FD/kernel
// ownership; the listener and its backlog have their own enclosing admission.
// Each endpoint serializes this operation and owns at most one active session.
type ReaderAcceptPlan struct {
	Bytes  int
	Accept func(context.Context) (*net.UnixConn, error)
}

// AcceptReaderSession attaches a supervisor-started consumer to one fixed,
// controller-installed grant. A nonnil selector chooses latest delivery;
// nil chooses ROI. Caller owns a returned resource even on partial failure and
// must Close/WaitClosed it. The dispatcher/control owner outlives the session.
// This method does not serialize all readers through the control command loop.
// Selection occurs AFTER accept against the exact current grant binding, not
// before blocking in accept or by the source name supplied by a reader.
func (d *ControlDispatcher) AcceptReaderSession(ctx context.Context, grant string, selectDelivery func(fd.BindingRef) (*DeliveryPublication, error),
	plan ReaderAcceptPlan, maxBytes int, maxWorkNS uint64) (session *FrameSession, transport *fd.Resource[peer.Connection], err error) {
	if ctx == nil || d == nil || d.control == nil || !controlToken(grant) || plan.Accept == nil || plan.Bytes < 1 ||
		maxBytes < 1 || maxBytes > 1<<28 || maxWorkNS == 0 || maxWorkNS > MaxFrameRequestBudgetNS {
		return nil, nil, peer.ErrInvalid
	}
	if d.State().Closed {
		return nil, nil, peer.ErrRetired
	}
	var p *DeliveryPublication
	transport, err = fd.OwnRuntimeResource(ctx, d.control.runtime, fd.ResourcePlan[peer.Connection]{Bytes: plan.Bytes,
		Create: func(ctx context.Context) (fd.Created[peer.Connection], error) {
			conn, acceptErr := plan.Accept(ctx)
			if conn == nil {
				if acceptErr == nil {
					acceptErr = peer.ErrUnavailable
				}
				return fd.Created[peer.Connection]{}, acceptErr
			}
			// Even accept/hello failure transfers the returned socket to this
			// one resource owner. Do not hide an uncertain close behind an error.
			owner := &acceptedReaderOwner{conn: conn}
			created := fd.Created[peer.Connection]{Value: owner, Owned: true}
			if acceptErr != nil {
				return created, acceptErr
			}
			helloCtx, cancel := context.WithTimeout(ctx, time.Duration(maxWorkNS))
			defer cancel()
			err := d.control.WithConsumer(grant, func(access *Access, owned OwnedConsumer) error {
				consumer, ok := owned.(*linux.Lease)
				if !ok {
					return peer.ErrUnavailable
				}
				if selectDelivery != nil {
					var err error
					p, err = selectDelivery(access.binding)
					if err != nil {
						return err
					}
					if p == nil || p.binding == nil || p.binding.Reference() != access.binding || p.fence != d.control.runtime.Fence() || int(p.layout.MaxPayload) > maxBytes {
						return peer.ErrInvalid
					}
					if err := access.authorizePublication(p); err != nil {
						return err
					}
				}
				var err error
				owner.connection, err = linux.AttachStartedConsumer(helloCtx, conn, consumer, time.Duration(maxWorkNS))
				return err
			})
			return created, err
		}, Release: func(c peer.Connection) error { return c.Close() },
	})
	if err != nil {
		return nil, transport, err
	}
	if p != nil {
		session, err = NewDeliverySession(ctx, d.control.runtime, d.control, grant, transport, p, maxWorkNS)
	} else {
		session, err = NewFrameSession(ctx, d.control.runtime, d.control, grant, transport, maxBytes, maxWorkNS)
	}
	if err != nil {
		transport.Close()
		return nil, transport, errors.Join(err, transport.WaitClosed(ctx))
	}
	// Successful FrameSession now solely owns/retire-closes this resource.
	return session, nil, nil
}

type acceptedReaderOwner struct {
	conn       *net.UnixConn
	connection *linux.AttachedConnection
}

func (c *acceptedReaderOwner) CurrentGeneration() (string, error) {
	if c.connection == nil {
		return "", peer.ErrRetired
	}
	return c.connection.CurrentGeneration()
}
func (c *acceptedReaderOwner) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	if c.connection == nil {
		return 0, peer.Principal{}, peer.ErrRetired
	}
	return c.connection.Receive(ctx, p)
}
func (c *acceptedReaderOwner) WritePacket(ctx context.Context, p []byte) error {
	if c.connection == nil {
		return peer.ErrRetired
	}
	return c.connection.WritePacket(ctx, p)
}
func (c *acceptedReaderOwner) Close() error { return c.conn.Close() }
