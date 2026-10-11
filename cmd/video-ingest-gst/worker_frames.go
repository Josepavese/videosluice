package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

type gstreamerCodecBootstrap struct {
	vps bool
	sps bool
	pps bool
	idr bool
}

func (bootstrap *gstreamerCodecBootstrap) observe(codec string, payload []byte) {
	for _, nal := range gstreamerAnnexBNALUnits(payload) {
		if len(nal) == 0 {
			continue
		}
		switch codec {
		case "h264":
			switch nal[0] & 0x1f {
			case 5:
				bootstrap.idr = true
			case 7:
				bootstrap.sps = true
			case 8:
				bootstrap.pps = true
			}
		case "h265":
			switch (nal[0] >> 1) & 0x3f {
			case 16, 17, 18, 19, 20, 21, 22, 23:
				bootstrap.idr = true
			case 32:
				bootstrap.vps = true
			case 33:
				bootstrap.sps = true
			case 34:
				bootstrap.pps = true
			}
		}
	}
}

func (bootstrap gstreamerCodecBootstrap) complete(codec string) bool {
	switch codec {
	case "h264":
		return bootstrap.sps && bootstrap.pps && bootstrap.idr
	case "h265":
		return bootstrap.vps && bootstrap.sps && bootstrap.pps && bootstrap.idr
	default:
		return false
	}
}

func gstreamerAnnexBNALUnits(payload []byte) [][]byte {
	type boundary struct {
		start int
		nal   int
	}
	boundaries := make([]boundary, 0, 8)
	for offset := 0; offset+3 <= len(payload); {
		length := 0
		if offset+4 <= len(payload) && payload[offset] == 0 && payload[offset+1] == 0 &&
			payload[offset+2] == 0 && payload[offset+3] == 1 {
			length = 4
		} else if payload[offset] == 0 && payload[offset+1] == 0 && payload[offset+2] == 1 {
			length = 3
		}
		if length == 0 {
			offset++
			continue
		}
		boundaries = append(boundaries, boundary{start: offset, nal: offset + length})
		offset += length
	}
	units := make([][]byte, 0, len(boundaries))
	for index, entry := range boundaries {
		end := len(payload)
		if index+1 < len(boundaries) {
			end = boundaries[index+1].start
		}
		if entry.nal < end {
			units = append(units, payload[entry.nal:end])
		}
	}
	return units
}

type gstreamerFrameRouter struct {
	mu              sync.Mutex
	writer          *muxWriter
	active          map[string]*isolatedGStreamerRole
	qualityRequests <-chan qualityRequest
}

func newGStreamerFrameRouter(
	writer *muxWriter,
	qualityRequests <-chan qualityRequest,
) *gstreamerFrameRouter {
	return &gstreamerFrameRouter{
		writer: writer, active: make(map[string]*isolatedGStreamerRole),
		qualityRequests: qualityRequests,
	}
}

func (router *gstreamerFrameRouter) setInitial(roles []*isolatedGStreamerRole) error {
	if router == nil || router.writer == nil {
		return errors.New("GStreamer frame router is unavailable")
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	for _, role := range roles {
		if role == nil || role.role == "" || router.active[role.role] != nil {
			return errors.New("GStreamer initial role set is invalid")
		}
		router.active[role.role] = role
	}
	return nil
}

func (router *gstreamerFrameRouter) dispatch(role *isolatedGStreamerRole, payload []byte) error {
	if router == nil || role == nil {
		return errors.New("GStreamer frame dispatch is invalid")
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if router.active[role.role] != role {
		return nil
	}
	if role.role == "analysis" && router.qualityRequests != nil {
		select {
		case request := <-router.qualityRequests:
			request.response <- measureNV12Luma(
				payload, role.cfg.subWidth, role.cfg.subHeight, role.cfg.qualityStride)
		default:
		}
	}
	return router.writer.write(role.stream, payload)
}

func (router *gstreamerFrameRouter) activeRole(role string) *isolatedGStreamerRole {
	if router == nil {
		return nil
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	return router.active[role]
}

func (router *gstreamerFrameRouter) switchRoles(
	replacements map[string]*isolatedGStreamerRole,
) error {
	if router == nil || len(replacements) == 0 {
		return errors.New("GStreamer role switch is empty")
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	for role, replacement := range replacements {
		switch role {
		case "analysis", "archive", "live":
		default:
			return fmt.Errorf("unsupported GStreamer role %q", role)
		}
		if replacement != nil && replacement.role != role {
			return fmt.Errorf("GStreamer role replacement %q has foreign identity", role)
		}
	}
	for role, replacement := range replacements {
		if replacement == nil {
			delete(router.active, role)
		} else {
			router.active[role] = replacement
		}
	}
	return nil
}

func (role *isolatedGStreamerRole) observeAndDispatch(
	router *gstreamerFrameRouter,
	payload []byte,
) error {
	if role == nil || router == nil {
		return errors.New("GStreamer role frame path is unavailable")
	}
	role.mu.Lock()
	role.frameID++
	if role.codec != "" {
		role.bootstrap.observe(role.codec, payload)
	}
	role.mu.Unlock()
	select {
	case role.frameChanged <- struct{}{}:
	default:
	}
	return router.dispatch(role, payload)
}

func (role *isolatedGStreamerRole) evidence() (videoproviders.WorkerFanoutBranchReceipt, bool) {
	if role == nil {
		return videoproviders.WorkerFanoutBranchReceipt{}, false
	}
	role.mu.Lock()
	defer role.mu.Unlock()
	if role.frameID == 0 {
		return videoproviders.WorkerFanoutBranchReceipt{}, false
	}
	bootstrap := videoproviders.WorkerFanoutBootstrapNotApplicable
	if role.codec != "" {
		if !role.bootstrap.complete(role.codec) {
			return videoproviders.WorkerFanoutBranchReceipt{}, false
		}
		bootstrap = videoproviders.WorkerFanoutBootstrapComplete
	}
	return videoproviders.WorkerFanoutBranchReceipt{
		Role: role.role, BranchEpoch: role.branchEpoch,
		FrameID: role.frameID, CodecBootstrap: bootstrap, RuntimeMode: role.runtimeMode,
	}, true
}

func (role *isolatedGStreamerRole) frameCount() uint64 {
	if role == nil {
		return 0
	}
	role.mu.Lock()
	defer role.mu.Unlock()
	return role.frameID
}

func (role *isolatedGStreamerRole) noteForwardExit(err error) {
	role.mu.Lock()
	role.forwardErr = err
	role.mu.Unlock()
	role.forwardOnce.Do(func() { close(role.forwardDone) })
	select {
	case role.frameChanged <- struct{}{}:
	default:
	}
}

func (role *isolatedGStreamerRole) forwardResult() (error, bool) {
	if role == nil {
		return errors.New("GStreamer role is unavailable"), true
	}
	select {
	case <-role.forwardDone:
		role.mu.Lock()
		defer role.mu.Unlock()
		if role.forwardErr == nil {
			return errors.New("GStreamer role exited cleanly"), true
		}
		return role.forwardErr, true
	default:
		return nil, false
	}
}

func (role *isolatedGStreamerRole) waitEvidence(
	ctx context.Context,
	minimumFrame uint64,
) (videoproviders.WorkerFanoutBranchReceipt, error) {
	for {
		if err, exited := role.forwardResult(); exited {
			return videoproviders.WorkerFanoutBranchReceipt{}, err
		}
		if evidence, ready := role.evidence(); ready && evidence.FrameID > minimumFrame {
			return evidence, nil
		}
		select {
		case <-ctx.Done():
			return videoproviders.WorkerFanoutBranchReceipt{}, ctx.Err()
		case <-role.frameChanged:
		}
	}
}

func startGStreamerRoleForwarder(
	ctx context.Context,
	role *isolatedGStreamerRole,
	router *gstreamerFrameRouter,
	warmupUntil time.Time,
) {
	go func() {
		var err error
		if role.role == "analysis" {
			err = forwardNV12(role.reader, role.cfg, warmupUntil, func(payload []byte) error {
				return role.observeAndDispatch(router, payload)
			})
		} else {
			err = forwardRTP(role.reader, role.codec, warmupUntil, func(payload []byte) error {
				return role.observeAndDispatch(router, payload)
			})
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		role.noteForwardExit(err)
	}()
}
