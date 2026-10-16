package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Josepavese/videosluice/quality"
)

const (
	qualityObservationInterval = time.Second
	// Control-plane metadata only, never pixels. Current producers emit a
	// fixed 4x3 grid in each of three domains; this is a generous read budget.
	maxQualityObservationBytes = 1 << 20
)

type qualityObservationKey struct {
	path  string
	epoch uint64
}

// All fields are protected by rt.mu. readFile is a trusted per-instance test/
// filesystem adapter, configured before the worker starts, never an RPC input.
type brokerQualityObservation struct {
	readFile func(string) ([]byte, error)
	key      qualityObservationKey
	state    quality.State
	err      error
	ready    bool
	lastLog  time.Time
}

func (rt *videoBrokerRuntime) qualityKeyLocked() (qualityObservationKey, bool) {
	path := strings.TrimSpace(rt.plan.QualityMonitorStatePath)
	return qualityObservationKey{path, rt.mediaEpoch}, rt.plan.QualityMonitorEnabled && rt.plan.AnalysisEnabled && path != ""
}

func (rt *videoBrokerRuntime) qualityStateLocked(now time.Time) (quality.State, error) {
	key, enabled := rt.qualityKeyLocked()
	observation := &rt.qualityObservation
	if !enabled || !observation.ready || observation.key != key {
		return quality.State{}, errors.New("quality observation pending for current source epoch")
	}
	if observation.err != nil {
		return quality.State{}, observation.err
	}
	// Always evaluate the producer's original timestamp against the caller's
	// current clock; a slow/read-cached sample is never renamed as fresh.
	return validateVideoQualitySignalState(rt.plan, observation.state, now)
}

func readQualityObservation(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxQualityObservationBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxQualityObservationBytes {
		return nil, fmt.Errorf("quality observation exceeds %d-byte metadata budget", maxQualityObservationBytes)
	}
	return data, nil
}

// One reader and one decoded snapshot per source. There is no queue and no
// timeout-spawned replacement: kernel file I/O cannot be cancelled. The normal
// runtime wait group joins this worker on shutdown, retaining real ownership
// until read returns. Cancellation and epoch fencing prohibit late publication.
func (rt *videoBrokerRuntime) observeQuality(ctx context.Context) {
	ticker := time.NewTicker(qualityObservationInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		rt.refreshQualityObservation(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (rt *videoBrokerRuntime) refreshQualityObservation(ctx context.Context) {
	rt.mu.Lock()
	key, enabled := rt.qualityKeyLocked()
	read := rt.qualityObservation.readFile
	rt.mu.Unlock()
	if !enabled || ctx.Err() != nil {
		return
	}
	if read == nil {
		read = readQualityObservation
	}
	started := time.Now()
	data, err := read(key.path)
	readFinished := time.Now()
	stage := "read"
	var state quality.State
	if err == nil && ctx.Err() == nil {
		stage = "decode"
		state, err = quality.Decode(data)
		if err == nil {
			stage = "complete"
		}
	}
	if ctx.Err() != nil {
		return
	}
	finished := time.Now()
	rt.mu.Lock()
	current, enabled := rt.qualityKeyLocked()
	if !enabled || current != key || ctx.Err() != nil {
		rt.mu.Unlock()
		return
	}
	observation := &rt.qualityObservation
	changed := observation.ready && (observation.err != nil) != (err != nil)
	shouldLog := (err != nil || changed || finished.Sub(started) >= time.Second) &&
		(observation.lastLog.IsZero() || finished.Sub(observation.lastLog) >= 30*time.Second)
	observation.key, observation.state, observation.err, observation.ready = key, state, err, true
	if shouldLog {
		observation.lastLog = finished
	}
	rt.mu.Unlock()
	if shouldLog {
		status := "observed"
		if err != nil {
			status = "unavailable"
		}
		// No raw file contents, arbitrary exception text or pixel data.
		log.Printf("[video-broker] event=quality_observation status=%s media_epoch=%d elapsed_ms=%d read_ms=%d decode_us=%d bytes=%d stage=%s", status, key.epoch, finished.Sub(started).Milliseconds(), readFinished.Sub(started).Milliseconds(), finished.Sub(readFinished).Microseconds(), len(data), stage)
	}
}
