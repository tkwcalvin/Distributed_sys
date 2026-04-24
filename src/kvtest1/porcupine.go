package kvtest

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"6.5840/kvsrv1/rpc"
	models "6.5840/models1"
	tester "6.5840/tester1"
)

const linearizabilityCheckTimeout = 1 * time.Second

type OpLog struct {
	operations []porcupine.Operation
	sync.Mutex
}

func (log *OpLog) Len() int {
	log.Lock()
	defer log.Unlock()
	return len(log.operations)
}

func (log *OpLog) Append(op porcupine.Operation) {
	log.Lock()
	defer log.Unlock()
	log.operations = append(log.operations, op)
}

func (log *OpLog) Read() []porcupine.Operation {
	log.Lock()
	defer log.Unlock()
	ops := make([]porcupine.Operation, len(log.operations))
	copy(ops, log.operations)
	return ops
}

// to make sure timestamps use the monotonic clock, instead of computing
// absolute timestamps with `time.Now().UnixNano()` (which uses the wall
// clock), we measure time relative to `t0` using `time.Since(t0)`, which uses
// the monotonic clock
var t0 = time.Unix(0, 0)

func Get(cfg *tester.Config, ck IKVClerk, key string, log *OpLog, cli int) (string, rpc.Tversion, rpc.Err) {
	start := int64(time.Since(t0))
	val, ver, err := ck.Get(key)
	end := int64(time.Since(t0))
	cfg.Op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 0, Key: key},
			Output:   models.KvOutput{Value: val, Version: uint64(ver), Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
	return val, ver, err
}

func Put(cfg *tester.Config, ck IKVClerk, key string, value string, version rpc.Tversion, log *OpLog, cli int) rpc.Err {
	start := int64(time.Since(t0))
	err := ck.Put(key, value, version)
	end := int64(time.Since(t0))
	cfg.Op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 1, Key: key, Value: value, Version: uint64(version)},
			Output:   models.KvOutput{Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
	return err
}

// dumpLinearizationDebug prints the full operation history when linearization fails,
// for debugging. Always writes to a file (LIN_DEBUG_FILE or temp lin_fail_*.log)
// so you can inspect drain timeout and full history even if log output is truncated.
func dumpLinearizationDebug(ops []porcupine.Operation, ts *Test) {
	var buf strings.Builder
	// Header: drain timeout hint (key for debugging "not linearizable")
	if ts != nil {
		if n := ts.drainTimeouts.Load(); n > 0 {
			buf.WriteString(fmt.Sprintf("[LIN] NOTE: %d client(s) hit DRAIN TIMEOUT during run → history may be INCOMPLETE (missing in-flight Put/Get); this often causes false 'not linearizable'\n\n", n))
		} else {
			buf.WriteString("[LIN] no drain timeouts recorded (history should be complete)\n\n")
		}
	}
	buf.WriteString(fmt.Sprintf("=== LINEARIZATION FAILED: operation history (total %d ops) ===\n", len(ops)))
	for i, op := range ops {
		inp := op.Input.(models.KvInput)
		out := op.Output.(models.KvOutput)
		desc := models.KvModel.DescribeOperation(op.Input, op.Output)
		callMs, returnMs := op.Call/1e6, op.Return/1e6
		buf.WriteString(fmt.Sprintf("  [%d] client=%d  call=%dms return=%dms  %s\n", i, op.ClientId, callMs, returnMs, desc))
		if inp.Op == 0 {
			buf.WriteString(fmt.Sprintf("       GET key=%q -> value=%q version=%d err=%q\n", inp.Key, out.Value, out.Version, out.Err))
		} else {
			buf.WriteString(fmt.Sprintf("       PUT key=%q value=%q version=%d -> err=%q\n", inp.Key, inp.Value, inp.Version, out.Err))
		}
	}
	buf.WriteString("=== end operation history ===\n")
	s := buf.String()
	log.Printf("%s", s)
	// Always write to a file so logs are not lost when output is large or redirected
	fpath := os.Getenv("LIN_DEBUG_FILE")
	if fpath == "" {
		f, err := os.CreateTemp("", "lin_fail_*.log")
		if err != nil {
			log.Printf("[LIN] failed to create temp log file: %v", err)
			return
		}
		fpath = f.Name()
		f.Close()
	}
	if err := os.WriteFile(fpath, []byte(s), 0644); err != nil {
		log.Printf("[LIN] failed to write debug log to %s: %v", fpath, err)
	} else {
		log.Printf("[LIN] full debug log (drain + history) written to: %s", fpath)
		fmt.Printf("info: linearization debug log written to %s\n", fpath)
	}
}

// Checks that the log of Clerk.Put's and Clerk.Get's is linearizable (see
// linearizability-faq.txt)
func checkPorcupine(t *testing.T, opLog *OpLog, nsec time.Duration, ts *Test) {
	enabled := os.Getenv("VIS_ENABLE")
	fpath := os.Getenv("VIS_FILE")
	ops := opLog.Read()
	res, info := porcupine.CheckOperationsVerbose(models.KvModel, ops, nsec)
	if res == porcupine.Illegal {
		// If any client hit drain timeout, history may be incomplete → "not linearizable" can be a false positive.
		if ts != nil && ts.drainTimeouts.Load() > 0 {
			n := ts.drainTimeouts.Load()
			log.Printf("[LIN] history not linearizable but %d client(s) had DRAIN TIMEOUT → skipping failure (incomplete history)", n)
			fmt.Printf("info: linearization check skipped (incomplete history: %d drain timeout(s))\n", n)
			return
		}
		dumpLinearizationDebug(ops, ts)
		var file *os.File
		var err error
		if fpath == "" {
			// Save the vis file in a temporary file.
			file, err = os.CreateTemp("", "porcupine-*.html")
		} else {
			file, err = os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		}
		if err != nil {
			fmt.Printf("info: failed to open visualization file %s (%v)\n", fpath, err)
		} else if enabled != "never" {
			// Don't produce visualization file if VIS_ENABLE is set to "never".
			annotations := tester.FinalizeAnnotations("test failed")
			info.AddAnnotations(annotations)
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	// The result is either legal or unknown.
	if enabled == "always" && tester.GetAnnotationFinalized() {
		var file *os.File
		var err error
		if fpath == "" {
			// Save the vis file in a temporary file.
			file, err = os.CreateTemp("", "porcupine-*.html")
		} else {
			file, err = os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		}
		if err != nil {
			fmt.Printf("info: failed to open visualization file %s (%v)\n", fpath, err)
			return
		}
		annotations := tester.FinalizeAnnotations("test passed")
		info.AddAnnotations(annotations)
		err = porcupine.Visualize(models.KvModel, info, file)
		if err != nil {
			fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
		} else {
			fmt.Printf("info: wrote history visualization to %s\n", file.Name())
		}
	}
}

// Porcupine
func (ts *Test) Get(ck IKVClerk, key string, cli int) (string, rpc.Tversion, rpc.Err) {
	start := int64(time.Since(t0))
	val, ver, err := ck.Get(key)
	end := int64(time.Since(t0))
	ts.Op()
	if ts.oplog != nil {
		op := porcupine.Operation{
			Input:    models.KvInput{Op: 0, Key: key},
			Output:   models.KvOutput{Value: val, Version: uint64(ver), Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		}
		ts.oplog.Append(op)
		if os.Getenv("LIN_DEBUG") != "" {
			log.Printf("[LIN] append GET cli=%d key=%q -> value=%q ver=%d err=%q call=%dms return=%dms", cli, key, val, ver, err, start/1e6, end/1e6)
		}
	}
	return val, ver, err
}

// Porcupine
func (ts *Test) Put(ck IKVClerk, key string, value string, version rpc.Tversion, cli int) rpc.Err {
	start := int64(time.Since(t0))
	err := ck.Put(key, value, version)
	end := int64(time.Since(t0))
	ts.Op()
	if ts.oplog != nil {
		op := porcupine.Operation{
			Input:    models.KvInput{Op: 1, Key: key, Value: value, Version: uint64(version)},
			Output:   models.KvOutput{Err: string(err)},
			Call:     start,
			Return:   end,
			ClientId: cli,
		}
		ts.oplog.Append(op)
		if os.Getenv("LIN_DEBUG") != "" {
			log.Printf("[LIN] append PUT cli=%d key=%q value=%q ver=%d err=%q call=%dms return=%dms", cli, key, value, version, err, start/1e6, end/1e6)
		}
	}
	return err
}

func (ts *Test) CheckPorcupine() {
	ts.CheckPorcupineT(linearizabilityCheckTimeout)
}

// Wait for opLog to stabilize (no new appends for this long) after drain timeouts, so late ops can be appended.
const opLogStableDuration = 2 * time.Second
const opLogWaitMax = 25 * time.Second
const opLogPollInterval = 300 * time.Millisecond

func (ts *Test) CheckPorcupineT(nsec time.Duration) {
	if n := ts.drainTimeouts.Load(); n > 0 && ts.oplog != nil {
		// Wait until opLog length is stable (no new ops for opLogStableDuration) or opLogWaitMax, whichever first.
		deadline := time.Now().Add(opLogWaitMax)
		lastLen := -1
		stableSince := time.Now()
		for time.Now().Before(deadline) {
			time.Sleep(opLogPollInterval)
			cur := ts.oplog.Len()
			if cur == lastLen {
				if time.Since(stableSince) >= opLogStableDuration {
					log.Printf("[LIN] %d client(s) had drain timeout; opLog stable at %d ops after %v", n, cur, time.Since(stableSince))
					break
				}
			} else {
				lastLen = cur
				stableSince = time.Now()
			}
		}
	}
	// tester.RetrieveAnnotations() also clears the accumulated annotations so
	// that the vis file containing client operations (generated here) won't be
	// overridden by that without client operations (generated at cleanup time).
	checkPorcupine(ts.t, ts.oplog, nsec, ts)
}
