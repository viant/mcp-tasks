// Package store provides a durable reference backend. Hosts with an existing
// run store should implement tasks.Store against that canonical store instead.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	tasks "github.com/viant/mcp-tasks"
	"io"
	"os"
	"path/filepath"
	"time"
)

type File struct {
	directory  string
	maxRecords int
}

// Open uses a dedicated local directory. Every operation takes a process-safe
// directory lock. Durability assumes a filesystem honoring rename and fsync.
func Open(directory string, maxRecords int) (*File, error) {
	if directory == "" || maxRecords < 1 {
		return nil, errors.New("storage directory and positive capacity required")
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("task storage must be a real directory")
	}
	return &File{directory: directory, maxRecords: maxRecords}, nil
}
func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func (s *File) path(owner tasks.Owner, id string) string {
	key, _ := json.Marshal([]string{owner.Tenant, owner.Subject, id})
	return filepath.Join(s.directory, digest(string(key))+".json")
}
func (s *File) lock(ctx context.Context) (*flock.Flock, error) {
	lock := flock.New(filepath.Join(s.directory, ".lock"), flock.SetPermissions(0600))
	held, err := lock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return nil, err
	}
	if !held {
		return nil, ctx.Err()
	}
	return lock, nil
}
func validate(record tasks.Record) error {
	if !record.Owner.Valid() || record.Revision == 0 || record.OriginMethod != tasks.MethodCallTool {
		return errors.New("invalid private task record")
	}
	return record.State.Validate()
}
func (s *File) read(owner tasks.Owner, id string) (tasks.Record, error) {
	var record tasks.Record
	if !owner.Valid() || !tasks.ValidID(id) {
		return record, tasks.ErrNotFound
	}
	file, err := os.Open(s.path(owner, id))
	if errors.Is(err, os.ErrNotExist) {
		return record, tasks.ErrNotFound
	}
	if err != nil {
		return record, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, tasks.MaxJSONBytes*4+1))
	if err != nil {
		return record, err
	}
	if len(raw) > tasks.MaxJSONBytes*4 {
		return record, errors.New("stored task exceeds budget")
	}
	if err = json.Unmarshal(raw, &record); err != nil {
		return record, err
	}
	if record.Owner != owner || record.State.TaskID != id {
		return tasks.Record{}, tasks.ErrNotFound
	}
	return record, validate(record)
}
func (s *File) write(path string, record tasks.Record) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw) > tasks.MaxJSONBytes*4 {
		return errors.New("private task record exceeds budget")
	}
	file, err := os.CreateTemp(s.directory, ".commit-")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if _, err = file.Write(raw); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	return s.syncDirectory()
}
func (s *File) syncDirectory() error {
	directory, err := os.Open(s.directory)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
func (s *File) Create(ctx context.Context, record tasks.Record) error {
	if err := validate(record); err != nil {
		return err
	}
	lock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	path := s.path(record.Owner, record.State.TaskID)
	if _, err = os.Stat(path); err == nil {
		return tasks.ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	files, err := filepath.Glob(filepath.Join(s.directory, "*.json"))
	if err != nil {
		return err
	}
	if len(files) >= s.maxRecords {
		return errors.New("task storage capacity reached")
	}
	return s.write(path, record)
}
func (s *File) Get(ctx context.Context, owner tasks.Owner, id string) (tasks.Record, error) {
	lock, err := s.lock(ctx)
	if err != nil {
		return tasks.Record{}, err
	}
	defer lock.Unlock()
	if err = ctx.Err(); err != nil {
		return tasks.Record{}, err
	}
	return s.read(owner, id)
}
func (s *File) CompareAndSwap(ctx context.Context, owner tasks.Owner, id string, revision uint64, next tasks.Record) error {
	if err := validate(next); err != nil {
		return err
	}
	if next.Owner != owner || next.State.TaskID != id || next.Revision != revision+1 || revision == ^uint64(0) {
		return errors.New("invalid task revision update")
	}
	lock, err := s.lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	current, err := s.read(owner, id)
	if err != nil {
		return err
	}
	if current.Revision != revision {
		return tasks.ErrConflict
	}
	if !next.State.CreatedAt.Equal(current.State.CreatedAt) || next.OriginMethod != current.OriginMethod {
		return errors.New("task origin is immutable")
	}
	if current.State.Status.Terminal() {
		before, _ := json.Marshal(struct {
			Status tasks.Status
			Result json.RawMessage
			Error  *tasks.RPCError
		}{current.State.Status, current.State.Result, current.State.Error})
		after, _ := json.Marshal(struct {
			Status tasks.Status
			Result json.RawMessage
			Error  *tasks.RPCError
		}{next.State.Status, next.State.Result, next.State.Error})
		if string(before) != string(after) {
			return tasks.ErrTerminal
		}
	}
	if current.CancelRequested && !next.CancelRequested {
		return errors.New("cancellation intent cannot be cleared")
	}
	if string(current.Origin) != string(next.Origin) {
		return errors.New("private task origin is immutable")
	}
	for key, value := range current.Issued {
		a, _ := json.Marshal(value)
		b, _ := json.Marshal(next.Issued[key])
		if string(a) != string(b) {
			return tasks.ErrKeyReuse
		}
	}
	for key, value := range current.Responses {
		if string(value) != string(next.Responses[key]) {
			return errors.New("accepted input cannot be changed")
		}
	}
	return s.write(s.path(owner, id), next)
}

// Sweep explicitly discards elapsed-TTL records. It never stops/restarts work.
// Execution retention and cancellation are owned by the native runtime adapter.
func (s *File) Sweep(ctx context.Context, now time.Time) (int, error) {
	lock, err := s.lock(ctx)
	if err != nil {
		return 0, err
	}
	defer lock.Unlock()
	files, err := filepath.Glob(filepath.Join(s.directory, "*.json"))
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, path := range files {
		if err = ctx.Err(); err != nil {
			return removed, err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return removed, err
		}
		var record tasks.Record
		if err = json.Unmarshal(raw, &record); err != nil {
			return removed, fmt.Errorf("invalid stored task: %w", err)
		}
		if record.State.Expired(now) {
			if err = os.Remove(path); err != nil {
				return removed, err
			}
			removed++
		}
	}
	if removed > 0 {
		err = s.syncDirectory()
	}
	return removed, err
}
