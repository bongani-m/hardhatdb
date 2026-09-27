package persist

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// persistHarness runs enginetest against a fresh Badger directory per engine.
type persistHarness struct {
	setupData []setup.SetupScript
	store     *Store
	session   *Session
}

func newPersistHarness() *persistHarness {
	return &persistHarness{}
}

func (h *persistHarness) Setup(scripts ...[]setup.SetupScript) {
	h.setupData = nil
	for i := range scripts {
		h.setupData = append(h.setupData, scripts[i]...)
	}
}

func (h *persistHarness) NewContext() *sql.Context {
	if h.session == nil {
		h.session = NewSession(enginetest.NewBaseSession(), h.store)
	}
	return sql.NewContext(context.Background(), sql.WithSession(h.session))
}

func (h *persistHarness) NewSession() *sql.Context {
	h.session = NewSession(enginetest.NewBaseSession(), h.store)
	return h.NewContext()
}

func (h *persistHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	if h.store != nil {
		_ = h.store.Close()
		h.store = nil
	}
	h.session = nil
	store, err := Open(filepath.Join(t.TempDir(), "gms.db"))
	if err != nil {
		return nil, err
	}
	h.store = store
	t.Cleanup(func() { _ = store.Close() })

	engine, err := enginetest.NewEngine(t, h, store, h.setupData, memory.NewStatsProv())
	if err != nil {
		return nil, err
	}
	// Match the memory harness: drop setup session state so each query starts clean.
	h.session = nil
	return engine, nil
}

func TestEngineQueries(t *testing.T) {
	enginetest.TestQueries(t, newPersistHarness())
}

func TestEngineJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, newPersistHarness())
}

func TestEngineInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, newPersistHarness())
}

func TestEngineUpdate(t *testing.T) {
	enginetest.TestUpdate(t, newPersistHarness())
}

func TestEngineDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, newPersistHarness())
}

func TestEngineReplaceInto(t *testing.T) {
	enginetest.TestReplaceInto(t, newPersistHarness())
}

func TestEngineTruncate(t *testing.T) {
	enginetest.TestTruncate(t, newPersistHarness())
}

func TestEngineScripts(t *testing.T) {
	enginetest.TestScripts(t, newPersistHarness())
}

func TestEngineViews(t *testing.T) {
	enginetest.TestViews(t, newPersistHarness())
}

func TestEngineCreateTable(t *testing.T) {
	enginetest.TestCreateTable(t, newPersistHarness())
}

func TestEngineDropTable(t *testing.T) {
	enginetest.TestDropTable(t, newPersistHarness())
}

func TestEngineInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, newPersistHarness())
}

func TestEngineQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, newPersistHarness())
}

func TestEngineForeignKeys(t *testing.T) {
	enginetest.TestForeignKeys(t, newPersistHarness())
}

func TestEngineIndexes(t *testing.T) {
	enginetest.TestIndexes(t, newPersistHarness())
}

func TestEngineFulltextIndexes(t *testing.T) {
	enginetest.TestFulltextIndexes(t, newPersistHarness())
}

func TestEngineTriggers(t *testing.T) {
	enginetest.TestTriggers(t, newPersistHarness())
}

func TestEngineStoredProcedures(t *testing.T) {
	enginetest.TestStoredProcedures(t, newPersistHarness())
}

func TestEngineColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, newPersistHarness())
}

func TestEngineJsonScripts(t *testing.T) {
	enginetest.TestJsonScripts(t, newPersistHarness(), nil)
}
