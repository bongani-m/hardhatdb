package sqle

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// hardhatdbHarness runs enginetest against a fresh Badger directory per engine.
type hardhatdbHarness struct {
	setupData []setup.SetupScript
	store     *Store
	session   *Session
}

func newHardhatdbHarness() *hardhatdbHarness {
	return &hardhatdbHarness{}
}

func (h *hardhatdbHarness) Setup(scripts ...[]setup.SetupScript) {
	h.setupData = nil
	for i := range scripts {
		h.setupData = append(h.setupData, scripts[i]...)
	}
}

func (h *hardhatdbHarness) NewContext() *sql.Context {
	if h.session == nil {
		h.session = NewSession(enginetest.NewBaseSession(), h.store)
	}
	return sql.NewContext(context.Background(), sql.WithSession(h.session))
}

func (h *hardhatdbHarness) NewSession() *sql.Context {
	h.session = NewSession(enginetest.NewBaseSession(), h.store)
	return h.NewContext()
}

func (h *hardhatdbHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	if h.store != nil {
		_ = h.store.Close()
		h.store = nil
	}
	h.session = nil
	store, err := Open(filepath.Join(t.TempDir(), "hardhatdb.db"))
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
	enginetest.TestQueries(t, newHardhatdbHarness())
}

func TestEngineJoinQueries(t *testing.T) {
	enginetest.TestJoinQueries(t, newHardhatdbHarness())
}

func TestEngineInsertInto(t *testing.T) {
	enginetest.TestInsertInto(t, newHardhatdbHarness())
}

func TestEngineUpdate(t *testing.T) {
	enginetest.TestUpdate(t, newHardhatdbHarness())
}

func TestEngineDeleteFrom(t *testing.T) {
	enginetest.TestDelete(t, newHardhatdbHarness())
}

func TestEngineReplaceInto(t *testing.T) {
	enginetest.TestReplaceInto(t, newHardhatdbHarness())
}

func TestEngineTruncate(t *testing.T) {
	enginetest.TestTruncate(t, newHardhatdbHarness())
}

func TestEngineScripts(t *testing.T) {
	enginetest.TestScripts(t, newHardhatdbHarness())
}

func TestEngineViews(t *testing.T) {
	enginetest.TestViews(t, newHardhatdbHarness())
}

func TestEngineCreateTable(t *testing.T) {
	enginetest.TestCreateTable(t, newHardhatdbHarness())
}

func TestEngineDropTable(t *testing.T) {
	enginetest.TestDropTable(t, newHardhatdbHarness())
}

func TestEngineInfoSchema(t *testing.T) {
	enginetest.TestInfoSchema(t, newHardhatdbHarness())
}

func TestEngineQueryErrors(t *testing.T) {
	enginetest.TestQueryErrors(t, newHardhatdbHarness())
}

func TestEngineForeignKeys(t *testing.T) {
	enginetest.TestForeignKeys(t, newHardhatdbHarness())
}

func TestEngineIndexes(t *testing.T) {
	enginetest.TestIndexes(t, newHardhatdbHarness())
}

func TestEngineFulltextIndexes(t *testing.T) {
	enginetest.TestFulltextIndexes(t, newHardhatdbHarness())
}

func TestEngineTriggers(t *testing.T) {
	enginetest.TestTriggers(t, newHardhatdbHarness())
}

func TestEngineStoredProcedures(t *testing.T) {
	enginetest.TestStoredProcedures(t, newHardhatdbHarness())
}

func TestEngineColumnDefaults(t *testing.T) {
	enginetest.TestColumnDefaults(t, newHardhatdbHarness())
}

func TestEngineJsonScripts(t *testing.T) {
	enginetest.TestJsonScripts(t, newHardhatdbHarness(), nil)
}
