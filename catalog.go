package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.TableRenamer = (*Database)(nil)
var _ sql.CollatedDatabase = (*Database)(nil)
var _ sql.ViewDatabase = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.StoredProcedureDatabase = (*Database)(nil)
var _ sql.EventDatabase = (*Database)(nil)

type storedView struct {
	Name    string `json:"name"`
	Select  string `json:"select"`
	Create  string `json:"create"`
	SqlMode string `json:"sqlMode"`
}

// GetCollation implements sql.CollatedDatabase.
func (d *Database) GetCollation(ctx *sql.Context) sql.CollationID {
	collation := sql.Collation_Default
	_ = d.store.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return nil
		}
		raw := bucket.Get(keyCollation)
		if len(raw) == 2 {
			collation = sql.CollationID(binary.BigEndian.Uint16(raw))
		}
		return nil
	})
	if collation == sql.Collation_Unspecified {
		return sql.Collation_Default
	}
	return collation
}

// SetCollation implements sql.CollatedDatabase.
func (d *Database) SetCollation(ctx *sql.Context, collation sql.CollationID) error {
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	return d.store.updateQuery(ctx, func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		var coll [2]byte
		binary.BigEndian.PutUint16(coll[:], uint16(collation))
		return bucket.Put(keyCollation, coll[:])
	})
}

// CreateView implements sql.ViewDatabase.
func (d *Database) CreateView(ctx *sql.Context, name string, selectStatement, createViewStmt string) error {
	views, err := d.readViews()
	if err != nil {
		return err
	}
	for _, view := range views {
		if strings.EqualFold(view.Name, name) {
			return sql.ErrExistingView.New(d.name, name)
		}
	}
	views = append(views, storedView{
		Name:    name,
		Select:  selectStatement,
		Create:  createViewStmt,
		SqlMode: sql.LoadSqlMode(ctx).String(),
	})
	return d.writeViews(views)
}

// DropView implements sql.ViewDatabase.
func (d *Database) DropView(ctx *sql.Context, name string) error {
	views, err := d.readViews()
	if err != nil {
		return err
	}
	next := views[:0]
	found := false
	for _, view := range views {
		if strings.EqualFold(view.Name, name) {
			found = true
			continue
		}
		next = append(next, view)
	}
	if !found {
		return sql.ErrViewDoesNotExist.New(d.name, name)
	}
	return d.writeViews(next)
}

// GetViewDefinition implements sql.ViewDatabase.
func (d *Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	views, err := d.readViews()
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}
	for _, view := range views {
		if strings.EqualFold(view.Name, viewName) {
			return sql.ViewDefinition{
				Name:                view.Name,
				TextDefinition:      view.Select,
				CreateViewStatement: view.Create,
				SqlMode:             view.SqlMode,
			}, true, nil
		}
	}
	return sql.ViewDefinition{}, false, nil
}

// AllViews implements sql.ViewDatabase.
func (d *Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	views, err := d.readViews()
	if err != nil {
		return nil, err
	}
	out := make([]sql.ViewDefinition, len(views))
	for i, view := range views {
		out[i] = sql.ViewDefinition{
			Name:                view.Name,
			TextDefinition:      view.Select,
			CreateViewStatement: view.Create,
			SqlMode:             view.SqlMode,
		}
	}
	return out, nil
}

func (d *Database) readViews() ([]storedView, error) {
	var raw []byte
	err := d.store.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		raw = append([]byte(nil), bucket.Get(keyViews)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var views []storedView
	if err := json.Unmarshal(raw, &views); err != nil {
		return nil, err
	}
	return views, nil
}

func (d *Database) writeViews(views []storedView) error {
	raw, err := json.Marshal(views)
	if err != nil {
		return err
	}
	return d.store.update(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		return bucket.Put(keyViews, raw)
	})
}

// GetTriggers implements sql.TriggerDatabase.
func (d *Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	return d.readTriggers()
}

// CreateTrigger implements sql.TriggerDatabase.
func (d *Database) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	triggers, err := d.readTriggers()
	if err != nil {
		return err
	}
	for _, trigger := range triggers {
		if strings.EqualFold(trigger.Name, definition.Name) {
			return fmt.Errorf("persist: trigger %s already exists", definition.Name)
		}
	}
	triggers = append(triggers, definition)
	return d.writeTriggers(triggers)
}

// DropTrigger implements sql.TriggerDatabase.
func (d *Database) DropTrigger(ctx *sql.Context, name string) error {
	triggers, err := d.readTriggers()
	if err != nil {
		return err
	}
	next := triggers[:0]
	found := false
	for _, trigger := range triggers {
		if strings.EqualFold(trigger.Name, name) {
			found = true
			continue
		}
		next = append(next, trigger)
	}
	if !found {
		return sql.ErrTriggerDoesNotExist.New(name)
	}
	return d.writeTriggers(next)
}

func (d *Database) readTriggers() ([]sql.TriggerDefinition, error) {
	var raw []byte
	err := d.store.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		raw = append([]byte(nil), bucket.Get(keyTriggers)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var triggers []sql.TriggerDefinition
	if err := json.Unmarshal(raw, &triggers); err != nil {
		return nil, err
	}
	return triggers, nil
}

func (d *Database) writeTriggers(triggers []sql.TriggerDefinition) error {
	raw, err := json.Marshal(triggers)
	if err != nil {
		return err
	}
	return d.store.update(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		return bucket.Put(keyTriggers, raw)
	})
}

// GetStoredProcedure implements sql.StoredProcedureDatabase.
func (d *Database) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	procedures, err := d.readProcedures()
	if err != nil {
		return sql.StoredProcedureDetails{}, false, err
	}
	for _, procedure := range procedures {
		if strings.EqualFold(procedure.Name, name) {
			return procedure, true, nil
		}
	}
	return sql.StoredProcedureDetails{}, false, nil
}

// GetStoredProcedures implements sql.StoredProcedureDatabase.
func (d *Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return d.readProcedures()
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase.
func (d *Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	procedures, err := d.readProcedures()
	if err != nil {
		return err
	}
	for _, procedure := range procedures {
		if strings.EqualFold(procedure.Name, spd.Name) {
			return sql.ErrStoredProcedureAlreadyExists.New(spd.Name)
		}
	}
	procedures = append(procedures, spd)
	return d.writeJSON(keyProcedures, procedures)
}

// DropStoredProcedure implements sql.StoredProcedureDatabase.
func (d *Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	procedures, err := d.readProcedures()
	if err != nil {
		return err
	}
	next := procedures[:0]
	found := false
	for _, procedure := range procedures {
		if strings.EqualFold(procedure.Name, name) {
			found = true
			continue
		}
		next = append(next, procedure)
	}
	if !found {
		return sql.ErrStoredProcedureDoesNotExist.New(name)
	}
	return d.writeJSON(keyProcedures, next)
}

func (d *Database) readProcedures() ([]sql.StoredProcedureDetails, error) {
	var procedures []sql.StoredProcedureDetails
	err := d.readJSON(keyProcedures, &procedures)
	return procedures, err
}

// GetEvent implements sql.EventDatabase.
func (d *Database) GetEvent(ctx *sql.Context, name string) (sql.EventDefinition, bool, error) {
	events, err := d.readEvents()
	if err != nil {
		return sql.EventDefinition{}, false, err
	}
	for _, event := range events {
		if strings.EqualFold(event.Name, name) {
			return event, true, nil
		}
	}
	return sql.EventDefinition{}, false, nil
}

// GetEvents implements sql.EventDatabase.
func (d *Database) GetEvents(ctx *sql.Context) ([]sql.EventDefinition, interface{}, error) {
	events, err := d.readEvents()
	return events, nil, err
}

// SaveEvent implements sql.EventDatabase.
func (d *Database) SaveEvent(ctx *sql.Context, ed sql.EventDefinition) (bool, error) {
	events, err := d.readEvents()
	if err != nil {
		return false, err
	}
	for _, event := range events {
		if strings.EqualFold(event.Name, ed.Name) {
			return false, sql.ErrEventAlreadyExists.New(ed.Name)
		}
	}
	events = append(events, ed)
	if err := d.writeJSON(keyEvents, events); err != nil {
		return false, err
	}
	return strings.EqualFold(ed.Status, "ENABLE"), nil
}

// DropEvent implements sql.EventDatabase.
func (d *Database) DropEvent(ctx *sql.Context, name string) error {
	events, err := d.readEvents()
	if err != nil {
		return err
	}
	next := events[:0]
	found := false
	for _, event := range events {
		if strings.EqualFold(event.Name, name) {
			found = true
			continue
		}
		next = append(next, event)
	}
	if !found {
		return sql.ErrEventDoesNotExist.New(name)
	}
	return d.writeJSON(keyEvents, next)
}

// UpdateEvent implements sql.EventDatabase.
func (d *Database) UpdateEvent(ctx *sql.Context, originalName string, ed sql.EventDefinition) (bool, error) {
	events, err := d.readEvents()
	if err != nil {
		return false, err
	}
	found := false
	for i, event := range events {
		if strings.EqualFold(event.Name, originalName) {
			events[i] = ed
			found = true
			break
		}
	}
	if !found {
		return false, sql.ErrEventDoesNotExist.New(originalName)
	}
	if err := d.writeJSON(keyEvents, events); err != nil {
		return false, err
	}
	return strings.EqualFold(ed.Status, "ENABLE"), nil
}

// UpdateLastExecuted implements sql.EventDatabase.
func (d *Database) UpdateLastExecuted(ctx *sql.Context, eventName string, lastExecuted time.Time) error {
	events, err := d.readEvents()
	if err != nil {
		return err
	}
	for i, event := range events {
		if strings.EqualFold(event.Name, eventName) {
			events[i].LastExecuted = lastExecuted.UTC()
			return d.writeJSON(keyEvents, events)
		}
	}
	return sql.ErrEventDoesNotExist.New(eventName)
}

// NeedsToReloadEvents implements sql.EventDatabase.
func (d *Database) NeedsToReloadEvents(ctx *sql.Context, token interface{}) (bool, error) {
	return false, nil
}

func (d *Database) readEvents() ([]sql.EventDefinition, error) {
	var events []sql.EventDefinition
	err := d.readJSON(keyEvents, &events)
	return events, err
}

func (d *Database) readJSON(key []byte, dest interface{}) error {
	var raw []byte
	err := d.store.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		raw = append([]byte(nil), bucket.Get(key)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return err
	}
	return json.Unmarshal(raw, dest)
}

func (d *Database) writeJSON(key []byte, value interface{}) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return d.store.update(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		return bucket.Put(key, raw)
	})
}

func (d *Database) readForeignKeys() ([]sql.ForeignKeyConstraint, error) {
	var raw []byte
	err := d.store.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		raw = append([]byte(nil), bucket.Get(keyForeignKeys)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var keys []sql.ForeignKeyConstraint
	if err := json.Unmarshal(raw, &keys); err != nil {
		return nil, err
	}
	return keys, nil
}

func (d *Database) writeForeignKeys(keys []sql.ForeignKeyConstraint) error {
	raw, err := json.Marshal(keys)
	if err != nil {
		return err
	}
	return d.store.update(func(tx *kvTx) error {
		bucket := databaseBucket(tx, d.name)
		if bucket == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		return bucket.Put(keyForeignKeys, raw)
	})
}
