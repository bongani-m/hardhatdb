// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	jsonContentType = "application/json"
	defaultPage     = 1
	defaultSize     = 20
	maxPageSize     = 100
	homeListSize    = 50
	timeLayout      = "2006-01-02T15:04:05.000000Z"
)

// NewHandler serves the cluster page and the accounts JSON API.
func NewHandler(store Store) http.Handler {
	h := &handler{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", h.home)
	mux.HandleFunc("POST /", h.createForm)
	mux.HandleFunc("GET /cluster", h.cluster)
	mux.HandleFunc("GET /accounts", h.list)
	mux.HandleFunc("POST /accounts", h.create)
	mux.HandleFunc("GET /accounts/{id}", h.get)
	mux.HandleFunc("PUT /accounts/{id}", h.update)
	mux.HandleFunc("DELETE /accounts/{id}", h.delete)
	h.mux = mux
	return h
}

type handler struct {
	store Store
	mux   *http.ServeMux
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type accountDoc struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Status    int       `json:"status"`
	Tags      []string  `json:"tags"`
	CreatedAt string    `json:"created_at"`
	Notes     []noteDoc `json:"notes"`
}

type noteDoc struct {
	ID        int64  `json:"id"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
}

type listDoc struct {
	Page     int          `json:"page"`
	Size     int          `json:"size"`
	Total    int          `json:"total"`
	Accounts []accountDoc `json:"accounts"`
}

type insertDoc struct {
	Account   accountDoc  `json:"account"`
	WroteOn   string      `json:"wrote_on"`
	ReadBack  *accountDoc `json:"read_back"`
	OtherNode otherDoc    `json:"other_node"`
}

type otherDoc struct {
	Addr    string      `json:"addr"`
	Account *accountDoc `json:"account"`
}

type clusterDoc struct {
	Nodes []NodeStatus `json:"nodes"`
}

type errorDoc struct {
	Error string `json:"error"`
}

type accountRequest struct {
	Name      string   `json:"name"`
	Email     string   `json:"email"`
	Status    *int     `json:"status"`
	Tags      []string `json:"tags"`
	Note      *string  `json:"note"`
	CreatedAt *string  `json:"created_at"`
	Node      string   `json:"node"`
	ID        *int64   `json:"id"`
}

func (h *handler) home(w http.ResponseWriter, r *http.Request) {
	h.renderHome(w, r, nil, "", http.StatusOK)
}

func (h *handler) createForm(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.renderHome(w, r, nil, "invalid form", http.StatusBadRequest)
		return
	}
	in, err := accountFromForm(r)
	if err != nil {
		h.renderHome(w, r, nil, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := h.store.Insert(r.Context(), in)
	if err != nil {
		h.renderHome(w, r, nil, publicStoreErr(err), statusFor(err))
		return
	}
	h.renderHome(w, r, &result, "", http.StatusOK)
}

func (h *handler) renderHome(w http.ResponseWriter, r *http.Request, result *InsertResult, formErr string, status int) {
	selected := r.URL.Query().Get("node")
	if result != nil && result.WroteOn != "" {
		selected = result.WroteOn
	}
	if selected == "" && r.FormValue("node") != "" {
		selected = r.FormValue("node")
	}
	addrs := h.store.Addrs()
	if selected == "" && len(addrs) > 0 {
		selected = addrs[0]
	}
	data := pageData{
		Nodes:    h.store.Status(r.Context()),
		Addrs:    addrs,
		Selected: selected,
		Result:   result,
		Error:    formErr,
	}
	list, err := h.store.List(r.Context(), Filter{}, 1, homeListSize, "")
	if err != nil && data.Error == "" {
		data.Error = publicStoreErr(err)
		if status == http.StatusOK {
			status = http.StatusInternalServerError
		}
	} else if err == nil {
		data.Accounts = list.Accounts
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := homeTmpl.Execute(w, data); err != nil {
		log.Printf("render page: %v", err)
	}
}

func (h *handler) cluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, clusterDoc{Nodes: h.store.Status(r.Context())})
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	page, size, f, err := parseListQuery(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := h.store.List(r.Context(), f, page, size, r.URL.Query().Get("node"))
	if err != nil {
		h.writeStoreErr(w, err)
		return
	}
	accounts := make([]accountDoc, 0, len(result.Accounts))
	for _, account := range result.Accounts {
		accounts = append(accounts, accountResponse(account))
	}
	writeJSON(w, http.StatusOK, listDoc{Page: page, Size: size, Total: result.Total, Accounts: accounts})
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	account, err := h.store.Get(r.Context(), id, r.URL.Query().Get("node"))
	if err != nil {
		h.writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, accountResponse(account))
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	var req accountRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in, err := accountFromCreate(req)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := h.store.Insert(r.Context(), in)
	if err != nil {
		h.writeStoreErr(w, err)
		return
	}
	w.Header().Set("Location", "/accounts/"+strconv.FormatInt(result.Account.ID, 10))
	writeJSON(w, http.StatusCreated, insertResponse(result))
}

func (h *handler) update(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var req accountRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	account, err := accountFromUpdate(id, req)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	node := r.URL.Query().Get("node")
	if node == "" {
		node = req.Node
	}
	if err := h.store.Update(r.Context(), account, node); err != nil {
		h.writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, accountResponse(account))
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.Delete(r.Context(), id, r.URL.Query().Get("node")); err != nil {
		h.writeStoreErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func accountResponse(a Account) accountDoc {
	notes := make([]noteDoc, 0, len(a.Notes))
	for _, note := range a.Notes {
		notes = append(notes, noteDoc{
			ID:        note.ID,
			Body:      note.Body,
			CreatedAt: note.CreatedAt.UTC().Format(timeLayout),
		})
	}
	return accountDoc{
		ID:        a.ID,
		Name:      a.Name,
		Email:     a.Email,
		Status:    a.Status,
		Tags:      normalizeTags(a.Tags),
		CreatedAt: a.CreatedAt.UTC().Format(timeLayout),
		Notes:     notes,
	}
}

func insertResponse(result InsertResult) insertDoc {
	doc := insertDoc{
		Account: accountResponse(result.Account),
		WroteOn: result.WroteOn,
		OtherNode: otherDoc{
			Addr: result.OtherAddr,
		},
	}
	if result.ReadBack != nil {
		read := accountResponse(*result.ReadBack)
		doc.ReadBack = &read
	}
	if result.Other != nil {
		other := accountResponse(*result.Other)
		doc.OtherNode.Account = &other
	}
	return doc
}

func (h *handler) writeStoreErr(w http.ResponseWriter, err error) {
	h.writeError(w, statusFor(err), publicStoreErr(err))
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func publicStoreErr(err error) string {
	switch {
	case errors.Is(err, ErrNotFound):
		return "not found"
	case errors.Is(err, ErrConflict):
		return "account already exists"
	default:
		log.Printf("store error: %v", err)
		return "internal error"
	}
}

func (h *handler) writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorDoc{Error: msg})
}

func parseID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id < 1 {
		return 0, errors.New("id must be a positive integer")
	}
	return id, nil
}

func parseListQuery(r *http.Request) (int, int, Filter, error) {
	q := r.URL.Query()
	page := defaultPage
	if s := q.Get("page"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return 0, 0, Filter{}, errors.New("page must be an integer >= 1")
		}
		page = n
	}
	size := defaultSize
	if s := q.Get("size"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxPageSize {
			return 0, 0, Filter{}, fmt.Errorf("size must be an integer from 1 to %d", maxPageSize)
		}
		size = n
	}
	if page > (int(^uint(0)>>1))/size {
		return 0, 0, Filter{}, errors.New("page is too large")
	}
	f := Filter{Name: q.Get("name"), Email: q.Get("email"), Tag: q.Get("tag")}
	if s := q.Get("status"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n > 127 {
			return 0, 0, Filter{}, errors.New("status must be an integer from 0 to 127")
		}
		f.Status = &n
	}
	if s := q.Get("created_after"); s != "" {
		t, err := parseFilterTime("created_after", s)
		if err != nil {
			return 0, 0, Filter{}, err
		}
		f.CreatedAfter = &t
	}
	if s := q.Get("created_before"); s != "" {
		t, err := parseFilterTime("created_before", s)
		if err != nil {
			return 0, 0, Filter{}, err
		}
		f.CreatedBefore = &t
	}
	return page, size, f, nil
}

func accountFromCreate(req accountRequest) (InsertInput, error) {
	name := strings.TrimSpace(req.Name)
	email := strings.TrimSpace(req.Email)
	if name == "" || email == "" {
		return InsertInput{}, errors.New("name and email are required")
	}
	if req.Note == nil || strings.TrimSpace(*req.Note) == "" {
		return InsertInput{}, errors.New("note is required")
	}
	created := time.Now().UTC()
	if req.CreatedAt != nil {
		t, err := parseFilterTime("created_at", *req.CreatedAt)
		if err != nil {
			return InsertInput{}, err
		}
		created = t
	}
	status := 1
	if req.Status != nil {
		if *req.Status < 0 || *req.Status > 127 {
			return InsertInput{}, errors.New("status must be an integer from 0 to 127")
		}
		status = *req.Status
	}
	var id int64
	if req.ID != nil {
		if *req.ID < 1 {
			return InsertInput{}, errors.New("id must be a positive integer")
		}
		id = *req.ID
	}
	return InsertInput{
		Name:      name,
		Email:     email,
		Status:    status,
		Tags:      normalizeTags(req.Tags),
		Note:      strings.TrimSpace(*req.Note),
		CreatedAt: created,
		Node:      req.Node,
		ID:        id,
	}, nil
}

func accountFromForm(r *http.Request) (InsertInput, error) {
	note := r.FormValue("note")
	statusText := r.FormValue("status")
	var status *int
	if statusText != "" {
		n, err := strconv.Atoi(statusText)
		if err != nil {
			return InsertInput{}, errors.New("status must be an integer from 0 to 127")
		}
		status = &n
	}
	var created *string
	if value := r.FormValue("created_at"); value != "" {
		created = &value
	}
	var id *int64
	if value := strings.TrimSpace(r.FormValue("id")); value != "" {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 1 {
			return InsertInput{}, errors.New("id must be a positive integer")
		}
		id = &n
	}
	return accountFromCreate(accountRequest{
		Name:      r.FormValue("name"),
		Email:     r.FormValue("email"),
		Status:    status,
		Tags:      splitTags(r.FormValue("tags")),
		Note:      &note,
		CreatedAt: created,
		Node:      r.FormValue("node"),
		ID:        id,
	})
}

func accountFromUpdate(id int64, req accountRequest) (Account, error) {
	name := strings.TrimSpace(req.Name)
	email := strings.TrimSpace(req.Email)
	if name == "" || email == "" {
		return Account{}, errors.New("name and email are required")
	}
	if req.Status == nil || *req.Status < 0 || *req.Status > 127 {
		return Account{}, errors.New("status must be an integer from 0 to 127")
	}
	if req.Tags == nil {
		return Account{}, errors.New("tags is required")
	}
	if req.CreatedAt == nil {
		return Account{}, errors.New("created_at is required")
	}
	created, err := parseFilterTime("created_at", *req.CreatedAt)
	if err != nil {
		return Account{}, err
	}
	return Account{
		ID:        id,
		Name:      name,
		Email:     email,
		Status:    *req.Status,
		Tags:      req.Tags,
		CreatedAt: created,
		Notes:     []Note{},
	}, nil
}

func splitTags(value string) []string {
	if strings.TrimSpace(value) == "" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	tags := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			tags = append(tags, part)
		}
	}
	return tags
}

func parseFilterTime(label, value string) (time.Time, error) {
	t, err := parseTime(value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339 or YYYY-MM-DD HH:MM:SS", label)
	}
	return t, nil
}

func parseTime(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	var last error
	for _, layout := range layouts {
		t, err := time.Parse(layout, value)
		if err == nil {
			return t.UTC(), nil
		}
		last = err
	}
	return time.Time{}, last
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		media, _, err := mime.ParseMediaType(ct)
		if err != nil || media != "application/json" {
			return errors.New("content type must be application/json")
		}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return publicDecodeError(err)
	}
	return nil
}

func publicDecodeError(err error) error {
	var max *http.MaxBytesError
	if errors.As(err, &max) {
		return errors.New("request body is too large")
	}
	if errors.Is(err, io.EOF) {
		return errors.New("request body is required")
	}
	var syn *json.SyntaxError
	if errors.As(err, &syn) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("invalid JSON body")
	}
	var ute *json.UnmarshalTypeError
	if errors.As(err, &ute) && ute.Field != "" {
		return fmt.Errorf("%s has the wrong type", ute.Field)
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

type pageData struct {
	Nodes    []NodeStatus
	Addrs    []string
	Selected string
	Accounts []Account
	Result   *InsertResult
	Error    string
}

var homeTmpl = template.Must(template.New("home").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>HardhatDB</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 52rem; line-height: 1.4; color: #1a1a1a; }
    h1 { margin-bottom: 0.25rem; }
    .lede { color: #444; }
    .nodes { display: flex; gap: 0.75rem; flex-wrap: wrap; margin: 1rem 0 1.5rem; }
    .card { border: 1px solid #ccc; border-radius: 6px; padding: 0.75rem 1rem; min-width: 12rem; }
    .card h2 { font-size: 1rem; margin: 0 0 0.35rem; }
    .card p { margin: 0.15rem 0; }
    .muted { color: #555; }
    .result { background: #f4f7f4; padding: 0.75rem 1rem; border-radius: 6px; }
    .error { color: #8a1f1f; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid #ddd; vertical-align: top; }
    label { display: block; margin-top: 0.75rem; }
    input[type="text"], input[type="email"], input[type="number"] { width: 100%; box-sizing: border-box; padding: 0.4rem; }
    fieldset { margin-top: 1rem; }
    button { margin-top: 1rem; }
  </style>
</head>
<body>
  <h1>HardhatDB</h1>
  <p class="lede">A write to any node is forwarded. The connection that wrote then sees that commit. Another node can still be behind.</p>

  <section class="nodes">
    {{range .Nodes}}
    <article class="card">
      <h2>{{.Addr}}</h2>
      {{if .Err}}<p>down</p>{{else}}
      <p>{{.Role}}{{if .Suffrage}} · {{.Suffrage}}{{end}}</p>
      <p class="muted">leader {{.Leader}}</p>
      <p class="muted">lag {{.Lag}}</p>
      {{end}}
    </article>
    {{end}}
  </section>

  {{if .Error}}<p class="error">{{.Error}}</p>{{end}}
  {{if .Result}}
  <section class="result">
    <h2>Last write</h2>
    <p>Wrote on {{.Result.WroteOn}}.</p>
    <p>Same connection: {{if .Result.ReadBack}}{{.Result.ReadBack.Name}}{{else}}missing{{end}}.</p>
    <p>Other node {{if .Result.OtherAddr}}{{.Result.OtherAddr}}{{else}}none{{end}}: {{if .Result.Other}}{{.Result.Other.Name}}{{else}}missing{{end}}.</p>
  </section>
  {{end}}

  <h2>Accounts</h2>
  <table>
    <thead>
      <tr><th>Name</th><th>Email</th><th>Status</th><th>Tags</th><th>Notes</th></tr>
    </thead>
    <tbody>
      {{range .Accounts}}
      <tr>
        <td>{{.Name}}</td>
        <td>{{.Email}}</td>
        <td>{{.Status}}</td>
        <td>{{range $i, $tag := .Tags}}{{if $i}}, {{end}}{{$tag}}{{end}}</td>
        <td>{{range $i, $note := .Notes}}{{if $i}}; {{end}}{{$note.Body}}{{end}}</td>
      </tr>
      {{end}}
    </tbody>
  </table>

  <h2>New account</h2>
  <form method="post" action="/">
    <label>Id <input type="number" name="id" min="1" placeholder="required when sharded"></label>
    <label>Name <input type="text" name="name" required></label>
    <label>Email <input type="email" name="email" required></label>
    <label>Status <input type="number" name="status" min="0" max="127" value="1" required></label>
    <label>Tags <input type="text" name="tags" placeholder="demo, cluster"></label>
    <label>First note <input type="text" name="note" required></label>
    <fieldset>
      <legend>Write via</legend>
      {{range .Addrs}}
      <label><input type="radio" name="node" value="{{.}}" {{if eq . $.Selected}}checked{{end}}> {{.}}</label>
      {{end}}
    </fieldset>
    <button type="submit">Create account and note</button>
  </form>
  <p class="muted">accounts.id is the shard column. examples/compose.sharded.yaml places ids below 3 on g1–g3 and ids from 3 up on g4–g6. Leave Id empty on the single Raft group.</p>
</body>
</html>
`))
