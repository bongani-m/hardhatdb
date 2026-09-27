package sqlserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/google/uuid"

	"github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/cluster"
	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
	"github.com/dolthub/go-mysql-server/sql"
)

func (h *partHandler) saveRange(c *mysql.Conn, query string, p hardhatdb.TablePlacement, kr hardhatdb.KeyRange, explicit bool) error {
	if !explicit {
		peers, group, err := h.localPeers()
		if err != nil {
			return err
		}
		kr = hardhatdb.KeyRange{
			ID:    group + "/" + strings.ToLower(p.DB) + "." + strings.ToLower(p.Table),
			DB:    p.DB,
			Table: p.Table,
			Start: kr.Start,
			End:   kr.End,
			Group: group,
			Peers: peers,
			State: hardhatdb.RangeActive,
		}
		query = rangeSQL(p, kr)
	}
	if kr.Group == "" || len(kr.Peers) == 0 {
		return fmt.Errorf("hardhatdb: range needs a group and peers")
	}
	if h.meta.IsLeader() {
		if err := h.meta.SavePlacement(p); err != nil {
			return err
		}
		return h.meta.PutRange(kr)
	}
	return h.execMeta(c, query)
}

func rangeSQL(p hardhatdb.TablePlacement, r hardhatdb.KeyRange) string {
	q := fmt.Sprintf("SHARD TABLE %s.%s BY %s", p.DB, p.Table, p.Column)
	if len(p.Check) > 0 {
		q += " CHECK " + p.Check[0]
	}
	q += " RANGE"
	if n, ok := hardhatdb.DecodeIntKey(r.Start); ok {
		q += fmt.Sprintf(" START %d", n)
	}
	if n, ok := hardhatdb.DecodeIntKey(r.End); ok {
		q += fmt.Sprintf(" END %d", n)
	}
	q += " GROUP " + r.Group
	for _, peer := range r.Peers {
		q += fmt.Sprintf(" PEER %s %s %s", peer.ID, peer.Raft, peer.Forward)
	}
	return q
}

func (h *partHandler) localPeers() ([]hardhatdb.RangePeer, string, error) {
	voters, err := h.store.Voters()
	if err != nil {
		return nil, "", err
	}
	fwd := h.store.LocalForwardAddr()
	_, port, err := net.SplitHostPort(fwd)
	if err != nil {
		return nil, "", fmt.Errorf("hardhatdb: forward address: %w", err)
	}
	var peers []hardhatdb.RangePeer
	for _, voter := range voters {
		host, _, err := net.SplitHostPort(voter.Address)
		if err != nil {
			return nil, "", err
		}
		peerFwd := net.JoinHostPort(host, port)
		if voter.ID == h.store.NodeID() {
			peerFwd = fwd
		}
		peers = append(peers, hardhatdb.RangePeer{ID: voter.ID, Raft: voter.Address, Forward: peerFwd})
	}
	if len(peers) == 0 {
		return nil, "", fmt.Errorf("hardhatdb: range group has no voters")
	}
	return peers, h.store.GroupID(), nil
}

func (h *partHandler) rangeList() []hardhatdb.KeyRange {
	list, err := h.meta.Ranges()
	if err != nil {
		return nil
	}
	return list
}

func groupsOf(list []hardhatdb.KeyRange, place hardhatdb.TablePlacement) []string {
	var groups []string
	for _, r := range hardhatdb.RangesFor(list, place.DB, place.Table) {
		found := false
		for _, g := range groups {
			if g == r.Group {
				found = true
				break
			}
		}
		if !found {
			groups = append(groups, r.Group)
		}
	}
	return groups
}

func (h *partHandler) execRanged(c *mysql.Conn, pin *partPin, route Route, query string, binds []cluster.ForwardBind, callback mysql.ResultSpoolFn) error {
	if len(route.Groups) != 1 {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: statement spans shards")
	}
	group := route.Groups[0]
	if pin.inTx {
		if pin.begun == nil {
			pin.begun = map[string]bool{}
		}
		if !pin.begun[group] {
			if _, err := h.execOnGroup(c, group, "BEGIN", nil, true, false); err != nil {
				return err
			}
			pin.begun[group] = true
			pin.groups = append(pin.groups, group)
		}
	}
	if err := h.rejectDups(c, route); err != nil {
		return err
	}
	reply, err := h.execOnGroup(c, group, query, binds, pin.inTx, false)
	if err != nil {
		return err
	}
	return spoolReply(reply, callback)
}

func (h *partHandler) execOnGroup(c *mysql.Conn, group, query string, binds []cluster.ForwardBind, hold, release bool) (cluster.ForwardReply, error) {
	peers, err := h.peersFor(group)
	if err != nil {
		return cluster.ForwardReply{}, err
	}
	addr, err := h.groupForward(c, peers)
	if err != nil {
		return cluster.ForwardReply{}, err
	}
	req := h.request(c, h.store, query, binds)
	req.Hold = hold
	req.Release = release
	var reply cluster.ForwardReply
	if hosted := h.groups.get(group); hosted != nil && hosted.store.LocalForwardAddr() == addr {
		reply, err = hosted.store.ExecForward(req)
	} else {
		reply, err = h.call(c, h.store, addr, req)
	}
	if err != nil {
		return cluster.ForwardReply{}, err
	}
	if reply.Err != "" {
		return cluster.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	return reply, nil
}

func (h *partHandler) peersFor(group string) ([]hardhatdb.RangePeer, error) {
	for _, r := range h.rangeList() {
		if r.Group == group && len(r.Peers) > 0 {
			return r.Peers, nil
		}
		if r.State == hardhatdb.RangeCopying && r.RightGroup == group && len(r.RightPeers) > 0 {
			return r.RightPeers, nil
		}
	}
	if h.groups != nil && h.groups.get(group) != nil && group == h.store.GroupID() {
		peers, _, err := h.localPeers()
		return peers, err
	}
	return nil, fmt.Errorf("hardhatdb: group %s has no peers", group)
}

func (h *partHandler) groupForward(c *mysql.Conn, peers []hardhatdb.RangePeer) (string, error) {
	if len(peers) == 0 {
		return "", fmt.Errorf("hardhatdb: range has no peers")
	}
	reply, err := h.call(c, h.store, peers[0].Forward, queryReq(h.store, "SHOW RAFT STATUS", nil))
	if err != nil {
		return "", err
	}
	if reply.Err != "" {
		return "", fmt.Errorf("%s", reply.Err)
	}
	if len(reply.Rows) == 0 || len(reply.Rows[0]) < 2 || len(reply.Rows[0][1].Raw) == 0 {
		return "", fmt.Errorf("hardhatdb: range leader is unknown")
	}
	leader := string(reply.Rows[0][1].Raw)
	lhost, _, err := net.SplitHostPort(leader)
	if err != nil {
		lhost = leader
	}
	for _, p := range peers {
		host, _, err := net.SplitHostPort(p.Raft)
		if err != nil {
			continue
		}
		if host == lhost || p.Raft == leader {
			return p.Forward, nil
		}
	}
	return "", fmt.Errorf("hardhatdb: range leader is unknown")
}

func (h *partHandler) scatterGroups(c *mysql.Conn, groups []string, query string, binds []cluster.ForwardBind) (cluster.ForwardReply, error) {
	if len(groups) == 0 {
		return cluster.ForwardReply{}, nil
	}
	addrs := make([]string, len(groups))
	for i, group := range groups {
		peers, err := h.peersFor(group)
		if err != nil {
			return cluster.ForwardReply{}, err
		}
		if len(peers) == 0 {
			return cluster.ForwardReply{}, fmt.Errorf("hardhatdb: group %s has no peers", group)
		}
		addrs[i] = peers[0].Forward
	}
	req := h.request(c, h.store, query, binds)
	return scatterFanout(addrs, func(addr string) (cluster.ForwardReply, error) {
		for _, group := range groups {
			hosted := h.groups.get(group)
			if hosted != nil && hosted.store.LocalForwardAddr() == addr {
				return checkedReply(hosted.store.ExecForward(req))
			}
		}
		if h.store.LocalForwardAddr() == addr {
			return checkedReply(h.store.ExecForward(req))
		}
		return checkedReply(h.execPooled(c, h.store, addr, req))
	})
}

func (h *partHandler) finishRanges(c *mysql.Conn, query string, groups []string) error {
	if txKind(query) != txEnd || len(groups) < 2 || isRollback(query) {
		for _, group := range groups {
			if _, err := h.execOnGroup(c, group, query, nil, true, true); err != nil {
				return err
			}
		}
		return nil
	}
	id := uuid.NewString()
	var prepared []string
	abort := func() {
		for _, group := range prepared {
			_, _ = h.execOnGroup(c, group, "ABORT TX "+id, nil, true, true)
		}
	}
	for _, group := range groups {
		if _, err := h.execOnGroup(c, group, "PREPARE TX "+id, nil, true, false); err != nil {
			abort()
			return err
		}
		prepared = append(prepared, group)
	}
	n, err := h.nextCommit(c)
	if err != nil {
		abort()
		return err
	}
	if err := h.saveDecision(c, hardhatdb.TxnDecision{ID: id, Commit: n}); err != nil {
		abort()
		return err
	}
	q := fmt.Sprintf("COMMIT TX %s %d", id, n)
	for _, group := range groups {
		if _, err := h.execOnGroup(c, group, q, nil, true, true); err != nil {
			return err
		}
	}
	return nil
}

func isRollback(query string) bool {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return false
	}
	_, ok := stmt.(*sqlparser.Rollback)
	return ok
}

func (h *partHandler) nextCommit(c *mysql.Conn) (uint64, error) {
	if h.meta.IsLeader() {
		return h.meta.NextCommit()
	}
	reply, err := h.call(c, h.meta, "", queryReq(h.meta, "NEXT COMMIT", nil))
	if err != nil {
		return 0, err
	}
	if reply.Err != "" {
		return 0, fmt.Errorf("%s", reply.Err)
	}
	if reply.InsertID == 0 {
		return 0, fmt.Errorf("hardhatdb: commit number is missing")
	}
	return reply.InsertID, nil
}

func (h *partHandler) saveDecision(c *mysql.Conn, d hardhatdb.TxnDecision) error {
	if h.meta.IsLeader() {
		return h.meta.SaveDecision(d)
	}
	q := fmt.Sprintf("SAVE DECISION %s %d", d.ID, d.Commit)
	reply, err := h.call(c, h.meta, "", queryReq(h.meta, q, nil))
	if err != nil {
		return err
	}
	if reply.Err != "" {
		return fmt.Errorf("%s", reply.Err)
	}
	if reply.Index > 0 {
		return h.meta.WaitApplied(reply.Index, h.meta.ApplyTimeout())
	}
	return nil
}

func (h *partHandler) handleSplit(c *mysql.Conn, dbName, table string, at int64) error {
	key := hardhatdb.EncodeIntKey(at)
	kr, ok := hardhatdb.RangeHolding(h.rangeList(), dbName, table, key)
	if !ok {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "hardhatdb: no range holds the split key")
	}
	if h.store.GroupID() == kr.Group && h.store.IsLeader() {
		return h.doSplit(dbName, table, key)
	}
	_, err := h.execOnGroup(c, kr.Group, fmt.Sprintf("SPLIT RANGE %s.%s AT %d", dbName, table, at), nil, false, false)
	return err
}

func (h *partHandler) doSplit(dbName, table string, at []byte) error {
	h.splitMu.Lock()
	defer h.splitMu.Unlock()
	kr, ok := hardhatdb.RangeHolding(h.rangeList(), dbName, table, at)
	if !ok {
		return fmt.Errorf("hardhatdb: no range holds the split key")
	}
	if kr.State == hardhatdb.RangeCopying && kr.RightGroup != "" {
		at = append([]byte(nil), kr.SplitKey...)
		if err := h.openHalf(kr.Peers, kr.RightPeers, kr.RightGroup); err != nil {
			return err
		}
		src := h.groupStore(kr.Group)
		dst := h.groups.get(kr.RightGroup)
		if src == nil || dst == nil {
			return fmt.Errorf("hardhatdb: split group %s is not local", kr.RightGroup)
		}
		ctx := sql.NewContext(context.Background())
		return hardhatdb.SplitRange(ctx, h.catalog(), src, dst.store, dbName, table, at, hardhatdb.KeyRange{
			ID: kr.RightID, Group: kr.RightGroup, Peers: kr.RightPeers,
		})
	}
	src := h.groupStore(kr.Group)
	if src == nil || !src.IsLeader() {
		return fmt.Errorf("hardhatdb: split runs on the range leader")
	}
	delta := 10 * (h.groups.count() + 1)
	rightPeers, err := shiftPeers(kr.Peers, delta)
	if err != nil {
		return err
	}
	rightPeers = selfFirst(rightPeers, h.groups.nodeID)
	rightID := uuid.NewString()
	if err := h.openHalf(kr.Peers, rightPeers, rightID); err != nil {
		return err
	}
	dst := h.groups.get(rightID)
	if dst == nil {
		return fmt.Errorf("hardhatdb: split group %s is not local", rightID)
	}
	ctx := sql.NewContext(context.Background())
	return hardhatdb.SplitRange(ctx, h.catalog(), src, dst.store, dbName, table, at, hardhatdb.KeyRange{
		ID: rightID, Group: rightID, Peers: rightPeers, State: hardhatdb.RangeActive,
	})
}

func (h *partHandler) groupStore(id string) *hardhatdb.Store {
	if h.groups != nil {
		if hosted := h.groups.get(id); hosted != nil {
			return hosted.store
		}
	}
	if h.store.GroupID() == id {
		return h.store
	}
	return nil
}

func (h *partHandler) catalog() hardhatdb.RangeCatalog {
	return rangeCatalog{h: h}
}

type rangeCatalog struct {
	h *partHandler
}

func (c rangeCatalog) Ranges() ([]hardhatdb.KeyRange, error) {
	return c.h.meta.Ranges()
}

func (c rangeCatalog) ApplyRangeOp(op hardhatdb.RangeOp) error {
	if c.h.meta.IsLeader() || !c.h.meta.Replicating() {
		return c.h.meta.ApplyRangeOp(op)
	}
	raw, err := json.Marshal(op)
	if err != nil {
		return err
	}
	q := "RANGE OP " + base64.RawURLEncoding.EncodeToString(raw)
	reply, err := c.h.call(nil, c.h.meta, "", queryReq(c.h.meta, q, nil))
	if err != nil {
		return err
	}
	if reply.Err != "" {
		return fmt.Errorf("%s", reply.Err)
	}
	if reply.Index > 0 {
		return c.h.meta.WaitApplied(reply.Index, c.h.meta.ApplyTimeout())
	}
	return nil
}

func (h *partHandler) openHalf(oldPeers, newPeers []hardhatdb.RangePeer, groupID string) error {
	ordered := selfFirst(newPeers, h.groups.nodeID)
	raftPeers := make([]cluster.Peer, len(ordered))
	for i, p := range ordered {
		raftPeers[i] = cluster.Peer{ID: p.ID, Address: p.Raft}
	}
	var self *openSpec
	type remoteOpen struct {
		spec    openSpec
		forward string
	}
	var remotes []remoteOpen
	for i, p := range ordered {
		spec := openSpec{
			GroupID: groupID, NodeID: p.ID, Bootstrap: i == 0,
			Bind: p.Raft, Forward: p.Forward, Peers: raftPeers, Strict: true,
		}
		if p.ID == h.groups.nodeID {
			cp := spec
			self = &cp
			continue
		}
		old := peerByID(oldPeers, p.ID)
		if old.Forward == "" {
			return fmt.Errorf("hardhatdb: shard peer %s has no forward address", p.ID)
		}
		remotes = append(remotes, remoteOpen{spec: spec, forward: old.Forward})
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(remotes))
	for _, remote := range remotes {
		wg.Add(1)
		go func(spec openSpec, forward string) {
			defer wg.Done()
			reply, err := h.call(nil, h.store, forward, queryReq(h.store, openGroupSQL(spec.GroupID, hardhatdb.RangePeer{
				ID: spec.NodeID, Raft: spec.Bind, Forward: spec.Forward,
			}, spec.Bootstrap, ordered), nil))
			if err != nil {
				errs <- err
				return
			}
			if reply.Err != "" {
				errs <- fmt.Errorf("%s", reply.Err)
			}
		}(remote.spec, remote.forward)
	}
	var localErr error
	if self != nil {
		localErr = h.groups.open(*self)
	}
	wg.Wait()
	close(errs)
	if localErr != nil {
		return localErr
	}
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func selfFirst(peers []hardhatdb.RangePeer, id string) []hardhatdb.RangePeer {
	var self, rest []hardhatdb.RangePeer
	for _, p := range peers {
		if p.ID == id {
			self = append(self, p)
			continue
		}
		rest = append(rest, p)
	}
	return append(self, rest...)
}

func peerByID(peers []hardhatdb.RangePeer, id string) hardhatdb.RangePeer {
	for _, p := range peers {
		if p.ID == id {
			return p
		}
	}
	return hardhatdb.RangePeer{}
}

func (h *partHandler) handleMove(c *mysql.Conn, add bool, group, id, raft, forward string) error {
	if hosted := h.groups.get(group); hosted != nil && hosted.store.IsLeader() {
		return h.movePeer(add, group, id, raft, forward)
	}
	q := fmt.Sprintf("RANGE ADD VOTER %s %s %s %s", group, id, raft, forward)
	if !add {
		q = fmt.Sprintf("RANGE REMOVE SERVER %s %s", group, id)
	}
	_, err := h.execOnGroup(c, group, q, nil, false, false)
	return err
}

func (h *partHandler) movePeer(add bool, group, id, raft, forward string) error {
	hosted := h.groups.get(group)
	if hosted == nil || !hosted.store.IsLeader() {
		return fmt.Errorf("hardhatdb: range move runs on the group leader")
	}
	peer := hardhatdb.RangePeer{ID: id, Raft: raft, Forward: forward}
	if add {
		if err := h.catalog().ApplyRangeOp(hardhatdb.RangeOp{
			Kind: hardhatdb.RangeOpAddPeer, Group: group, Peer: peer,
		}); err != nil {
			return err
		}
		if err := hosted.store.AddVoter(id, raft); err != nil {
			return err
		}
		return hosted.store.Snapshot()
	}
	if err := hosted.store.RemoveServer(id); err != nil {
		return err
	}
	return h.catalog().ApplyRangeOp(hardhatdb.RangeOp{
		Kind: hardhatdb.RangeOpDelPeer, Group: group, Peer: hardhatdb.RangePeer{ID: id},
	})
}

func (h *partHandler) resumeSplits() {
	time.Sleep(time.Second)
	for _, r := range h.rangeList() {
		if r.State != hardhatdb.RangeCopying {
			continue
		}
		src := h.groupStore(r.Group)
		if src == nil || !src.IsLeader() {
			continue
		}
		if err := h.doSplit(r.DB, r.Table, r.SplitKey); err != nil {
			log.Printf("resume split %s: %v", r.ID, err)
		}
	}
}

func (h *partHandler) watchSplits(limit int64) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for range tick.C {
		ctx := sql.NewContext(context.Background())
		for _, hosted := range h.groups.list() {
			if !hosted.store.IsLeader() || hosted.store.LSMBytes() < limit {
				continue
			}
			for _, r := range h.rangeList() {
				if r.Group != hosted.id || r.State != hardhatdb.RangeActive {
					continue
				}
				at, ok, err := hosted.store.SplitKey(ctx, r.DB, r.Table, r.Start, r.End)
				if err != nil || !ok {
					continue
				}
				if err := h.doSplit(r.DB, r.Table, at); err != nil {
					log.Printf("split %s: %v", r.TableKey(), err)
				}
				break
			}
		}
	}
}
