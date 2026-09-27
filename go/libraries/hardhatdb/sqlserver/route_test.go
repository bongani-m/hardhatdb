package sqlserver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	hardhatdb "github.com/bongani-m/hardhatdb/go/libraries/hardhatdb/sqle"
)

func testPlaces() []hardhatdb.TablePlacement {
	return []hardhatdb.TablePlacement{
		{DB: "stress", Table: "accounts", Column: "id", Check: []string{"email"}},
		{DB: "stress", Table: "notes", Column: "account_id"},
	}
}

func TestRouteDDLAndPlacement(t *testing.T) {
	ddl, err := RouteQuery("CREATE TABLE accounts (id BIGINT NOT NULL, PRIMARY KEY (id))", "stress", nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, RouteDDL, ddl.Kind)

	_, err = RouteQuery("SHARD TABLE stress.accounts BY id CHECK email", "stress", nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "needs RANGE")

	_, err = RouteQuery("SELECT email FROM accounts WHERE id = 1", "stress", nil, testPlaces(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no ranges")
}

func TestCheckQueriesGroupsQuotesAndChunks(t *testing.T) {
	place := hardhatdb.TablePlacement{DB: "stress", Table: "accounts"}
	queries, dup := checkQueries(place, []routedValue{
		{Column: "email", Text: "user1@example.com", Quote: true},
		{Column: "email", Text: "o'reilly@b.c", Quote: true},
	})
	require.Nil(t, dup)
	require.Equal(t, []checkQuery{{
		Column: "email",
		SQL:    "SELECT email FROM stress.accounts WHERE email IN ('user1@example.com','o''reilly@b.c')",
	}}, queries)

	_, dup = checkQueries(place, []routedValue{
		{Column: "email", Text: "a@b.c", Quote: true},
		{Column: "email", Text: "a@b.c", Quote: true},
	})
	require.NotNil(t, dup)
	require.Equal(t, "a@b.c", dup.Text)
	require.Equal(t, "email", dup.Column)

	many := make([]routedValue, checkBatch+1)
	for i := range many {
		many[i] = routedValue{Column: "email", Text: fmt.Sprintf("u%d@b.c", i), Quote: true}
	}
	queries, dup = checkQueries(place, many)
	require.Nil(t, dup)
	require.Len(t, queries, 2)
	require.Equal(t, "email", queries[0].Column)
	require.Equal(t, "email", queries[1].Column)
}

func testRanges() ([]hardhatdb.TablePlacement, []hardhatdb.KeyRange) {
	places := []hardhatdb.TablePlacement{{DB: "stress", Table: "accounts", Column: "id", Check: []string{"email"}}}
	mid := hardhatdb.KeySuccessor(hardhatdb.EncodeIntKey(100))
	ranges := []hardhatdb.KeyRange{
		{ID: "lo", DB: "stress", Table: "accounts", End: mid, Group: "g0", State: hardhatdb.RangeActive},
		{ID: "hi", DB: "stress", Table: "accounts", Start: mid, Group: "g1", State: hardhatdb.RangeActive},
	}
	return places, ranges
}

func TestRouteRangeEqualityBetweenAndScatter(t *testing.T) {
	places, ranges := testRanges()
	one, err := RouteQuery("SELECT email FROM accounts WHERE id = 40", "stress", nil, places, ranges)
	require.NoError(t, err)
	require.Equal(t, RouteShard, one.Kind)
	require.Equal(t, []string{"g0"}, one.Groups)
	require.True(t, one.Ranged)

	hi, err := RouteQuery("SELECT email FROM accounts WHERE id = 140", "stress", nil, places, ranges)
	require.NoError(t, err)
	require.Equal(t, []string{"g1"}, hi.Groups)

	span, err := RouteQuery("SELECT email FROM accounts WHERE id >= 90 AND id < 110", "stress", nil, places, ranges)
	require.NoError(t, err)
	require.Equal(t, RouteScatter, span.Kind)
	require.Equal(t, []string{"g0", "g1"}, span.Groups)

	between, err := RouteQuery("SELECT email FROM accounts WHERE id BETWEEN 1 AND 50", "stress", nil, places, ranges)
	require.NoError(t, err)
	require.Equal(t, RouteShard, between.Kind)
	require.Equal(t, []string{"g0"}, between.Groups)

	all, err := RouteQuery("SELECT email FROM accounts WHERE email = 'a@b.c'", "stress", nil, places, ranges)
	require.NoError(t, err)
	require.Equal(t, RouteScatter, all.Kind)
	require.Equal(t, []string{"g0", "g1"}, all.Groups)

	_, err = RouteQuery("UPDATE accounts SET status = 1 WHERE id >= 90 AND id < 110", "stress", nil, places, ranges)
	require.Error(t, err)
	require.Contains(t, err.Error(), "spans shards")

	_, err = RouteQuery("INSERT INTO accounts (id, email) VALUES (1, 'a@b.c'), (140, 'c@d.e')", "stress", nil, places, ranges)
	require.Error(t, err)
	require.Contains(t, err.Error(), "spans shards")

	// A second statement in the same transaction is allowed. Commit uses two phases.
	left, err := RouteQuery("UPDATE accounts SET status = 1 WHERE id = 40", "stress", nil, places, ranges)
	require.NoError(t, err)
	right, err := RouteQuery("UPDATE accounts SET status = 1 WHERE id = 140", "stress", nil, places, ranges)
	require.NoError(t, err)
	require.Equal(t, []string{"g0"}, left.Groups)
	require.Equal(t, []string{"g1"}, right.Groups)

	created, err := RouteQuery("SHARD TABLE stress.accounts BY id RANGE", "stress", nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, RoutePlace, created.Kind)
	require.True(t, created.Ranged)
	require.Equal(t, "id", created.Place.Column)
	require.False(t, created.SpanSet)

	bounded, err := RouteQuery("SHARD TABLE stress.accounts BY id CHECK email RANGE END 2501 GROUP g0 PEER n1 10.0.0.1:7101 10.0.0.1:7102", "stress", nil, nil, nil)
	require.NoError(t, err)
	require.True(t, bounded.SpanSet)
	require.Equal(t, "g0", bounded.Span.Group)
	require.Equal(t, hardhatdb.EncodeIntKey(2501), bounded.Span.End)
	require.Empty(t, bounded.Span.Start)
	require.Equal(t, []string{"email"}, bounded.Place.Check)
	require.Equal(t, "10.0.0.1:7102", bounded.Span.Peers[0].Forward)
}
