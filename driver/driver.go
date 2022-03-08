package rdsqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"github.com/raff/rdsql"
)

func init() {
	sql.Register("rdsql", &RdsqlDriver{})
}

type RdsqlDriver struct{}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d RdsqlDriver) Open(name string) (driver.Conn, error) {
	return &rdsqlConn{client: rdsql.ClientWithURI(name, true)}, nil
}

type rdsqlConn struct {
	client *rdsql.Client // rdsql client
	tid    string        // transaction id
}

type Transaction struct {
	conn *rdsqlConn // driver connection
	id   string     // transaction id
}

func (tx *Transaction) Commit() error {
	tx.conn.tid = ""
	_, err := tx.conn.client.CommitTransaction(tx.id, nil)
	return err
}

func (tx *Transaction) Rollback() error {
	tx.conn.tid = ""
	_, err := tx.conn.client.RollbackTransaction(tx.id, nil)
	return err
}

func (rc *rdsqlConn) Begin() (driver.Tx, error) {
	// TODO: check if there is already a transaction pending
	// (can we nest transactions with rdsql or should we throw an error ?)

	tid, err := rc.client.BeginTransaction(nil)
	if err != nil {
		return nil, err
	}

	rc.tid = tid
	return &Transaction{conn: rc, id: tid}, nil
}

func (rc *rdsqlConn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (rc *rdsqlConn) Ping(ctx context.Context) (err error) {
	return rc.client.PingContext(ctx)
}

// QueryContext executes a query that may return rows, such as a SELECT.
func (rc *rdsqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := rc.client.ExecuteStatementContext(
		ctx,
		query,
		paramList(args),
		rc.tid) // last transaction id

	if err != nil {
		return nil, err
	}

	return &rowsIterator{results: res}, err
}

type execResult struct {
	nr int64
}

func (r *execResult) LastInsertId() (int64, error) {
	return 0, errors.New("no LastInsertId available")
}

func (r *execResult) RowsAffected() (int64, error) {
	return r.nr, nil
}

// ExecContext executes a query that should not return rows, such as an INSERT
func (rc *rdsqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := rc.client.ExecuteStatementContext(
		ctx,
		query,
		paramList(args),
		rc.tid) // last transaction id

	if err != nil {
		return nil, err
	}

	return &execResult{nr: res.NumberOfRecordsUpdated}, nil
}

func (rc *rdsqlConn) Close() (err error) {
	rc.client = nil
	return nil
}

func paramList(params []driver.NamedValue) []rdsql.Parameter {
	if len(params) == 0 {
		return nil
	}

	plist := make([]types.SqlParameter, 0, len(params))

	for _, p := range params {
		var field types.Field

		switch t := p.Value.(type) {
		case nil:
			field = &types.FieldMemberIsNull{Value: true}

		case bool:
			field = &types.FieldMemberBooleanValue{Value: t}

		case string:
			field = &types.FieldMemberStringValue{Value: t}

		case int:
			field = &types.FieldMemberLongValue{Value: int64(t)}

		case int8:
			field = &types.FieldMemberLongValue{Value: int64(t)}

		case int16:
			field = &types.FieldMemberLongValue{Value: int64(t)}

		case int64:
			field = &types.FieldMemberLongValue{Value: t}

		case float32:
			field = &types.FieldMemberDoubleValue{Value: float64(t)}

		case float64:
			field = &types.FieldMemberDoubleValue{Value: t}

		case time.Time:
			field = &types.FieldMemberStringValue{Value: t.Format("'2006-01-02 15:04:05'")}

		default:
			log.Fatalf("unsupported parameter type %T: %#v", t, t)
		}

		log.Println(p)
		plist = append(plist, types.SqlParameter{Name: aws.String(p.Name), Value: field})
	}

	return plist
}

type rowsIterator struct {
	results rdsql.Results
	curr    int
}

func (rows *rowsIterator) Columns() (cols []string) {
	for _, c := range rows.results.ColumnMetadata {
		cols = append(cols, aws.ToString(c.Label))
	}

	return
}

func (rows *rowsIterator) Next(dest []driver.Value) error {
	if rows.curr >= len(rows.results.Records) {
		return io.EOF
	}

	row := rows.results.Records[rows.curr]

	for i, f := range row {
		dest[i] = driverValue(f)
	}

	rows.curr++
	return nil
}

func (rows *rowsIterator) Close() (err error) {
	return
}

func driverValue(f rdsql.Field) driver.Value {
	// type switches can be used to check the union value
	switch v := f.(type) {
	case *types.FieldMemberArrayValue:
		//return fmt.Sprintf("unsupported array value: %v", v.Value) // Value is types.ArrayValue
		return nil

	case *types.FieldMemberBlobValue:
		return v.Value // Value is []byte

	case *types.FieldMemberBooleanValue:
		return v.Value // Value is bool

	case *types.FieldMemberDoubleValue:
		return v.Value // Value is float64

	case *types.FieldMemberIsNull:
		return nil

	case *types.FieldMemberLongValue:
		return v.Value // Value is int64

	case *types.FieldMemberStringValue:
		return v.Value // Value is string

	case *types.UnknownUnionMember:
		return nil
	}

	return nil
}
