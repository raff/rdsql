package rdsql

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

var SQLReaderBuffer = 10

// SQLReader returns a "list" of results from the specified queury
func SQLReader(db *Client, query string) chan string {
	ch := make(chan string, SQLReaderBuffer)

	results, err := db.ExecuteStatement(query, nil, "", nil)
	if err != nil {
		log.Printf("Query error: %s in %q", err.Error(), query)
		return nil
	}

	go func() {
		putResults(results, ch)
		close(ch)
	}()

	return ch
}

// SQLReaderLoop returns a "list" of results from the specified queury.
// It will execute the query in a loop until there are no more results.
func SQLReaderLoop(db *Client, query string) chan string {
	if err := db.Ping(nil); err != nil {
		log.Printf("Cannot connect to database: %v", err)
		return nil
	}

	ch := make(chan string, SQLReaderBuffer)

	go func() {
		for {
			results, err := db.ExecuteStatement(query, nil, "", nil)
			if err != nil {
				log.Printf("Query error: %s in %q", err.Error(), query)
				break
			}

			if len(results.Records) == 0 {
				break
			}

			putResults(results, ch)

			if err := db.Ping(nil); err != nil {
				log.Printf("Cannot connect to database: %v", err)
				break
			}
		}

		close(ch)
	}()

	return ch
}

func putResults(res Results, ch chan string) {
	var sb strings.Builder

	for _, row := range res.Records {
		sb.Reset()

		for i, r := range row {
			v := format(r)
			if i > 0 {
				sb.WriteString(" ")
			}

			sb.WriteString(v)
		}

		ch <- sb.String()
	}
}

func format(f Field) string {
	// type switches can be used to check the union value
	switch v := f.(type) {
	case *types.FieldMemberArrayValue:
		return fmt.Sprintf("unsupported array value: %v", v.Value) // Value is types.ArrayValue

	case *types.FieldMemberBlobValue:
		return string(v.Value) // Value is []byte

	case *types.FieldMemberBooleanValue:
		return strconv.FormatBool(v.Value) // Value is bool

	case *types.FieldMemberDoubleValue:
		return strconv.FormatFloat(v.Value, 'f', -1, 64) // Value is float64

	case *types.FieldMemberIsNull:
		return "NULL"

	case *types.FieldMemberLongValue:
		return strconv.FormatInt(v.Value, 10) // Value is int64

	case *types.FieldMemberStringValue:
		return v.Value // Value is string

	case *types.UnknownUnionMember:
		return fmt.Sprintf("unknown tag: %v", v.Tag)

	default:
		return fmt.Sprintf("union is nil or unknown type: %#v", f)
	}

	return ""
}
