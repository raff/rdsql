package rdsql

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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
		putResults(results, "", ch)
		close(ch)
	}()

	return ch
}

var field_re = regexp.MustCompile(`\{(\w+)\}`)

// SQLReaderLoop returns a "list" of results from the specified queury.
// It will execute the query in a loop until there are no more results.
// It is possible to start a query from where the previous left off by adding
// a placeholder like {field} (i.e. select a,b,c from table where a > {a} limit 10).
// In this case {a} will be replaced with the last value of `a` from the previous call.
// Pass the appropriate value for `first` to initialize {field} (i.e. "" or 0)
func SQLReaderLoop(db *Client, query, first string) chan string {
	if err := db.Ping(nil); err != nil {
		log.Printf("Cannot connect to database: %v", err)
		return nil
	}

	ch := make(chan string, SQLReaderBuffer)

	pattern := ""
	field := ""
	matches := field_re.FindAllStringSubmatch(query, -1)

	switch len(matches) {
	case 0:
		// nothing to do

	case 1:
		pattern = matches[0][0]
		field = matches[0][1]

	default:
		log.Println("too many placeholders - only one allowed")
		return nil
	}

	go func() {
		last := first

		for {
			q := query

			if pattern != "" {
				q = strings.ReplaceAll(q, pattern, last)
			}

			//log.Println("QUERY:", q)

			results, err := db.ExecuteStatement(q, nil, "", nil)
			if err != nil {
				log.Printf("Query error: %s in %q", err.Error(), query)
				break
			}

			if len(results.Records) == 0 {
				break
			}

			last = putResults(results, field, ch)

			if err := db.Ping(nil); err != nil {
				log.Printf("Cannot connect to database: %v", err)
				break
			}
		}

		close(ch)
	}()

	return ch
}

func putResults(res Results, col string, ch chan string) string {
	var sb strings.Builder

	cols := res.ColumnMetadata
	last := ""

	for _, row := range res.Records {
		sb.Reset()

		for i, r := range row {
			v := format(r)
			if i > 0 {
				sb.WriteString(" ")
			}

			sb.WriteString(v)

			if col != "" && i < len(cols) {
				label := aws.ToString(cols[i].Label)
				if col == label {
					last = v
				}
			}
		}

		ch <- sb.String()
	}

	return last
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
