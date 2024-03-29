package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/peterh/liner"
	"github.com/raff/rdsql"
)

const (
	stdoutName = "<stdout>"
)

var (
	resourceArn = os.Getenv("RDS_RESOURCE")
	secretArn   = os.Getenv("RDS_SECRET")
	dbName      = os.Getenv("RDS_DATABASE")
	profile     = os.Getenv("RDS_PROFILE")
	delimiter   = ""
	tformat     = "table"
	elapsed     bool
	debug       bool
	verbose     bool
	silent      bool
	output      = os.Stdout
	outputName  = stdoutName

	keywords = []string{
		"BEGIN",
		"START",
		"COMMIT",
		"ROLLBACK",
		"TRANSACTION",
		"END",
		"EXEC",
		"SELECT",
		"COUNT",
		"FROM",
		"WHERE",
		"IN",
		"IS",
		"NOT",
		"NULL",
		"AS",
		"AND",
		"OR",
		"LIKE",
		"INSERT",
		"IGNORE",
		"INTO",
		"VALUES",
		"UPDATE",
		"SET",
		"USE",
		"TABLE",
		"LIMIT",
		"ORDER BY",
		"ASC",
		"DESC",
		"CURRENT_TIMESTAMP",
		"DESCRIBE",
		"SHOW",
		"TABLES",
		"INDEX",
		"JOIN",
		"LEFT",
		"RIGHT",
		"OUTER",
		"INNER",
		"ON",
		"GROUP BY",
		"HAVING",
		"UNION",
		"ALL",
	}

	historyfile = ".rdsql"
)

func init() {
	sort.Strings(keywords)

	if profile == "" {
		profile = os.Getenv("AWS_PROFILE")
	}
}

func main() {
	flag.StringVar(&resourceArn, "resource", resourceArn, "resource ARN")
	flag.StringVar(&secretArn, "secret", secretArn, "resource secret")
	flag.StringVar(&dbName, "database", dbName, "database")
	flag.StringVar(&profile, "profile", profile, "AWS profile")
	flag.StringVar(&delimiter, "delimiter", delimiter, "Statement delimiter")
	flag.StringVar(&tformat, "format", tformat, "output format (tabs, table, csv)")
	flag.BoolVar(&elapsed, "elapsed", elapsed, "print elapsed time")
	flag.BoolVar(&debug, "debug", debug, "enable debugging")
	flag.BoolVar(&verbose, "verbose", verbose, "log statements before execution")
	flag.BoolVar(&silent, "silent", silent, "print less output (no column names, no total records")
	flag.IntVar(&rdsql.PingRetries, "wait", 10, "how long to wait for initial ping")
	flag.IntVar(&rdsql.QueryRetries, "retries", rdsql.QueryRetries, "number of query retries")

	timeout := flag.Duration("timeout", 2*time.Minute, "request timeout")
	cont := flag.Bool("continue", true, "continue after timeout (for DDL statements)")
	trans := flag.Bool("transaction", false, "wrap full session in a remote transaction")
	fparams := flag.String("params", "", "query parameters (comma separated list of name=value pair)")

	flag.Parse()

	switch tformat {
	case "csv", "tabs", "Tabs", "table", "Table":
		// good

	default:
		log.Fatalf("Invalid output format: %v", tformat)
	}

	awscfg := rdsql.GetAWSConfig(profile, debug)
	client := rdsql.ClientWithOptions(awscfg, resourceArn, secretArn, dbName)
	client.Continue = *cont
	client.Timeout = *timeout

	if len(delimiter) > 0 {
		delimiter = delimiter[0:1]
	}
	if delimiter == "[" {
		delimiter = ""
	}

	params := parseParams(*fparams)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer signal.Stop(c)

	if err := client.Ping(c); err != nil {
		log.Fatalf("Cannot connect to database: %v", err)
	}

	var transactionId string
	var dberr error
	var res rdsql.Results

	if *trans {
		shouldPrint := printElapsed("BEGIN TRANSACTION:", elapsed)
		tid, err := client.BeginTransaction(c)
		shouldPrint()

		if err != nil {
			log.Println("BEGIN TRANSACTION:", err)
			return
		}

		fmt.Println("BEGIN TRANSACTION:", tid)
		transactionId = tid

		defer func() {
			shouldPrint := printElapsed("END TRANSACTION:", elapsed)

			if res, err := client.EndTransaction(transactionId, dberr == nil, c); err != nil {
				log.Println("END TRANSACTION:", err)
			} else {
				log.Println("END TRANSACTION:", res)
			}

			shouldPrint()
		}()
	}

	if flag.NArg() != 0 {
		stmt := strings.Join(flag.Args(), " ")

		shouldPrint := printElapsed("EXEC:", elapsed)
		res, dberr = client.ExecuteStatement(stmt, rdsql.ParamMap(params), transactionId, c)
		shouldPrint()

		if dberr != nil {
			log.Println(dberr)
			log.Println("STMT:", stmt)
			return
		}

		printResults(res, tformat)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	var inputRedirected bool

	if _, err := liner.TerminalMode(); err != nil {
		inputRedirected = true
	}

	if !inputRedirected {
		if f, err := os.Open(historyfile); err == nil {
			line.ReadHistory(f)
			f.Close()
		}

		defer func() {
			if f, err := os.Create(historyfile); err == nil {
				line.WriteHistory(f)
				f.Close()
			}
		}()
	}

	line.SetWordCompleter(func(line string, pos int) (head string, completions []string, tail string) {
		head = line[:pos]
		tail = line[pos:]

		i := strings.LastIndex(head, " ")
		w := head[i+1:]

		head = strings.TrimSuffix(head, w)
		w = strings.ToUpper(w)

		for _, n := range keywords {
			if strings.HasPrefix(n, w) {
				completions = append(completions, n)
			}
		}
		return
	})

	var stmt string
	var multi bool
	var script bool

	prompt := map[bool]string{
		false: "> ",
		true:  ": ",
	}

	if _, err := liner.TerminalMode(); err != nil {
		prompt[false] = ""
		prompt[true] = ""
		script = true
	}

	for {
		l, err := line.Prompt(prompt[multi])
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				return
			}
			fmt.Println(err)
			return
		}

		if strings.HasPrefix(strings.TrimSpace(l), "--") { // comment
			continue
		}

		if delimiter == "" {
			if multi == false {
				if l == "[[[" {
					multi = true
					stmt = ""
					continue
				} else {
					stmt = l
				}
			} else {
				if l == "]]]" {
					multi = false
				} else {
					stmt += " " + strings.TrimSpace(l)
					continue
				}
			}
		} else {
			l = strings.TrimSpace(l)
			if strings.HasPrefix(l, `\`) || l == "" {
				// skip this, commands are never multi-line
			} else if multi == false {
				stmt = strings.TrimSuffix(l, delimiter)

				if !strings.HasSuffix(l, delimiter) {
					multi = true
					continue
				}
			} else {
				stmt += " " + strings.TrimSuffix(l, delimiter)

				if strings.HasSuffix(l, delimiter) {
					multi = false
				} else {
					continue
				}
			}
		}

		stmt = strings.TrimSpace(stmt)
		if len(l) == 0 {
			continue
		}

		if !inputRedirected {
			line.AppendHistory(stmt)
		}

		if strings.HasPrefix(stmt, `\`) {
			executeCommand(client, stmt)
			continue
		}

		if verbose {
			fmt.Println("--", stmt)
		}

		shouldPrint := printElapsed("EXEC:", elapsed)
		res, dberr = client.ExecuteStatement(stmt, rdsql.ParamMap(params), transactionId, c)
		shouldPrint()

		if dberr != nil {
			log.Println(dberr)
			log.Printf("STMT: %q\n", stmt)

			if script { // for scripts, break at first error
				break
			}
		} else {
			printResults(res, tformat)
		}
	}
}

func printElapsed(prefix string, print bool) func() {
	if !print {
		return func() {}
	}

	t := time.Now()

	return func() {
		fmt.Println(prefix, "Elapsed:", time.Since(t).Truncate(time.Millisecond))
	}
}

func printResults(res rdsql.Results, tformat string) {
	if debug {
		if dmesg, err := json.MarshalIndent(res, "", " "); err == nil {
			log.Println("RESULT")
			log.Println(string(dmesg))
		}
	}

	defer func() {
		if !silent {
			nr := res.NumberOfRecordsUpdated
			if nr > 0 {
				fmt.Println("Updated", nr, "records")
			} else {
				fmt.Println("\nTotal", len(res.Records))
			}
		}
	}()

	cols := res.ColumnMetadata
	if len(cols) == 0 {
		return
	}

	t := table.NewWriter()

	if strings.ToLower(tformat) == "tabs" {
		t.SetStyle(table.Style{
			Name: "minimalSyle",
			Box: table.BoxStyle{
				MiddleVertical: " ",
			},
			Options: table.Options{
				SeparateColumns: true,
			},
		})
	}

	if tformat == "csv" || tformat[0] == 'T' || !silent {
		tr := make(table.Row, len(cols))
		for i := 0; i < len(cols); i++ {
			tr[i] = aws.ToString(cols[i].Label)
		}

		t.AppendHeader(tr)
	}

	for _, row := range res.Records {
		tr := make(table.Row, len(cols))

		for i, r := range row {
			tr[i] = format(r)
		}

		t.AppendRow(tr)
	}

	if tformat == "csv" {
		fmt.Fprintln(output, t.RenderCSV())
	} else {
		fmt.Fprintln(output, t.Render())
	}
}

func format(f rdsql.Field) string {
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

func parseParams(s string) map[string]interface{} {
	if len(s) == 0 {
		return nil
	}

	parts := strings.Split(s, ",")
	params := make(map[string]interface{})

	for _, p := range parts {
		nv := strings.SplitN(p, "=", 2)
		if len(nv) != 2 {
			log.Fatal("invalid name=value pair")
		}

		if len(nv[1]) == 0 {
			params[nv[0]] = nil
		} else {
			params[nv[0]] = nv[1]
		}
	}

	return params
}

const help = `
Available commands are:

?         (\?) Synonym for 'help'
debug     (\D) Enable/disable debug mode
delimiter (\d) Set statement delimiter
help      (\h) Display this help
elapsed   (\e) Enable/disable elapsed time
timeout   (\t) Set request timeout
continue  (\c) Continue/stop after timeout
use       (\u) Use specified database
verbose   (\v) Enable/disable verbose mode
format    (\f) Set output format (tabs, table, csv)
echo      (\E) Echo text
output    (\o) output to file
`

func executeCommand(client *rdsql.Client, cmd string) {
	var c string
	params := strings.Fields(cmd)
	c, params = params[0], params[1:]

	switch {
	case c == `\?` || strings.HasPrefix(c, `\h`): // help
		fmt.Println(help)

	case strings.HasPrefix(c, `\D`): // debug [bool]
		if len(params) > 0 {
			debug, _ = strconv.ParseBool(params[0])
		}
		fmt.Println("debug", debug)

	case strings.HasPrefix(c, `\d`): // delimiter [char]
		if len(params) > 0 {
			d := params[0]
			if d[0] == '[' {
				delimiter = ""
			} else {
				delimiter = d[0:1]
			}
			if silent {
				return
			}
		}
		fmt.Println("delimiter", delimiter)

	case strings.HasPrefix(c, `\e`): // elapsed [bool]
		if len(params) > 0 {
			elapsed, _ = strconv.ParseBool(params[0])
			if silent {
				return
			}
		}
		fmt.Println("elapsed", elapsed)

	case strings.HasPrefix(c, `\c`): // continue [bool]
		if len(params) > 0 {
			client.Continue, _ = strconv.ParseBool(params[0])
			if silent {
				return
			}
		}
		fmt.Println("continue", client.Continue)

	case strings.HasPrefix(c, `\t`): // timeout [duration]
		if len(params) > 0 {
			client.Timeout, _ = time.ParseDuration(params[0])
			if silent {
				return
			}
		}
		fmt.Println("timeout", client.Timeout)

	case strings.HasPrefix(c, `\u`): // use [database name]
		if len(params) > 0 {
			if u, err := strconv.Unquote(params[0]); err == nil {
				params[0] = u
			}
			client.Database = params[0]
			if silent {
				return
			}
		}
		fmt.Println("use", client.Database)

	case strings.HasPrefix(c, `\v`): // verbose [bool]
		if len(params) > 0 {
			verbose, _ = strconv.ParseBool(params[0])
		}
		fmt.Println("verbose", verbose)

	case strings.HasPrefix(c, `\f`): // format [output format]
		if len(params) > 0 {
			switch params[0] {
			case "csv", "tabs", "Tabs", "table", "Table":
				tformat = params[0]

			default:
				fmt.Println("Invalid output format: %v", params[0])
			}
			if silent {
				return
			}
		}
		fmt.Println("format", tformat)
	case strings.HasPrefix(c, `\E`): // echo [text]
		if len(cmd) > 3 {
			cmd = cmd[3:]
		} else {
			cmd = ""
		}
		fmt.Println(cmd)

	case strings.HasPrefix(c, `\o`): // output [file|-]
		if len(params) > 0 {
			if params[0] == "-" {
				if output != os.Stdout {
					output.Sync()
					output.Close()
				}

				output = os.Stdout
				outputName = stdoutName
			} else if f, err := os.Create(params[0]); err != nil {
				fmt.Println(err)
			} else {
				if output != os.Stdout {
					output.Sync()
					output.Close()
				}

				output = f
				outputName = params[0]
			}
		}

		fmt.Println("output", outputName)

	default:
		fmt.Printf("unknown command %v\n", c)
		fmt.Println(help)
	}
}
