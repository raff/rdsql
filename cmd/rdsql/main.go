package main

import (
	"encoding/csv"
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

	"github.com/peterh/liner"
	"github.com/raff/rdsql"
)

var (
	resourceArn = os.Getenv("RDS_RESOURCE")
	secretArn   = os.Getenv("RDS_SECRET")
	dbName      = os.Getenv("RDS_DATABASE")
	profile     string
	elapsed     bool
	debug       bool

	timeout = 2 * time.Minute

	keywords = []string{
		"BEGIN",
		"START",
		"COMMIT",
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
		"INSERT",
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
	}
)

func init() {
	sort.Strings(keywords)

	profile = os.Getenv("RDS_PROFILE")
	if profile == "" {
		profile = os.Getenv("AWS_PROFILE")
	}
}

func main() {
	flag.StringVar(&resourceArn, "resource", resourceArn, "resource ARN")
	flag.StringVar(&secretArn, "secret", secretArn, "resource secret")
	flag.StringVar(&dbName, "database", dbName, "database")
	flag.StringVar(&profile, "profile", profile, "AWS profile")
	flag.DurationVar(&timeout, "timeout", timeout, "context timeout")
	flag.BoolVar(&elapsed, "elapsed", elapsed, "print elapsed time")
	flag.BoolVar(&debug, "debug", debug, "enable debugging")

	verbose := flag.Bool("verbose", false, "log statements before execution")
	csv := flag.Bool("csv", false, "print output as csv")
	trans := flag.Bool("transaction", false, "wrap full session in a remote transaction")
	fparams := flag.String("params", "", "query parameters (comma separated list of name=value pair)")

	flag.Parse()

	awscfg := rdsql.GetAWSConfig(profile, debug)
	client := rdsql.GetRDSClient(awscfg, resourceArn, secretArn, dbName)

	params := parseParams(*fparams)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer signal.Stop(c)

	var transactionId string
	var dberr error
	var res rdsql.Results

	if *trans {
		shouldPrint := printElapsed("BEGIN TRANSACTION:", elapsed)
		tid, err := client.BeginTransaction(c)
		shouldPrint()

		if err != nil {
			fmt.Println("BEGIN TRANSACTION:", err)
			return
		}

		fmt.Println("BEGIN TRANSACTION:", tid)
		transactionId = tid

		defer func() {
			shouldPrint := printElapsed("END TRANSACTION:", elapsed)

			if res, err := client.EndTransaction(transactionId, dberr == nil, c); err != nil {
				fmt.Println("END TRANSACTION:", err)
			} else {
				fmt.Println("END TRANSACTION:", res)
			}

			shouldPrint()
		}()
	}

	if flag.NArg() != 0 {
		stmt := strings.Join(flag.Args(), " ")

		shouldPrint := printElapsed("EXEC:", elapsed)
		res, dberr = client.ExecuteStatement(stmt, params, transactionId, c)
		shouldPrint()

		if dberr != nil {
			fmt.Println(dberr)
			fmt.Println("STMT:", stmt)
			return
		}

		printResults(res, *csv)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	/*
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
	*/

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

		stmt = strings.TrimSpace(stmt)
		if len(l) == 0 {
			continue
		}

		line.AppendHistory(stmt)

		if *verbose {
			fmt.Println("--", stmt)
		}

		shouldPrint := printElapsed("EXEC:", elapsed)
		res, dberr = client.ExecuteStatement(stmt, params, transactionId, c)
		shouldPrint()

		if dberr != nil {
			fmt.Println(dberr)
			fmt.Println("STMT:", stmt)

			if script { // for scripts, break at first error
				break
			}
		} else {
			printResults(res, *csv)
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

func printResults(res rdsql.Results, asCsv bool) {
	var cw *csv.Writer
	if asCsv {
		cw = csv.NewWriter(os.Stdout)
		defer cw.Flush()
	}

	if debug {
		if dmesg, err := json.MarshalIndent(res, "", " "); err == nil {
			fmt.Println("RESULT")
			fmt.Println(string(dmesg))
		}
	}

	nr := aws.Int64Value(res.NumberOfRecordsUpdated)
	if nr > 0 {
		fmt.Println("Updated", nr, "records")
	}

	cols := res.ColumnMetadata
	if len(cols) == 0 {
		return
	}

	csvRecord := make([]string, len(cols))
	for i := 0; i < len(cols); i++ {
		label := aws.StringValue(cols[i].Label)

		if asCsv {
			csvRecord[i] = label
		} else {
			if i != 0 {
				fmt.Print("\t")
			}
			fmt.Print(label)
		}
	}

	if asCsv {
		cw.Write(csvRecord)
	} else {
		fmt.Println()
	}

	for _, row := range res.Records {
		for i, r := range row {
			v := format(r)

			if asCsv {
				csvRecord[i] = v
			} else {
				if i != 0 {
					fmt.Print("\t")
				}
				fmt.Print(v)
			}
		}

		if asCsv {
			cw.Write(csvRecord)
		} else {
			fmt.Println()
		}

	}

	fmt.Println("Total", len(res.Records))
}

func format(f rdsql.Field) string {
	if aws.BoolValue(f.IsNull) {
		return "NULL"
	}

	if f.StringValue != nil {
		return aws.StringValue(f.StringValue)
	}

	if f.BooleanValue != nil {
		return strconv.FormatBool(aws.BoolValue(f.BooleanValue))
	}

	if f.LongValue != nil {
		return strconv.FormatInt(aws.Int64Value(f.LongValue), 10)
	}

	if f.DoubleValue != nil {
		return strconv.FormatFloat(aws.Float64Value(f.DoubleValue), 'f', -1, 64)
	}

	return f.String()
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
