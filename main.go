package main

import (
	"context"
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
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"

	"github.com/peterh/liner"
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

	flag.Parse()

	awscfg, err := external.LoadDefaultAWSConfig(external.WithSharedConfigProfile(profile))
	if err != nil {
		log.Fatalf("AWS configuration: %v", err)
	}

	if debug {
		awscfg.LogLevel = aws.LogDebugWithSigning // aws.LogDebug
	}

	if resourceArn == "" {
		log.Fatal("missing resource ARN")
	}

	if secretArn == "" {
		log.Fatal("missing secret ARN")
	}

	client := rdsdata.New(awscfg)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer signal.Stop(c)

	var transactionId string
	var dberr error

	if *trans {
		if tid, err := beginTransaction(client, c); err != nil {
			fmt.Println("BEGIN TRANSACTION:", err)
			return
		} else {
			fmt.Println("BEGIN TRANSACTION:", tid)
			transactionId = tid
		}

		defer func() {
			res, err := endTransaction(client, transactionId, dberr == nil, c)
			if err != nil {
				fmt.Println("END TRANSACTION:", err)
			} else {
				fmt.Println("END TRANSACTION:", res)
			}
		}()
	}

	if flag.NArg() != 0 {
		stmt := strings.Join(flag.Args(), " ")

		dberr = exec(client, stmt, transactionId, *csv, c)
		if dberr != nil {
			fmt.Println(dberr)
		}

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

		dberr = exec(client, stmt, transactionId, *csv, c)
		if dberr != nil {
			fmt.Println(dberr)

			if script { // for scripts, break at first error
				break
			}
		}
	}
}

func makeContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return context.WithCancel(ctx)
}

func printElapsed(prefix string) func() {
	t := time.Now()

	return func() {
		fmt.Println(prefix, "Elapsed:", time.Since(t))
	}
}

func stringOrNil(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
}

// parameters could be passed as :par1, :par2... in the SQL statement
// with associated parameter list in the request

func exec(client *rdsdata.Client, stmt, transactionId string, asCsv bool, c chan os.Signal) error {
	start := time.Now()

	var cw *csv.Writer
	if asCsv {
		cw = csv.NewWriter(os.Stdout)
		defer cw.Flush()
	}

	ctx, cancel := makeContext(timeout)
	defer cancel()

	go func() {
		select {
		case <-c:
			cancel()

		case <-ctx.Done():
		}
	}()

	res, err := client.ExecuteStatementRequest(&rdsdata.ExecuteStatementInput{
		Database:              aws.String(dbName),
		ResourceArn:           aws.String(resourceArn),
		SecretArn:             aws.String(secretArn),
		Sql:                   aws.String(stmt),
		IncludeResultMetadata: aws.Bool(true),
		TransactionId:         stringOrNil(transactionId),

		// ContinueAfterTimeout
		// IncludeResultMetadata
		// Schema
		// Parameters
		// ResultSetOptions
		// TransactionId
	}).Send(ctx)

	if elapsed {
		fmt.Println("EXEC Elapsed:", time.Since(start))
	}

	if err != nil {
		return err
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
		return nil
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
	return nil
}

func beginTransaction(client *rdsdata.Client, c chan os.Signal) (string, error) {
	ctx, cancel := makeContext(timeout)
	defer cancel()

	go func() {
		select {
		case <-c:
			cancel()

		case <-ctx.Done():
		}
	}()

	if elapsed {
		printElapsed("BEGIN TRANSACTION")()
	}

	res, err := client.BeginTransactionRequest(&rdsdata.BeginTransactionInput{
		Database:    aws.String(dbName),
		ResourceArn: aws.String(resourceArn),
		SecretArn:   aws.String(secretArn),
	}).Send(ctx)

	if err != nil {
		return "", err
	}

	return aws.StringValue(res.TransactionId), nil
}

func endTransaction(client *rdsdata.Client, tid string, commit bool, c chan os.Signal) (string, error) {
	ctx, cancel := makeContext(timeout)
	defer cancel()

	go func() {
		select {
		case <-c:
			cancel()

		case <-ctx.Done():
		}
	}()

	if elapsed {
		printElapsed("END TRANSACTION")()
	}

	if commit {
		res, err := client.CommitTransactionRequest(&rdsdata.CommitTransactionInput{
			ResourceArn:   aws.String(resourceArn),
			SecretArn:     aws.String(secretArn),
			TransactionId: aws.String(tid),
		}).Send(ctx)

		if err != nil {
			return "", err
		}

		return aws.StringValue(res.TransactionStatus), nil
	} else { // rollback
		res, err := client.RollbackTransactionRequest(&rdsdata.RollbackTransactionInput{
			ResourceArn:   aws.String(resourceArn),
			SecretArn:     aws.String(secretArn),
			TransactionId: aws.String(tid),
		}).Send(ctx)

		if err != nil {
			return "", err
		}

		return aws.StringValue(res.TransactionStatus), nil
	}
}

func format(f rdsdata.Field) string {
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
