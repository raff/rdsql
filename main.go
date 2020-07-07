package main

import (
	"context"
	"encoding/csv"
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
        profile string

	keywords = []string{
		"BEGIN",
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

	debug := flag.Bool("debug", false, "enabled debugging")
	elapsed := flag.Bool("t", false, "print elapsed time")
	csv := flag.Bool("csv", false, "print output as csv")
	timeout := flag.Duration("timeout", 5*time.Minute, "context timeout")

	flag.Parse()

	awscfg, err := external.LoadDefaultAWSConfig(external.WithSharedConfigProfile(profile))
	if err != nil {
		log.Fatalf("AWS configuration: %v", err)
	}

	if *debug {
		awscfg.LogLevel = aws.LogDebugWithSigning // aws.LogDebug
	}

	if resourceArn == "" {
		log.Fatal("missing resource ARN")
	}

	if secretArn == "" {
		log.Fatal("missing secret ARN")
	}

	//if dbName == "" {
	//        log.Fatal("missing database")
	//}

	client := rdsdata.New(awscfg)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer signal.Stop(c)

	if flag.NArg() != 0 {
		stmt := strings.Join(flag.Args(), " ")

		if err := exec(client, stmt, *elapsed, *csv, *timeout, c); err != nil {
			fmt.Println(err)
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

	var cmd string
	var multi bool

	prompt := map[bool]string{
		false: "> ",
		true:  ": ",
	}

	if _, err := liner.TerminalMode(); err != nil {
		prompt[false] = ""
		prompt[true] = ""
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
				cmd = ""
				continue
			} else {
				cmd = l
			}
		} else {
			if l == "]]]" {
				multi = false
			} else {
				cmd += " " + l
				continue
			}
		}

		cmd = strings.TrimSpace(cmd)
		if len(l) == 0 {
			continue
		}

		line.AppendHistory(cmd)

		err = exec(client, cmd, *elapsed, *csv, *timeout, c)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func exec(client *rdsdata.Client, stmt string, elapsed, asCsv bool, timeout time.Duration, c chan os.Signal) error {
	start := time.Now()

	var cw *csv.Writer
	if asCsv {
		cw = csv.NewWriter(os.Stdout)
		defer cw.Flush()
	}

	ctx := context.Background()
	var cancel func()

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

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

		// ContinueAfterTimeout
		// IncludeResultMetadata
		// Schema
		// Parameters
		// ResultSetOptions
		// TransactionId
	}).Send(ctx)

	if elapsed {
		fmt.Println("Elapsed time:", time.Since(start))
	}

	if err != nil {
		return err
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
