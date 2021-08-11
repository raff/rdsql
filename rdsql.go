// Package rdsql implements some methods to access an RDS Aurora Servless DB cluster via RDS DataService
//
// See https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html
package rdsql

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
)

var PingRetries = 5
var Verbose = true

// GetAWSConfig return an aws.Config profile
func GetAWSConfig(profile string, debug bool) aws.Config {
	var configs []func(*config.LoadOptions) error
	if profile != "" {
		configs = append(configs, config.WithSharedConfigProfile(profile))
	}

	configs = append(configs, config.WithLogConfigurationWarnings(debug))

	awscfg, err := config.LoadDefaultConfig(context.TODO(), configs...)
	if err != nil {
		log.Fatalf("AWS configuration: %v", err)
	}

	if debug {
		awscfg.ClientLogMode = aws.LogSigning | aws.LogRetries | aws.LogRequest | aws.LogResponse
	}

	return awscfg
}

// Client wraps *rdsdata.Client and client configuration (ResourceArn, SecretArn, etc...)
type Client struct {
	client *rdsdata.Client

	ResourceArn string
	SecretArn   string
	Database    string

	Timeout  time.Duration
	Continue bool // ContinueAfterTimeout
}

// ClientWithURI creates an instance of Client given an rdsql URI
//
// Format: rdsql:{profile};{resource};{secret};{database}
func ClientWithURI(uri string, debug bool) *Client {
	if !strings.HasPrefix(uri, "rdsql:") {
		log.Fatal("Not a valid rdsql URN")
	}

	parts := strings.Split(uri[6:], ";")
	if len(parts) != 3 && len(parts) != 4 {
		log.Fatal("Not a valid rdsql URN")
	}

	if len(parts) == 3 {
		parts = append(parts, "")
	}

	config := GetAWSConfig(parts[0], debug)
	return ClientWithOptions(config, parts[1], parts[2], parts[3])
}

// ClientWithOptions creates an instance of Client given a list of options
func ClientWithOptions(config aws.Config, res, secret, db string) *Client {
	if res == "" {
		log.Fatal("missing resource ARN")
	}

	if secret == "" {
		log.Fatal("missing secret ARN")
	}

	return &Client{
		client:      rdsdata.NewFromConfig(config),
		ResourceArn: res,
		SecretArn:   secret,
		Database:    db,
	}
}

// BeginTransaction executes rdsdata BeginTransaction
func (c *Client) BeginTransaction(terminate chan os.Signal) (string, error) {
	ctx, cancel := makeContext(c.Timeout, terminate)
	defer cancel()

	res, err := c.client.BeginTransaction(ctx, &rdsdata.BeginTransactionInput{
		Database:    StringOrNil(c.Database),
		ResourceArn: aws.String(c.ResourceArn),
		SecretArn:   aws.String(c.SecretArn),
	})

	if res == nil {
		return "", err
	}

	return aws.ToString(res.TransactionId), err
}

// CommitTransaction executes rdsdata CommitTransaction
func (c *Client) CommitTransaction(tid string, terminate chan os.Signal) (string, error) {
	ctx, cancel := makeContext(c.Timeout, terminate)
	defer cancel()

	go func() {
		select {
		case <-terminate:
			cancel()

		case <-ctx.Done():
			if err := ctx.Err(); err != nil && err != context.Canceled {
				if Verbose {
					log.Println("context error:", err)
				}
			}
		}
	}()

	res, err := c.client.CommitTransaction(ctx, &rdsdata.CommitTransactionInput{
		ResourceArn:   aws.String(c.ResourceArn),
		SecretArn:     aws.String(c.SecretArn),
		TransactionId: aws.String(tid),
	})

	if res == nil {
		return "", err
	}

	return aws.ToString(res.TransactionStatus), err
}

// RollbackTransaction executes rdsdata RollbackTransaction
func (c *Client) RollbackTransaction(tid string, terminate chan os.Signal) (string, error) {
	ctx, cancel := makeContext(c.Timeout, terminate)
	defer cancel()

	res, err := c.client.RollbackTransaction(ctx, &rdsdata.RollbackTransactionInput{
		ResourceArn:   aws.String(c.ResourceArn),
		SecretArn:     aws.String(c.SecretArn),
		TransactionId: aws.String(tid),
	})

	if res == nil {
		return "", err
	}

	return aws.ToString(res.TransactionStatus), err
}

// EndTransaction executes either a commit or a rollback request
func (c *Client) EndTransaction(tid string, commit bool, terminate chan os.Signal) (string, error) {
	if commit {
		return c.CommitTransaction(tid, terminate)
	}

	return c.RollbackTransaction(tid, terminate)
}

// Results is an alias for *rdsdata.ExecuteStatementOutput
type Results = *rdsdata.ExecuteStatementOutput

// ColumnMetadata is an alias for rdsdata types.ColumnMetadata
type ColumnMetadata = types.ColumnMetadata

// Field is an alias for rdsdata types.Field
type Field = types.Field

type FieldMemberIsNull = types.FieldMemberIsNull
type FieldMemberBooleanValue = types.FieldMemberBooleanValue
type FieldMemberStringValue = types.FieldMemberStringValue
type FieldMemberLongValue = types.FieldMemberLongValue

// ExecuteStatement executes a SQL statement and return Results
//
// parameters could be passed as :par1, :par2... in the SQL statement
// with associated parameter list in the request
func (c *Client) ExecuteStatement(stmt string, params map[string]interface{}, transactionId string, terminate chan os.Signal) (Results, error) {
	ctx, cancel := makeContext(c.Timeout, terminate)
	defer cancel()

	res, err := c.client.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		Database:              StringOrNil(c.Database),
		ResourceArn:           aws.String(c.ResourceArn),
		SecretArn:             aws.String(c.SecretArn),
		Sql:                   aws.String(stmt),
		Parameters:            makeParams(params),
		IncludeResultMetadata: true,
		TransactionId:         StringOrNil(transactionId),
		ContinueAfterTimeout:  c.Continue,
		// Schema
		// ResultSetOptions
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

// Ping verifies the connection to the database is still alive.
func (c *Client) Ping(terminate chan os.Signal) (err error) {
	for i := 0; i < PingRetries; i++ {
		if i > 0 {
			// if Verbose {
			//  log.Println(err)
			// }

			time.Sleep(time.Second)

			if Verbose {
				log.Println("RETRY", i)
			}
		}

		_, err = c.ExecuteStatement("SELECT CURRENT_TIMESTAMP", nil, "", terminate)
		// assume BadRequestException is because Aurora serverless is restarting and retry

		if err == nil {
			break
		}

		if !strings.Contains(err.Error(), "BadRequestException") {
			break
		}
	}

	if err != nil && Verbose {
		log.Printf("ERROR %T - %#v", err, err)
	}

	return err
}

func makeParams(params map[string]interface{}) []types.SqlParameter {
	if len(params) == 0 {
		return nil
	}

	plist := make([]types.SqlParameter, 0, len(params))

	for k, v := range params {
		var field types.Field

		switch t := v.(type) {
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

		plist = append(plist, types.SqlParameter{Name: aws.String(k), Value: field})
	}

	return plist
}

func makeContext(timeout time.Duration, terminate chan os.Signal) (ctx context.Context, cancel context.CancelFunc) {
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.TODO(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.TODO())
	}

	go func() {
		select {
		case <-terminate:
			cancel()

		case <-ctx.Done():
			if err := ctx.Err(); err != nil && err != context.Canceled {
				if Verbose {
					log.Println("context error:", err)
				}
			}
		}
	}()

	return ctx, cancel
}

// StringOrNil return nil for an empty string or aws.String
func StringOrNil(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
}
