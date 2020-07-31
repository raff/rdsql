// Package rdsql implements some methods to access an RDS Aurora Servless DB cluster via RDS DataService
//
// See https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html
package rdsql

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
)

// GetAWSConfig return an aws.Config profile
func GetAWSConfig(profile string, debug bool) aws.Config {
	var configs []external.Config
	if profile != "" {
		configs = append(configs, external.WithSharedConfigProfile(profile))
	}

	awscfg, err := external.LoadDefaultAWSConfig(configs...)
	if err != nil {
		log.Fatalf("AWS configuration: %v", err)
	}

	if debug {
		awscfg.LogLevel = aws.LogDebugWithSigning // aws.LogDebug
	}

	return awscfg
}

// StringOrNil return nil for an empty string or aws.String
func StringOrNil(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
}

// RDSClient wraps *rdsdata.Client and client configuration (ResourceArn, SecretArn, etc...)
type RDSClient struct {
	client *rdsdata.Client

	ResourceArn string
	SecretArn   string
	Database    string

	Timeout time.Duration
}

// GetRDSClient creates an instance of RDSClient
func GetRDSClient(config aws.Config, res, secret, db string) *RDSClient {
	if res == "" {
		log.Fatal("missing resource ARN")
	}

	if secret == "" {
		log.Fatal("missing secret ARN")
	}

	return &RDSClient{
		client:      rdsdata.New(config),
		ResourceArn: res,
		SecretArn:   secret,
		Database:    db,
	}
}

// BeginTransaction executes rdsdata BeginTransactionRequest
func (c *RDSClient) BeginTransaction(terminate chan os.Signal) (string, error) {
	ctx, cancel := makeContext(c.Timeout)
	defer cancel()

	go func() {
		select {
		case <-terminate:
			cancel()

		case <-ctx.Done():
		}
	}()

	res, err := c.client.BeginTransactionRequest(&rdsdata.BeginTransactionInput{
		Database:    StringOrNil(c.Database),
		ResourceArn: aws.String(c.ResourceArn),
		SecretArn:   aws.String(c.SecretArn),
	}).Send(ctx)

	if err != nil {
		return "", err
	}

	return aws.StringValue(res.TransactionId), nil
}

// CommitTransaction executes rdsdata CommitTransactionRequest
func (c *RDSClient) CommitTransaction(tid string, terminate chan os.Signal) (string, error) {
	ctx, cancel := makeContext(c.Timeout)
	defer cancel()

	go func() {
		select {
		case <-terminate:
			cancel()

		case <-ctx.Done():
		}
	}()

	res, err := c.client.CommitTransactionRequest(&rdsdata.CommitTransactionInput{
		ResourceArn:   aws.String(c.ResourceArn),
		SecretArn:     aws.String(c.SecretArn),
		TransactionId: aws.String(tid),
	}).Send(ctx)

	return aws.StringValue(res.TransactionStatus), err
}

// RollbackTransaction executes rdsdata RollbackTransactionRequest
func (c *RDSClient) RollbackTransaction(tid string, terminate chan os.Signal) (string, error) {
	ctx, cancel := makeContext(c.Timeout)
	defer cancel()

	go func() {
		select {
		case <-terminate:
			cancel()

		case <-ctx.Done():
		}
	}()

	res, err := c.client.RollbackTransactionRequest(&rdsdata.RollbackTransactionInput{
		ResourceArn:   aws.String(c.ResourceArn),
		SecretArn:     aws.String(c.SecretArn),
		TransactionId: aws.String(tid),
	}).Send(ctx)

	return aws.StringValue(res.TransactionStatus), err
}

// EndTransaction executes either a commit or a rollback request
func (c *RDSClient) EndTransaction(tid string, commit bool, terminate chan os.Signal) (string, error) {
	if commit {
		return c.CommitTransaction(tid, terminate)
	}

	return c.RollbackTransaction(tid, terminate)
}

// Results is an alias for *rdsdata.ExecuteStatementOutput
type Results = *rdsdata.ExecuteStatementOutput

// ColumnMetadata is an alias for rdsdata.ColumnMetadata
type ColumnMetadata = rdsdata.ColumnMetadata

// Field is an alias for rdsdata.Field
type Field = rdsdata.Field

// ExecuteStatement executes a SQL statement and return Results
//
// parameters could be passed as :par1, :par2... in the SQL statement
// with associated parameter list in the request
func (c *RDSClient) ExecuteStatement(stmt string, params map[string]interface{}, transactionId string, terminate chan os.Signal) (Results, error) {
	ctx, cancel := makeContext(c.Timeout)
	defer cancel()

	go func() {
		select {
		case <-terminate:
			cancel()

		case <-ctx.Done():
		}
	}()

	res, err := c.client.ExecuteStatementRequest(&rdsdata.ExecuteStatementInput{
		Database:              StringOrNil(c.Database),
		ResourceArn:           aws.String(c.ResourceArn),
		SecretArn:             aws.String(c.SecretArn),
		Sql:                   aws.String(stmt),
		Parameters:            makeParams(params),
		IncludeResultMetadata: aws.Bool(true),
		TransactionId:         StringOrNil(transactionId),
		// ContinueAfterTimeout
		// Schema
		// ResultSetOptions
	}).Send(ctx)

	if err != nil {
		return nil, err
	}

	return res.ExecuteStatementOutput, nil
}

// Ping verifies the connection to the database is still alive.
func (c *RDSClient) Ping(terminate chan os.Signal) error {
	_, err := c.ExecuteStatement("SELECT CURRENT_TIMESTAMP", nil, "", terminate)
	return err
}

func makeParams(params map[string]interface{}) []rdsdata.SqlParameter {
	if len(params) == 0 {
		return nil
	}

	plist := make([]rdsdata.SqlParameter, 0, len(params))

	for k, v := range params {
		var field rdsdata.Field

		switch t := v.(type) {
		case nil:
			field.IsNull = aws.Bool(true)

		case bool:
			field.BooleanValue = aws.Bool(t)

		case string:
			field.StringValue = aws.String(t)

		case int:
			field.LongValue = aws.Int64(int64(t))

		case int8:
			field.LongValue = aws.Int64(int64(t))

		case int16:
			field.LongValue = aws.Int64(int64(t))

		case int64:
			field.LongValue = aws.Int64(t)

		case float32:
			field.DoubleValue = aws.Float64(float64(t))

		case float64:
			field.DoubleValue = aws.Float64(t)

		case time.Time:
			field.StringValue = aws.String(t.Format("'2006-01-02 15:04:05'"))

		default:
			log.Fatalf("unsupported parameter type %T: %#v", t, t)
		}

		plist = append(plist, rdsdata.SqlParameter{Name: aws.String(k), Value: &field})
	}

	return plist
}

func makeContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}

	return context.WithCancel(ctx)
}
