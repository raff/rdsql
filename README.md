[![Go Documentation](http://godoc.org/github.com/raff/rdsql?status.svg)](http://godoc.org/github.com/raff/rdsql)
[![Go Report Card](https://goreportcard.com/badge/github.com/raff/rdsql)](https://goreportcard.com/report/github.com/raff/rdsql)

# rdsql
A SQL client for AWS Aurora serverless using rds-data API

Package usage:

    import "github.com/raff/rdsql"

    awscfg := rdsql.GetAWSConfig(profile, debug)
    client := rdsql.ClientWithOptions(awscfg, resourceArn, secretArn, dbName)

    ch := make(chan os.Signal, 1)

    tid, err := client.BeginTransaction(ch)

    params := map[string]interface{} {
        "p1": "value1",
        "p2", 42,
    }

    res, dberr = client.ExecuteStatement("SELECT * FROM table WHERE col1 = :p1 AND col2 < :p2", params, tid, ch)

    //
    // you can also use:
    //   client.EndTransaction(tid, dberr == nil, ch)
    //

    if dberr != nil {
        client.RollbackTransaction(tid, ch)
    } else {
        client.CommitTransaction(tid, ch)
    }

Command usage:

    rdsql [options] [sql statement]
      -continue
            continue after timeout (for DDL statements) (default true)
      -csv
            print output as csv
      -database string
            database
      -debug
            enable debugging
      -elapsed
            print elapsed time
      -params string
            query parameters (comma separated list of name=value pair)
      -profile string
            AWS profile
      -resource string
            resource ARN
      -secret string
            resource secret
      -timeout duration
            request timeout (default 2m0s)
      -transaction
            wrap full session in a remote transaction
      -verbose
            log statements before execution

Environment variables:

    RDS_RESOURCE (database cluster resource ARN, same as -resource)
    RDS_SECRET (database resource secret ARN, same as -secret)
    RDS_PROFILE (AWS account profile, same as -profile)
