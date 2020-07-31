# rdsql
A SQL client for AWS Aurora serverless using rds-data API

Library usage:

    import "github.com/raff/rdsql"

    awscfg := rdsql.GetAWSConfig(profile, debug)
    client := rdsql.GetRDSClient(awscfg, resourceArn, secretArn, dbName)

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
      -csv
            print output as csv
      -database string
            database
      -debug
            enabled debugging
      -profile string
            AWS profile
      -resource string
            resource ARN
      -secret string
            resource secret
      -t	
            print elapsed time
      -timeout duration
            context timeout (default 5m0s)

Environment variables:

    RDS_RESOURCE (database cluster resource ARN, same as -resource)
    RDS_SECRET (database resource secret ARN, same as -secret)
    RDS_PROFILE (AWS account profile, same as -profile)
