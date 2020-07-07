# rdsql
A SQL client for AWS Aurora serverless using rds-data API

Usage:

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
    RDS_SECERT (database resource secret ARN, same as -secret)
    RDS_PROFILE (AWS account profile, same as -profile)
