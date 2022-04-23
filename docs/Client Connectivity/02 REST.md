# REST API

!!! warning
    This is not currently not available

## Submit a Query (asynchronous)

This API submits a SQL query and receives a Job reference in return.

**End Point**

~~~
[POST] /api/v1/job
~~~

**Request Body**

~~~
{
    "sql": String
}
~~~

**Parameters**

Parameter | Type   | Description
--------- | ------ | ------------------------------------------
sql       | String | Represents the SQL query you want to run.

**Response**

Returns a Job ID for the query. Monitoring of the job status and fetching results needs to be completed using the Jobs endpoint.

~~~
{
  "id": string
}
~~~

## Run a Query (synchronous)

This API submits a SQL query and waits for the result before returning.

**End Point**

~~~
[POST] /api/v1/query
~~~

**Request Body**

~~~
{
    "sql": String
}
~~~

**Parameters**

Parameter | Type   | Description
--------- | ------ | ------------------------------------------
sql       | String | Represents the SQL query you want to run.


## Job Status

This API retrieves information about a job.

**End Point**

~~~
[GET] /api/v1/job/{id}
~~~

**Parameters**

Parameter | Type   | Description
--------- | ------ | ------------------------------------------
id        | String | The reference for the Job to retrieve the status for.

**Response**

Returns a Job ID for the query. Monitoring of the job status and fetching results needs to be completed using the Jobs endpoint.

~~~
{
  "id": string,
  "jobState": string [PENDING, RUNNING, FAILED, PARTIAL, COMPLETED],
  "startedAt": string (ISO 8601 timestamp),
  "statusAt": string (ISO 8601 timestamp)
}
~~~

**Response Fields**

Parameter | Description
--------- | -------------------------------------------------------------------
id        | The reference for the Job to retrieve the status for.
jobState  | The current state of the Job, either `PENDING`, `RUNNING`, `FAILED`, `PARTIAL` or `COMPLETED`
startedAt | IS0 8601 date (example: `2022-04-21T18:49:13Z`) representing the datetime the query was started.
statusAt  | IS0 8601 date (example: `2022-04-21T18:49:13Z`) representing the datetime the current status was active from.

**Job Statuses**

State       | Meaning
----------- | -----------------------------------
`PENDING`   | Job has not been run yet
`RUNNING`   | Job is currently running
`FAILED`    | Job has failed to complete
`PARTIAL`   | Job has completed at least one page of results
`COMPLETED` | Job has completed

**HTTP Response Codes**

Code  | Meaning
----- | ------------------------------------
`200` | Successful
`403` | The User does not have permisson to this Job
`404` | The Job could not be found

## Job Result

This API retrieves the results of a Job.

**End Point**

~~~
[GET] /api/v1/job/{id}/results?offset={offset}&limit={limit}
~~~

**Parameters**

Parameter | Type   | Description
--------- | ------ | ------------------------------------------
id        | String | The reference for the Job to retrieve the status for.
offset    | Number | The first record to return. Default 0.
limit     | Number | The maximum number of records to return. Maximum 1000, Default 1000.

**Response**

Returns the results of a `COMPLETED` Job.

~~~
{
  "id": string,
  "stats", dictionary,
  "schema": [FieldSchema],
  "rows": [JSON]
}
~~~

**Response Fields**

Parameter | Description
--------- | -------------------------------------------------------------------


**HTTP Response Codes**

Code  | Meaning
----- | ------------------------------------
`200` | Successful
`403` | The User does not have permisson to this Job
`404` | The Job could not be found
`409` | The Job is not in a `COMPLETED` state