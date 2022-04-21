# HTTP API

!!! warning
    This is not currently not available

## Submit a Query

This API submits a SQL query.

**End Point**

~~~
[POST] /api/v1/submit
~~~

**Request Body**

~~~json
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

~~~json
{
  "id": String
}
~~~

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

~~~json
{
  "id": String
}
~~~

**Response Fields**

Parameter | Type   | Description
--------- | ------ | ------------------------------------------
id        | String | The reference for the Job to retrieve the status for.
jobState  | 
startedAt |
endedAt   |


## Get Job Result