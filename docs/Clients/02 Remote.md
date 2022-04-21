# HTTP API

!!! warning
    This is not supported

## Submit a Query

This API submits a SQL query

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

## Get Job Result