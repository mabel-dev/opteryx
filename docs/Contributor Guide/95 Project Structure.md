# Project Structure

## Folder Structure

Opteryx's repository folder structure is described below:

~~~
/
 ├── connectors/         <- modules to connect to data sources
 ├── functions/          <- modules to execute functions within SQL statements
 ├── managers/           <- libraries responsible for key functional units
 │   ├── cache/          <- modules implementing the caching mechanism
 │   ├── expression/     <- modules implementing expression evaluation
 │   ├── process/        <- modules implementing process management
 │   ├── query/           
 │   │   └── planner/    <- modules implementing query planning 
 │   └── schemes/        <- modules implementing storage schemes
 ├── models/             <- internal data models
 ├── operators/          <- modules implementing steps in the query plan
 ├── samples/            <- sample data
 ├── third_party/        <- third party code
 │   ├── distogram/ 
 │   ├── fuzzy/   
 │   ├── hyperloglog/  
 │   ├── pyarrow_ops/ 
 │   └── ...  
 ├── utils/              <- helper libraries
 └── ...       
~~~

