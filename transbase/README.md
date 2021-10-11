<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Transbase Driver for YCSB using its JDBC interface
This driver enables YCSB to work with the Transbase DBMS accessible via the JDBC protocol.

## Getting Started
### 1. Set up your database
This driver will connect to Transbase databases that use the JDBC protocol.
Please refer to the Transbase documentation on information on how to install, configure and start your system.
It can be found here: https://www.transbase.de.

### 2. Set up YCSB
You can clone the YCSB project and compile it to stay up to date with the latest changes. Or you can just download the latest release and unpack it. Either way, instructions for doing so can be found here: https://github.com/brianfrankcooper/YCSB.

### 3. Configure your database and table.
You can name your database what ever you want, you will need to provide the database name in the JDBC connection string.

This interface will create the table to be used in its init() function if it does not exist.
It will be created according to YCSB core properties:

The default is to just use 'usertable' as the table name.
The expected table definition will be similar to the following:

```sql
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(*) PRIMARY KEY,
	field0 STRING, field1 STRING,
	field2 STRING, field3 STRING,
	field4 STRING, field5 STRING,
	field6 STRING, field7 STRING,
	field8 STRING, field9 STRING
);
```

Key take aways:

* The primary key field needs to be named YCSB_KEY
* The other fields need to be prefixed with field and count up starting from 0
* Add the same number of fields as you specify in the YCSB core properties, default is 10.
* The type of the fields STRING is equivalent to VARCHAR(*).
* The length of the fields is limited only by the fact that the entire record needs to fit in a single database page.

### 4. Configure YCSB connection properties
You need to set the following connection configurations:

```sh
db.url=jdbc:transbase://127.0.0.1:2024/ycsb
db.user=tbadmin
db.passwd=tbadmin
```

Be sure to use a valid JDBC connection string and credentials to your database.

You can add these to your workload configuration or a separate properties file and specify it with ```-P``` or you can add the properties individually to your ycsb command with ```-p```.

### 5. Add the Transbase JDBC Driver to the classpath
There are several ways to do this, but a couple easy methods are to put a copy of your Driver jar in ```YCSB_HOME/jdbc-binding/lib/``` or just specify the path to your Driver jar with ```-cp``` in your ycsb command.

### 6. Running a workload
Before you can actually run the workload, you need to "load" the data first.

```sh
bin/ycsb load jdbc -P workloads/workloada -P db.properties -cp tbjdbc.jar
```

Then, you can run the workload:

```sh
bin/ycsb run transbase -P workloads/workloada -P db.properties -cp tbjdbc.jar
```

## Configuration Properties

```sh
db.url=jdbc:transbase://127.0.0.1:2024/ycsb	# The Database connection URL.
db.user=tbadmin								# User name for the connection.
db.passwd=tbadmin							# Password for the connection.
db.batchsize=1000             # The batch size for doing batched inserts. Defaults to 0. Set to >0 to use batching.
jdbc.batchupdateapi=true      # Use addBatch()/executeBatch() instead of executeUpdate() for writes (default: false)
jdbc.autocommit=true		  # The JDBC connection auto-commit property for the driver.
```

Please refer to https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for all other YCSB core properties.

