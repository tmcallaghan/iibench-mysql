iibench-mysql
===============

iiBench Benchmark for MySQL / Percona / MariaDB


Requirements
=====================

* Java 1.6 or 1.7
* The MySQL Java connector must exist and be in the CLASSPATH, as in "export CLASSPATH=/home/tcallaghan/java_goodies/mysql-connector-java-5.1.30-bin.jar:.".
* This example assumes that you already have a MySQL/Percona/MariaDB server running on the same machine as the iiBench client application.
* You can connect a different server or port by editing the run.simple.bash script. 
* You must edit the run.simple.bash script to specific a different user/password/database.


Running the benchmark
=====================

In the default configuration the benchmark will run for 1 hour, or 1 billion inserts, whichever comes first, using 4 insertion threads.

To run:

```bash
git clone https://github.com/tmcallaghan/iibench-mysql.git
cd iibench-mysql

```

Edit run.simple.bash to match your environment. You will most likely want to change the server/port and credentials for your database.

```bash
./run.simple.bash

```