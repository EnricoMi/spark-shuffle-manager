# Spark Backup Shuffle Manager (WIP)

A shuffle manager that copies shuffle data to a directory accessible via the Hadoop filesystem abstraction, e.g. S3.
When executors become unavailable, their shuffle data is being read off that backup directory.

## Build

Simply run
```
mvn package
```
and add `target/spark-backup-shuffle-manager_*.jar` to your Spark environment.

Note: This jar has to be deployed to your Spark nodes prior to spinning up executor processes.

## Use
Add the following options to your Spark job:
```
spark.shuffle.manager=org.apache.spark.shuffle.BackupShuffleManager
spark.shuffle.backup.path=...
spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.BackupShuffleDataIO
```

Using a S3A bucket, add the following:
```
spark.shuffle.backup.path=s3a://bucket/path
spark.hadoop.fs.s3a.access.key=ACCESSKEY
spark.hadoop.fs.s3a.secret.key=SECRETKEY
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.path.style.access=true
```
