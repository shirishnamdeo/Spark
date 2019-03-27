
spark2-shell \
    --master yarn \
    --dleploy-mode client \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.scheduler.minRegisteredResourcesRatio=1 \
    --conf spark.cores.max=31 \
    --conf spark.kryoserializer.buffer.max-2000M \
    --conf spark.yarn. principal=<tech-account>@GBL.AD.HEDANI.NET \
    --conf spark.yarn. keytab=<keytab_filename>.keytab\
    --conf spark.yarn. access. hadoopFileSystems=hdfs:://<namenode_address>:<port> \
    --name "Shirish_spark2_shell" \
    --driver-memory 3G \
    --driver-cores 2 \
    --num-executors 5 \
    --executor-cores 6 \
    --executor-memory 86 \
    -- total-executor-cores 30 \
    --jars <jars_to_pass_to_spark_JVM>



-- spark2-shell --jars sparkling-water-assembly-1.6.13.1.jar
-- Importing h2o here also works