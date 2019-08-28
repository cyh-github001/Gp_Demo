package com.ETL.Tags




object HbaseSpark {

//  Logger.getLogger("org").setLevel(Level.ERROR)
//
//  val spark = SparkSession.builder().master("local[*]").appName(s"${this.getClass.getSimpleName}").getOrCreate()
//  val sc = spark.sparkContext
//
//  val table = "student"
//  val family = "info"
//  val column1 = "name"
//  val column2 = "age"
//
//  val conf = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.property.clientPort", "2181")
//  conf.set("spark.executor.memory", "3000m")
//  conf.set("hbase.zookeeper.quorum", "192.168.1.11:2181,192.168.1.12:2181")
//  conf.set("zookeeper.znode.parent", "/hbase-unsecure")
//  conf.set(TableInputFormat.INPUT_TABLE, table)
//
//  val startRowkey = "1"
//  val endRowkey = "4"
//
//  val scan = new Scan(Bytes.toBytes(startRowkey), Bytes.toBytes(endRowkey))
//  scan.setCacheBlocks(false)
//
//  val proto = ProtobufUtil.toScan(scan)
//  val scanToString = Base64.encodeBytes(proto.toByteArray)
//  conf.set(TableInputFormat.SCAN, scanToString)
//
//  val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
//    classOf[ImmutableBytesWritable],
//    classOf[Result])
//  val counts = hbaseRdd.count()
//  println(counts)
//
//  //读取hbase信息,扫描信息
//  hbaseRdd.foreach { case (_, result) => {
//    val key: String = Bytes.toString(result.getRow)
//    val name = Bytes.toString(result.getValue(family.getBytes(), column1.getBytes()))
//    val age = Bytes.toString(result.getValue(family.getBytes(), column2.getBytes()))
//    println(s"the name is $name,the age is $age")
//  }
//  }
//
//  //写入数据到hbase
//  //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
//  val jobConf = new JobConf(conf)
//  jobConf.setOutputFormat(classOf[TableOutputFormat])
//  jobConf.set(TableOutputFormat.OUTPUT_TABLE, table)
//  val dataRdd = sc.makeRDD(Array("111,jack,15", "222,Lily,16", "333,mike,16"))
//  val rdd = dataRdd.map(_.split(',')).map(arr => {
//    val put = new Put(Bytes.toBytes(arr(0)))
//    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
//    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
//    (new ImmutableBytesWritable, put)
//  })
//  rdd.saveAsHadoopDataset(jobConf)
//
//  spark.stop()

}
