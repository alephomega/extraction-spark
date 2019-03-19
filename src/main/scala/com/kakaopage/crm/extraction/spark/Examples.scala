package com.kakaopage.crm.extraction.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Examples {

  val sc = {
    val conf = new SparkConf().setAppName("function-test").setMaster("local")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    sc.hadoopConfiguration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    sc
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()



    //    import spark.implicits._
    //
    //    var df = Seq(
    //      ("2019-02-01T00:00:00+07:00", Array(1, 2, 3), "a"),
    //      ("2019-02-02T00:00:00+07:00", Array(4, 0, 6), "b"),
    //      ("2019-02-03T00:00:00+07:00", Array(7, -8, 9), "c"),
    //      ("2019-02-04T00:00:00+07:00", Array(0, 0, 0), "d")
    //    ).toDF("at", "values", "str")
    //
    //    df = df.withColumn("xxx", struct("at", "values", "str"))
    //
    //    df.printSchema()
    //
    //    df = df.withColumn("filter-test", Functions.invoke(new Filter(new Value("xxx"), new Equals(new MaxOf(new Value("col2")), new Constant[Int](6)))))

    //    df.show(10, truncate = false)
    //
    //    df = df.withColumn("filter", Functions.invoke(new Filter(new Value("struct"), new Equal(new DiffTime(new Time(new Value("at"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), new Now(), TimeUnit.DAYS), new Constant[Int](28)))))



    //
    //    df = df.withColumn("diff", lit(Functions.invoke(new DiffTime(new Time(new Value("at"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), new Now(), TimeUnit.DAYS))))
    //    df = df.withColumn("elem_2", lit(Functions.invoke(new ElementAt(new Value("values"), 1))))
    //    df = df.withColumn("max", Functions.invoke(new MaxOf(new Value("values"))))
    //    df = df.withColumn("min", Functions.invoke(new MinOf(new Value("values"))))
    //    df = df.withColumn("contains", Functions.invoke(new Contains[Int](new Value("values"), 0)))
    //    df = df.withColumn("pos", Functions.invoke(new MaxOf(new Value("values"))).gt(0))
    //
    //    val elems = new util.ArrayList[extraction.Function]()
    //    elems.add(new Constant[String]("a"))
    //    elems.add(new Constant[String]("b"))
    //    elems.add(new Constant[String]("c"))
    //    df = df.withColumn("array", Functions.invoke(new ArrayOf(elems)))
    //


    //    df = df.filter(Predicates.run(new Equal(new DiffTime(new Time(new Value("at"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), new Now(), TimeUnit.DAYS), new Constant[Int](28))))

    //    df = df.withColumn(
    //      "sum",
    //      call(new SumOf(new Value("values")), df).asInstanceOf[Column]
    //    )


    val summary = spark.read.json("/home/johan/Documents/CRM/samples/test/test3.json")
    val desc = "{\"expressions\":[{\"variable\":\"s1\",\"operation\":{\"@Symbol\":\"σ\",\"condition\":{\"@Symbol\":\"∧\",\"_1\":[{\"@Symbol\":\"=\",\"_1\":{\"@FuncIdentifier\":\"val\",\"dataset\":\"events\",\"attribute\":\"s\"},\"_2\":{\"@FuncIdentifier\":\"const\",\"value\":\"IDN\"}},{\"@Symbol\":\"=\",\"_1\":{\"@FuncIdentifier\":\"val\",\"dataset\":\"events\",\"attribute\":\"ev\"},\"_2\":{\"@FuncIdentifier\":\"const\",\"value\":\"READING\"}},{\"@Symbol\":\"∈\",\"_1\":{\"@FuncIdentifier\":\"format\",\"time\":{\"@FuncIdentifier\":\"time\",\"text\":{\"@FuncIdentifier\":\"val\",\"dataset\":\"events\",\"attribute\":\"at\"}},\"pattern\":\"yyyy-MM-dd\",\"timezone\":\"Asia/Jakarta\"},\"_2\":{\"@FuncIdentifier\":\"const\",\"value\":[\"2018-12-31\",\"2019-01-01\"]}}]},\"_1\":{\"name\":\"summary\"}}},{\"variable\":\"s2\",\"operation\":{\"@Symbol\":\"π\",\"attributes\":[{\"@FuncIdentifier\":\"as\",\"function\":{\"@FuncIdentifier\":\"val\",\"attribute\":\"u\"},\"alias\":\"customer\"},{\"@FuncIdentifier\":\"as\",\"function\":{\"@FuncIdentifier\":\"val\",\"attribute\":\"ev\"},\"alias\":\"event\"},{\"@FuncIdentifier\":\"as\",\"function\":{\"@FuncIdentifier\":\"val\",\"attribute\":\"at\"},\"alias\":\"at\"},{\"@FuncIdentifier\":\"as\",\"function\":{\"@FuncIdentifier\":\"val\",\"attribute\":\"meta\"},\"alias\":\"meta\"}],\"_1\":{\"name\":\"s1\"}}}],\"sink\":{\"name\":\"test\",\"relation\":{\"name\":\"s2\"}}}"

//    val executor = new ExtractionJobExecutor("t1", desc)
//    val steps = executor.steps
//
//    print(steps.isEmpty)





//    var df = spark.read.json("/home/johan/Documents/CRM/samples/test/test2.json")
//    df.alias("df")
//    val ds = Seq(Bag(df, "df"))
//    df = df.withColumn("reading", Functions.column(new Value("df", "READING"), Seq(Bag(df, "df")))).drop(col("READING")).drop("PURCHASE").drop("LOGIN").drop("interval")
//    df = df.withColumn("const", Functions.column(new Constant[String]("constant"), Seq(Bag(df, "df"))))
//    df = df.withColumn("frequency", Functions.column(new Value("df", "reading.frequency"), Seq(Bag(df, "df"))))
//    df = df.withColumn("at", Functions.column(new Value("df", "reading.lastTime"), Seq(Bag(df, "df"))))
//    df = df.withColumn("hour", Functions.column(new Value("df", "reading.distribution.hour"), Seq(Bag(df, "df"))))
//    df = df.withColumn("day", Functions.column(new Value("df", "reading.distribution.day"), Seq(Bag(df, "df"))))
//
//    df = df.withColumn("max", Functions.column(new MaxOf(new Value("df", "hour")), Seq(Bag(df, "df"))))
//    df = df.withColumn("min", Functions.column(new MinOf(new Value("df", "day")), Seq(Bag(df, "df"))))
//    df = df.withColumn("dt1", Functions.column(new TimeFormat(
//      new Time(new Value("df", "at")), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(df, "df"))))
//
//    df = df.withColumn("dt2", Functions.column(new TimeFormat(
//      new Time(new Value("df", "at")), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Seoul"), Seq(Bag(df, "df"))))
//
//    df = df.withColumn("now", Functions.column(new Now(), Seq(Bag(df, "df"))))
//    df = df.withColumn("diff", Functions.column(new DiffTime(new Time(new Value("df", "at")), new Now(), TimeUnit.DAYS), Seq(Bag(df, "df"))))
//    df = df.withColumn("contains", Functions.column(new Contains[Int](new Value("df", "day"), 2), Seq(Bag(df, "df"))))
//    df = df.withColumn("elementAt", Functions.column(new ElementAt(new Value("df", "hour"), 0), Seq(Bag(df, "df"))))
//    df = df.withColumn("cardinality", Functions.column(new Cardinality(new Value("df", "hour")), Seq(Bag(df, "df"))))
//
//
//    val l = new util.ArrayList[extraction.Function]()
//    l.add(new Constant[Int](3))
//    l.add(new Constant[String]("2"))
//    l.add(new Constant[Double](1.0))
//    df = df.withColumn("array", Functions.column(new ArrayOf(l), Seq(Bag(df, "df"))))
//
//    val df2 = df
//
//
//    val attributes = new util.ArrayList[extraction.Function]().asInstanceOf[util.List[Alias]]
//    attributes.add(new Alias(new Value("df", "u"), "u"))
//    attributes.add(new Alias(new Value("df", "frequency"), "freq"))
//    attributes.add(new Alias(new Value("df", "day"), "day"))
//    attributes.add(new Alias(new Value("df", "array"), "arr"))
//    attributes.add(new Alias(new Time(new Value("df", "at")), "at"))
//
//    var projection = new Projection(attributes, new Relation("s1", null))
//    df = ProjectionExecutor.execute(Bag(df, "df"), projection, "df").df
//    df.printSchema()
//
//    var changes = new util.ArrayList[Pair[String, String]]()
//    changes.add(new Pair[String, String]("arr", "array"))
//    var renaming = new Renaming(changes.asInstanceOf[util.List[Pair[String, String]]], new Relation("s2", null))
//    df = RenamingExecutor.execute(Bag(df, "df"), renaming, "df").df
//
//
//    df.show(5, false)
//
//    df2.show(5, false)
//
//    val predicates = new util.ArrayList[Predicate]()
//    predicates.add(new Equals(new Value("s1", "u"), new Value("s2", "u")))
//    predicates.add(new Equals(new Value("s2", "const"), new Constant[String]("constant")))
//
//    var condition = new Conjunction(predicates)
//    var join = new Join(condition, new Relation("s1", null), new Relation("s2", null))
//
//    df = ThetaJoinExecutor.execute(Bag(df, "s1"), Bag(df2, "s2"), join, "df").df
//
//
////    df.printSchema()
////    val predicates = new util.ArrayList[Predicate]()
////    predicates.add(new Equals(new Value("ev"), new Constant[String]("READING")))
////    predicates.add(new Equals(new TimeFormat(new Time(new Value("at")), "HH", "Asia/Jakarta"), new Constant[String]("03")))
////
////    df = df.withColumn("filter", Functions.column(new Filter(new Value("h"), new Conjunction(predicates))))
////    df = df.withColumn("time1", Functions.column(new Time(new Constant[String]("2019-01-01T00:00:00+07:00"))))
//////    df = df.withColumn("time2", Functions.column(new Time(new Constant[String]("2019-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss", "Asia/Seoul")))
////
////    df = df.withColumn("format0", Functions.column(new TimeFormat(new Time(new Constant[String]("2019-01-01T00:00:00+07:00")), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta")))
////
////    df = df.withColumn("format1", Functions.column(new TimeFormat(new Now(), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Seoul")))
////    df = df.withColumn("format2", Functions.column(new TimeFormat(new Now(), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta")))
////    df = df.withColumn("format3", Functions.column(new TimeFormat(new Now(), "yyyy-MM-dd'T'HH:mm:ssXXX", "UTC")))
//
//    df = df.withColumn("win", sum("frequency").over(Window.partitionBy("diff").orderBy("u")))
//
//    val orderings = new util.ArrayList[ColumnOrdering]()
//    orderings.add(new ColumnOrdering("at", OrderBy.ASC))
//    orderings.add(new ColumnOrdering("freq", OrderBy.DESC))
//
//    val sorting = new Sorting(orderings.asInstanceOf[util.List[ColumnOrdering]], new Relation("df", null))
//    df = SortingExecutor.execute(Bag(df, "df"), sorting, "s3").df
//
//    val by = new util.ArrayList[Alias]()
//    by.add(new Alias(new Value("df", "frequency"), "f"))
//    by.add(new Alias(new TimeFormat(new Value("df", "at"), "yyyy-MM-dd HH", "Asia/Jakarta"), "t"))
//
//    val agg = new util.ArrayList[Alias]()
//    agg.add(new Alias(new Count(new Value("df", "u")), "n"))
//    agg.add(new Alias(new Sum(new Value("df", "win")), "s"))
//
//    val grouping = new Grouping(by, agg, new Relation("df", null))
//    df = GroupingExecutor.execute(Bag(df, "df"), grouping, "s4").df
//
//
//    df.show(10, false)
  }
}
