package com.kakaopage.crm.extraction.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kakaopage.crm.extraction.Function
import com.kakaopage.crm.extraction.functions._
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._

class FunctionsTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfter {

  var history: DataFrame = _

  before {
    val path = getClass.getResource("/history.json").getPath
    history = sqlContext.read.json(path)
    history.printSchema()
  }

  test("const test") {
    history.withColumn("const", Functions.column(new Constant[Double](1.0), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("value test") {
    history.withColumn("value", Functions.column(new Value(null, "customer"), Seq(Bag(history, "s0")))).show(5, truncate = false)
    history.withColumn("value", Functions.column(new Value("s0", "customer"), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("alias test") {
    history.select(Functions.column(new Alias(new Value(null, "customer"), "user"), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("array_of test") {
    history.withColumn("array_of", Functions.column(new ArrayOf(List[Function](new Constant[String]("a"), new Constant[String]("b"), new Constant[String]("c")).asJava), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("cardinality test") {
    history.withColumn("cardinality", Functions.column(new Cardinality(new Value(null, "events")), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("collect test") {
    history.select(Functions.column(new Collect(new Value(null, "customer"), false), Seq(Bag(history, "s0")))).show(1, truncate = false)
  }

  test("contains test") {
    history.withColumn("contains", Functions.column(new Contains[String](new ArrayOf(List[Function](new Constant[String]("a"), new Constant[String]("b"), new Constant[String]("c")).asJava), "z"), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("count test") {
    history.select(Functions.column(new Count(new Constant[Int](1)), Seq(Bag(history, "s0")))).show(1, truncate = false)
  }

  test("explode test") {
    history.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("time test") {
    history.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(history, "s0")))).withColumn("time_out", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(history, "dataSet'")))).show(5, truncate = false)
  }

  test("diff test") {
    history.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(history, "s0")))).withColumn("diff_out", Functions.column(new DiffTime(new Value(null, "col.at"), new Constant[String]("2019-02-01T00:00:00Z"), "hours"), Seq(Bag(history, "dataSet'")))).show(5, truncate = false)
  }

  test("element_at test") {
    history.withColumn("element_at", Functions.column(new ElementAt(new Value(null, "events"), 10), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("format test") {
    history.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(history, "s0")))).withColumn("time", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(history, "dataSet'")))).withColumn("format", Functions.column(new TimeFormat(new Value(null, "time"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(history, "dataSet''")))).show(5, truncate = false)
  }

  test("end_of_day test") {
    history.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(history, "s0")))).withColumn("time", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(history, "dataSet'")))).withColumn("format", Functions.column(new TimeFormat(new Value(null, "time"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(history, "dataSet''")))).withColumn("end_of_day", Functions.column(new EndOfDay(new Value(null, "time"), "Asia/Jakarta"), Seq(Bag(history, "dataSet'''")))).withColumn("end_of_day_format", Functions.column(new TimeFormat(new Value(null, "end_of_day"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(history, "dataSet''''")))).show(5, truncate = false)
  }

  test("start_of_day test") {
    history.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(history, "s0")))).withColumn("time", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(history, "dataSet'")))).withColumn("format", Functions.column(new TimeFormat(new Value(null, "time"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(history, "dataSet''")))).withColumn("start_of_day", Functions.column(new StartOfDay(new Value(null, "time"), "Asia/Jakarta"), Seq(Bag(history, "dataSet'''")))).withColumn("start_of_day_format", Functions.column(new TimeFormat(new Value(null, "start_of_day"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(history, "dataSet''''")))).show(5, truncate = false)
  }

  test("now test") {
    history.select(Functions.column(new Now(), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("time_add test") {
    history.select(Functions.column(new TimeAdd(new Now(), "hours", 2, "Asia/Jakarta"), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("max test") {
    history.select(Functions.column(new Max(new Value(null, "customer")), Seq(Bag(history, "s0")))).show(1, truncate = false)
  }

  test("min test") {
    history.select(Functions.column(new Min(new Value(null, "customer")), Seq(Bag(history, "s0")))).show(1, truncate = false)
  }

  test("sum test") {
    history.select(Functions.column(new Sum(new Value(null, "customer")), Seq(Bag(history, "s0")))).show(1, truncate = false)
  }

  test("max_of test") {
    history.withColumn("array_of", Functions.column(new ArrayOf(List[Function](new Constant[Double](1), new Constant[Double](2.5), new Constant[Double](5)).asJava), Seq(Bag(history, "s0")))).withColumn("max_of", Functions.column(new MaxOf(new Value(null, "array_of")), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("min_of test") {
    history.withColumn("array_of", Functions.column(new ArrayOf(List[Function](new Constant[Double](1), new Constant[Double](2.5), new Constant[Double](5)).asJava), Seq(Bag(history, "s0")))).withColumn("min_of", Functions.column(new MinOf(new Value(null, "array_of")), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }

  test("paste test") {
    history.withColumn("paste", Functions.column(new Paste(Seq[Function](new Value(null, "customer"), new Constant("0123456789")).asJava, "-"), Seq(Bag(history, "s0")))).show(5, truncate = false)
  }
}
