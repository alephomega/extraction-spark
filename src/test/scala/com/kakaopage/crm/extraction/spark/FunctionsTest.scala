package com.kakaopage.crm.extraction.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kakaopage.crm.extraction.Function
import com.kakaopage.crm.extraction.functions._
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._

class FunctionsTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfter {

  var df: DataFrame = _

  before {
    val path = getClass.getResource("/history.json").getPath
    df = sqlContext.read.json(path)
    df.printSchema()
  }

  test("const test") {
    df.withColumn("const", Functions.column(new Constant[Double](1.0), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("value test") {
    df.withColumn("value", Functions.column(new Value(null, "customer"), Seq(Bag(df, "dataSet")))).show(5, false)
    df.withColumn("value", Functions.column(new Value("dataSet", "customer"), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("alias test") {
    df.select(Functions.column(new Alias(new Value(null, "customer"), "user"), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("array_of test") {
    df.withColumn("array_of", Functions.column(new ArrayOf(List[Function](new Constant[String]("a"), new Constant[String]("b"), new Constant[String]("c")).asJava), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("cardinality test") {
    df.withColumn("cardinality", Functions.column(new Cardinality(new Value(null, "events")), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("collect test") {
    df.select(Functions.column(new Collect(new Value(null, "customer"), false), Seq(Bag(df, "dataSet")))).show(1, false)
  }

  test("contains test") {
    df.withColumn("contains", Functions.column(new Contains[String](new ArrayOf(List[Function](new Constant[String]("a"), new Constant[String]("b"), new Constant[String]("c")).asJava), "z"), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("count test") {
    df.select(Functions.column(new Count(new Constant[Int](1)), Seq(Bag(df, "dataSet")))).show(1, false)
  }

  test("explode test") {
    df.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("time test") {
    df.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(df, "dataSet")))).withColumn("time_out", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(df, "dataSet'")))).show(5, false)
  }

  test("diff test") {
    df.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(df, "dataSet")))).withColumn("diff_out", Functions.column(new DiffTime(new Value(null, "col.at"), new Constant[String]("2019-02-01T00:00:00Z"), "hours"), Seq(Bag(df, "dataSet'")))).show(5, false)
  }

  test("element_at test") {
    df.withColumn("element_at", Functions.column(new ElementAt(new Value(null, "events"), 10), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("format test") {
    df.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(df, "dataSet")))).withColumn("time", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(df, "dataSet'")))).withColumn("format", Functions.column(new TimeFormat(new Value(null, "time"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(df, "dataSet''")))).show(5, false)
  }

  test("end_of_day test") {
    df.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(df, "dataSet")))).withColumn("time", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(df, "dataSet'")))).withColumn("format", Functions.column(new TimeFormat(new Value(null, "time"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(df, "dataSet''")))).withColumn("end_of_day", Functions.column(new EndOfDay(new Value(null, "time"), "Asia/Jakarta"), Seq(Bag(df, "dataSet'''")))).withColumn("end_of_day_format", Functions.column(new TimeFormat(new Value(null, "end_of_day"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(df, "dataSet''''")))).show(5, false)
  }

  test("start_of_day test") {
    df.select(Functions.column(new Explode(new Value(null, "events")), Seq(Bag(df, "dataSet")))).withColumn("time", Functions.column(new Time(new Value(null, "col.at")), Seq(Bag(df, "dataSet'")))).withColumn("format", Functions.column(new TimeFormat(new Value(null, "time"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(df, "dataSet''")))).withColumn("start_of_day", Functions.column(new StartOfDay(new Value(null, "time"), "Asia/Jakarta"), Seq(Bag(df, "dataSet'''")))).withColumn("start_of_day_format", Functions.column(new TimeFormat(new Value(null, "start_of_day"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), Seq(Bag(df, "dataSet''''")))).show(5, false)
  }

  test("now test") {
    df.select(Functions.column(new Now(), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("time_add test") {
    df.select(Functions.column(new TimeAdd(new Now(), "hours", 2, "Asia/Jakarta"), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("max test") {
    df.select(Functions.column(new Max(new Value(null, "customer")), Seq(Bag(df, "dataSet")))).show(1, false)
  }

  test("min test") {
    df.select(Functions.column(new Min(new Value(null, "customer")), Seq(Bag(df, "dataSet")))).show(1, false)
  }

  test("sum test") {
    df.select(Functions.column(new Sum(new Value(null, "customer")), Seq(Bag(df, "dataSet")))).show(1, false)
  }

  test("is_null test") {
    df.filter(Functions.column(new Null(new Value(null, "customer")), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("is_not_null test") {
    df.filter(Functions.column(new NotNull(new Value(null, "customer")), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("max_of test") {
    df.withColumn("array_of", Functions.column(new ArrayOf(List[Function](new Constant[Double](1), new Constant[Double](2.5), new Constant[Double](5)).asJava), Seq(Bag(df, "dataSet")))).withColumn("max_of", Functions.column(new MaxOf(new Value(null, "array_of")), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("min_of test") {
    df.withColumn("array_of", Functions.column(new ArrayOf(List[Function](new Constant[Double](1), new Constant[Double](2.5), new Constant[Double](5)).asJava), Seq(Bag(df, "dataSet")))).withColumn("min_of", Functions.column(new MinOf(new Value(null, "array_of")), Seq(Bag(df, "dataSet")))).show(5, false)
  }
}
