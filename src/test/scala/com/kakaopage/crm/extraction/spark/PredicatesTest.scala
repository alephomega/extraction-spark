package com.kakaopage.crm.extraction.spark

import java.util

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kakaopage.crm.extraction.{Function, Predicate}
import com.kakaopage.crm.extraction.functions._
import com.kakaopage.crm.extraction.predicates._
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._

class PredicatesTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfter {

  var df: DataFrame = _

  before {
    sqlContext.sparkContext.setLogLevel("WARN")

    val path = getClass.getResource("/events.json").getPath
    df = sqlContext.read.json(path)
    df.printSchema()
  }

  test("equals test") {
    df.filter(Predicates.eval(new Equals(new Value(null, "name"), new Constant[String]("PURCHASE")), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("greater_than test") {
    df.filter(Predicates.eval(new GreaterThan(new Value(null, "meta.amount"), new Constant[Long](500)), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("greater_than_or_equal_to test") {
    df.filter(Predicates.eval(new GreaterThanOrEqualTo(new Value(null, "meta.amount"), new Constant[Long](800)), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("less_than test") {
    df.filter(Predicates.eval(new LessThan(new Value(null, "meta.amount"), new Constant[Long](500)), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("less_than_or_equal_to test") {
    df.filter(Predicates.eval(new LessThanOrEqualTo(new Value(null, "meta.amount"), new Constant[Long](800)), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("between test") {
    df.filter(Predicates.eval(new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00"))), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("isin test") {
    df.filter(Predicates.eval(new IsIn[String](new Value(null, "meta.series"), new Constant(Seq("261", "436").asJava)), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("ignorable test") {
    df.filter(Predicates.eval(new Ignorable(new Equals(new Value(null, "id"), new Constant[String]("S3XDfVA2CaRFi3iG3TwjLE4B9E0XjuNq")), true), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("conjunction test") {
    df.filter(Predicates.eval(new Conjunction(Seq(new Equals(new Value(null, "id"), new Constant[String]("tP9XfFgicoTggqLkzykOJfr1yXIyYhzf")), new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00")))).asJava), Seq(Bag(df, "dataSet")))).show(5, false)
  }

  test("disjunction test") {
    df.filter(Predicates.eval(new Disjunction(Seq(new Equals(new Value(null, "id"), new Constant[String]("S3XDfVA2CaRFi3iG3TwjLE4B9E0XjuNq")), new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00")))).asJava), Seq(Bag(df, "dataSet")))).show(5, false)
  }
}
