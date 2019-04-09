package com.kakaopage.crm.extraction.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kakaopage.crm.extraction.functions._
import com.kakaopage.crm.extraction.predicates._
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._

class PredicatesTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfter {

  var events: DataFrame = _

  before {
    sqlContext.sparkContext.setLogLevel("WARN")

    val path = getClass.getResource("/events.json").getPath
    events = sqlContext.read.json(path)
    events.printSchema()
  }

  test("equals test") {
    events.filter(Predicates.eval(new Equals(new Value(null, "name"), new Constant[String]("PURCHASE")), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("greater-than test") {
    events.filter(Predicates.eval(new GreaterThan(new Value(null, "meta.amount"), new Constant[Long](500)), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("greater-than-or-equal-to test") {
    events.filter(Predicates.eval(new GreaterThanOrEqualTo(new Value(null, "meta.amount"), new Constant[Long](800)), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("less-than test") {
    events.filter(Predicates.eval(new LessThan(new Value(null, "meta.amount"), new Constant[Long](500)), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("less-than-or-equal-to test") {
    events.filter(Predicates.eval(new LessThanOrEqualTo(new Value(null, "meta.amount"), new Constant[Long](800)), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("between test") {
    events.filter(Predicates.eval(new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00"))), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("isin test") {
    events.filter(Predicates.eval(new IsIn[String](new Value(null, "meta.series"), new Constant(Seq("261", "436").asJava)), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("comment test") {
    events.filter(Predicates.eval(new Comment(new Equals(new Value(null, "id"), new Constant[String]("S3XDfVA2CaRFi3iG3TwjLE4B9E0XjuNq")), true), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("null test") {
    events.filter(Predicates.eval(new Negation(new Null(new Value(null, "meta.episode"))), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("conjunction test") {
    events.filter(Predicates.eval(new Conjunction(Seq(new Equals(new Value(null, "id"), new Constant[String]("tP9XfFgicoTggqLkzykOJfr1yXIyYhzf")), new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00")))).asJava), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }

  test("disjunction test") {
    events.filter(Predicates.eval(new Disjunction(Seq(new Equals(new Value(null, "id"), new Constant[String]("S3XDfVA2CaRFi3iG3TwjLE4B9E0XjuNq")), new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00")))).asJava), Seq(Bag(events, "s0")))).show(5, truncate = false)
  }
}
