package com.kakaopage.crm.extraction.spark

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.util.Date

import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.functions._
import com.kakaopage.crm.extraction.predicates._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.JavaConverters._

object Functions {

  def parse: (String) => Timestamp = (text: String) => {
    new Timestamp(Date.from(
      Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(text))
    ).getTime)
  }

  def format: (Timestamp, String, String) => String = (time: Timestamp, pattern: String, timezone: String) => {
    val formatter = DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(timezone))
    formatter.format(time.toInstant)
  }

  def diffTime: (Timestamp, Timestamp, String) => Long = (_1: Timestamp, _2: Timestamp, unit: String) => {
    val duration = Duration.between(_1.toInstant, _2.toInstant)

    unit match {
      case "days" => duration.toDays
      case "hours" => duration.toHours
      case "minutes" => duration.toMinutes
      case "seconds" => duration.getSeconds
      case "milliseconds" => duration.toMillis
      case "microseconds" => 1000 * duration.toMillis
      case "nanoseconds" => duration.toNanos
    }
  }

  def now: () => Timestamp = () => new Timestamp(System.currentTimeMillis)

  object UDF {

    def parse = udf(Functions.parse)

    def format = udf(Functions.format)

    def now = udf(Functions.now)

    def diffTime = udf(Functions.diffTime)
  }


  def gt[T](x:T, y:T)(implicit ordering:Ordering[T]) = ordering.gt(x, y)

  def lt[T](x:T, y:T)(implicit ordering:Ordering[T]) = ordering.lt(x, y)

  def geq[T](x:T, y:T)(implicit ordering:Ordering[T]) = ordering.gteq(x, y)

  def leq[T](x:T, y:T)(implicit ordering:Ordering[T]) = ordering.lteq(x, y)


  def eval(condition: Predicate, row: Row): Boolean = {

    condition match {
      case p: Equals =>
        invoke(p.firstOperand(), row).equals(invoke(p.secondOperand(), row))

      case p: GreaterThan =>
        (oval(invoke(p.firstOperand(), row)), oval(invoke(p.secondOperand(), row))) match {
          case (a: String, b:String) => gt(a, b)
          case (a: Double, b:Double) => gt(a, b)
          case (a: Long, b:Long) => gt(a, b)
          case _ => false
        }

      case p: GreaterThanOrEqualTo =>
        (oval(invoke(p.firstOperand(), row)), oval(invoke(p.secondOperand(), row))) match {
          case (a: String, b:String) => geq(a, b)
          case (a: Double, b:Double) => geq(a, b)
          case (a: Long, b:Long) => geq(a, b)
          case _ => false
        }

      case p: LessThan =>
        (oval(invoke(p.firstOperand(), row)), oval(invoke(p.secondOperand(), row))) match {
          case (a: String, b:String) => lt(a, b)
          case (a: Double, b:Double) => lt(a, b)
          case (a: Long, b:Long) => lt(a, b)
          case _ => false
        }

      case p: LessThanOrEqualTo =>
        (oval(invoke(p.firstOperand(), row)), oval(invoke(p.secondOperand(), row))) match {
          case (a: String, b:String) => leq(a, b)
          case (a: Double, b:Double) => leq(a, b)
          case (a: Long, b:Long) => leq(a, b)
          case _ => false
        }

      case p: IsIn[_] =>
        p.getElements.asScala.exists(element => invoke(p.getValue, row).equals(element))
    }
  }

  def is[T : Manifest](x: Any) = manifest.runtimeClass.isInstance(x)
  def as[T : Manifest](x: Any) : Option[T] = if (manifest.runtimeClass.isInstance(x)) Some(x.asInstanceOf[T]) else None

  def filter(condition: Predicate) = {
    condition match {
      case c: Conjunction => udf((rs: Seq[Row]) =>
        rs.filter(r => !c.getPredicates.asScala.exists(p => !eval(p, r))), schema)

      case c: Disjunction => udf((rs: Seq[Row]) =>
        rs.filter(r => c.getPredicates.asScala.exists(p => eval(p, r))), schema)

      case c: Negation => udf((rs: Seq[Row]) =>
        rs.filter(r => !eval(c.getPredicate, r)), schema)

      case c: Predicate => udf((rs: Seq[Row]) =>
        rs.filter(r => eval(c, r)), schema)
    }
  }

  def time(f: Time, rds: Seq[Bag]): Column = {
    UDF.parse(column(f.getText, rds))
  }
  
  def format(f: TimeFormat, rds: Seq[Bag]): Column = {
    UDF.format(column(f.getTime, rds), lit(f.getPattern), lit(f.getTimezone))
  }
  
  def diff(f: DiffTime, rds: Seq[Bag]): Column = {
    UDF.diffTime(column(f.firsTime, rds), column(f.secondTime, rds), lit(f.getUnit.name().toLowerCase()))
  }

  def now(f: Now, rds: Seq[Bag]): Column = {
    UDF.now()
  }
  
  def cardinality(f: Cardinality, rds: Seq[Bag]): Column = {
    size(column(f.getArray, rds))
  }

  def elementAt(f: ElementAt, rds: Seq[Bag]): Column = {
    column(f.getArray, rds).getItem(f.getIndex)
  }

  def contains(f: Contains[_], rds: Seq[Bag]): Column = {
    array_contains(column(f.getArray, rds), f.getValue)
  }

  def maxOf(f: MaxOf, rds: Seq[Bag]): Column = {
    sort_array(column(f.getArray, rds), asc = false).getItem(0)
  }

  def minOf(f: MinOf, rds: Seq[Bag]): Column = {
    sort_array(column(f.getArray, rds), asc = true).getItem(0)
  }

  def explodeCol(f: Explode, rds: Seq[Bag]): Column = {
    explode(column(f.getArray, rds))
  }

  def arrayOf(f: ArrayOf, rds: Seq[Bag]): Column = {
    array(f.getElements.asScala.map(element => lit(column(element, rds))): _*)
  }

  def filter(f: Filter, rds: Seq[Bag]): Column = {
    filter(f.getPredicate)(column(f.getArray, rds))
  }

  def cnt(f: Count, rds: Seq[Bag]): Column = {
    count(column(f.getFunction, rds))
  }

  def colsum(f: Sum, rds: Seq[Bag]): Column = {
    sum(column(f.getFunction, rds))
  }

  def colmax(f: Max, rds: Seq[Bag]): Column = {
    max(column(f.getFunction, rds))
  }

  def colmin(f: Min, rds: Seq[Bag]): Column = {
    min(column(f.getFunction, rds))
  }

  def collect(f: Collect, rds: Seq[Bag]): Column = {
    if (f.isDuplicated)
      collect_list(column(f.getFunction, rds))
    else
      collect_set(column(f.getFunction, rds))
  }

  def constant(f: Constant[_], rds: Seq[Bag]): Column = {
    lit(f.getValue)
  }

  def value(f: Value, rds: Seq[Bag]): Column = {
    rds.find(rd => rd.name.equals(f.getDataSet)) match {
      case Some(ds) => ds.df.col(f.getAttribute)
      case _ => col(f.getAttribute)
    }
  }

  def time(f: Time, r: Row): Any = {
    parse(invoke(f.getText, r).asInstanceOf[String])
  }

  def format(f: TimeFormat, r: Row): Any = {
    format(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getPattern, f.getTimezone)
  }

  def diff(f: DiffTime, r: Row): Any = {
    diffTime(
      invoke(f.firsTime, r).asInstanceOf[Timestamp],
      invoke(f.secondTime, r).asInstanceOf[Timestamp], f.getUnit.name().toLowerCase())
  }

  def  now(f: Now, r: Row): Any = {
    now()
  }

  def cardinality(f: Cardinality, r: Row): Any = {
    invoke(f.getArray, r).asInstanceOf[Seq[_]].size
  }

  def elementAt(f: ElementAt, r: Row): Any = {
    invoke(f.getArray, r).asInstanceOf[Seq[_]](f.getIndex)
  }

  def contains(f: Contains[_], r: Row): Any = {
    invoke(f.getArray, r).asInstanceOf[Seq[_]].contains(f.getValue)
  }

  def sumOf(f: SumOf, r: Row): Any = {
    invoke(f.getArray, r).asInstanceOf[Seq[_]].reduceLeft(bsum)
  }

  def maxOf(f: MaxOf, r: Row): Any = {
    invoke(f.getArray, r).asInstanceOf[Seq[_]].reduceLeft(bmax)
  }

  def minOf(f: MinOf, r: Row): Any = {
    invoke(f.getArray, r).asInstanceOf[Seq[_]].reduceLeft(bmin)
  }

  def arrayOf(f: ArrayOf, r: Row): Any = {
    f.getElements.asScala.map(element => invoke(element, r))
  }

  def value(f: Value, r: Row): Any = {
    r.get(r.fieldIndex(f.getAttribute))
  }

  def constant(f: Constant[_], r: Row): Any = {
    f.getValue
  }


  val column: (extraction.Function, Seq[Bag]) => Column = (function: extraction.Function, ds: Seq[Bag]) => {
    function match {
      case f: Time => time(f, ds)
      case f: TimeFormat => format(f, ds)
      case f: DiffTime => diff(f, ds)
      case f: Now => now(f, ds)
      case f: Cardinality => cardinality(f, ds)
      case f: ElementAt=> elementAt(f, ds)
      case f: Contains[_] => contains(f, ds)
      case f: MaxOf => maxOf(f, ds)
      case f: MinOf => minOf(f, ds)
      case f: Explode => explodeCol(f, ds)
      case f: ArrayOf => arrayOf(f, ds)
      case f: Filter => filter(f, ds)
      case f: Count => cnt(f, ds)
      case f: Sum => colsum(f, ds)
      case f: Max => colmax(f, ds)
      case f: Min => colmin(f, ds)
      case f: Collect => collect(f, ds)
      case f: Constant[_] => constant(f, ds)
      case f: Value => value(f, ds)
    }
  }

  val invoke: (extraction.Function, Row) => Any = (function: extraction.Function, row: Row) => {
    function match {
      case f: Time => time(f, row)
      case f: TimeFormat => format(f, row)
      case f: DiffTime => diff(f, row)
      case f: Now => now(f, row)
      case f: Cardinality => cardinality(f, row)
      case f: ElementAt => elementAt(f, row)
      case f: Contains[_] => contains(f, row)
      case f: SumOf => sumOf(f, row)
      case f: MaxOf => maxOf(f, row)
      case f: MinOf => minOf(f, row)
      case f: ArrayOf => arrayOf(f, row)
      case f: Value => value(f, row)
      case f: Constant[_] => constant(f, row)
    }
  }

  private def oval(x: Any): Any = {
    x match {
      case a: String => a
      case a: Double => a
      case a: Float => a.toDouble
      case a: Long => a
      case a: Int => a.toLong
      case a: Short => a.toLong
      case a: Byte => a.toLong
      case _ => toString
    }
  }

  private def bmax(x: Any, y: Any): Any = {
    (oval(x), oval(y)) match {
      case (a: String, b:String) => if (geq(a, b)) a else b
      case (a: Double, b:Double) => math.max(a, b)
      case (a: Long, b:Long) => math.max(a, b)
    }
  }

  private def bmin(x: Any, y: Any): Any = {
    (oval(x), oval(y)) match {
      case (a: String, b:String) => if (leq(a, b)) a else b
      case (a: Double, b:Double) => math.min(a, b)
      case (a: Long, b:Long) => math.min(a, b)
    }
  }

  private def bsum(x: Any, y: Any): Any = {
    (oval(x), oval(y)) match {
      case (a: Double, b:Double) => a + b
      case (a: Long, b:Long) => a + b
    }
  }

  val schema = {
    ArrayType(StructType(Seq(StructField("at", StringType), StructField("ev", StringType), StructField("meta", StructType(Seq(StructField("amount", DoubleType), StructField("episode", StringType), StructField("item", StringType), StructField("series", StringType)))))))
  }
}
