package com.kakaopage.crm.extraction.spark

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}
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

  def formatter(pattern: String, timezone: String): DateTimeFormatter = {
    DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(timezone))
  }

  def parse: (String, String, String) => Timestamp = (text: String, pattern: String, timezone: String) => {
    new Timestamp(Date.from(Instant.from(formatter(pattern, timezone).parse(text))).getTime)
  }

  def format: (Timestamp, String, String) => String = (time: Timestamp, pattern: String, timezone: String) => {
    formatter(pattern, timezone).format(time.toInstant)
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


  private object UDF {

    def parse = udf(Functions.parse)

    def format = udf(Functions.format)

    def now = udf(Functions.now)

    def diffTime = udf(Functions.diffTime)
  }


  private def gt[T](a:T, b:T)(implicit ordering:Ordering[T]) = ordering.gt(a, b)

  private def lt[T](a:T, b:T)(implicit ordering:Ordering[T]) = ordering.lt(a, b)

  private def geq[T](a:T, b:T)(implicit ordering:Ordering[T]) = ordering.gteq(a, b)

  private def leq[T](a:T, b:T)(implicit ordering:Ordering[T]) = ordering.lteq(a, b)


  private def ov(v: Any): Any = {
    v match {
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

  private def max(x: Any, y: Any): Any = {
    (ov(x), ov(y)) match {
      case (a: String, b:String) => max(a, b)
      case (a: Double, b:Double) => max(a, b)
      case (a: Long, b:Long) => max(a, b)
      case _ =>
    }
  }

  private def min(x: Any, y: Any): Any = {
    (ov(x), ov(y)) match {
      case (a: String, b:String) => min(a, b)
      case (a: Double, b:Double) => min(a, b)
      case (a: Long, b:Long) => min(a, b)
      case _ =>
    }
  }

  def eval(condition: Predicate, row: Row): Boolean = {

    condition match {
      case p: Equals => invoke(p.firstOperand(), row).equals(invoke(p.secondOperand(), row))

      case p: GreaterThan =>
        val _1 = invoke(p.firstOperand(), row)
        val _2 = invoke(p.secondOperand(), row)

        (ov(_1), ov(_2)) match {
          case (a: String, b:String) => gt(a, b)
          case (a: Double, b:Double) => gt(a, b)
          case (a: Long, b:Long) => gt(a, b)
          case _ => false
        }

      case p: GreaterThanOrEqualTo =>
        val _1 = invoke(p.firstOperand(), row)
        val _2 = invoke(p.secondOperand(), row)

        (ov(_1), ov(_2)) match {
          case (a: String, b:String) => geq(a, b)
          case (a: Double, b:Double) => geq(a, b)
          case (a: Long, b:Long) => geq(a, b)
          case _ => false
        }

      case p: LessThan =>
        val _1 = invoke(p.firstOperand(), row)
        val _2 = invoke(p.secondOperand(), row)

        (ov(_1), ov(_2)) match {
          case (a: String, b:String) => lt(a, b)
          case (a: Double, b:Double) => lt(a, b)
          case (a: Long, b:Long) => lt(a, b)
          case _ => false
        }

      case p: LessThanOrEqualTo =>
        val _1 = invoke(p.firstOperand(), row)
        val _2 = invoke(p.secondOperand(), row)

        (ov(_1), ov(_2)) match {
          case (a: String, b:String) => leq(a, b)
          case (a: Double, b:Double) => leq(a, b)
          case (a: Long, b:Long) => leq(a, b)
          case _ => false
        }

      case p: In[_] => p.getElements.asScala.exists(element => invoke(p.getValue, row).equals(element))
    }
  }

  def filter(condition: Predicate) = {
    condition match {
      case c: Conjunction => udf((rs: Seq[Row]) => {
        rs.filter(r => !c.getPredicates.asScala.exists(p => !eval(p, r)))
      }, schema)

      case c: Disjunction => udf((rs: Seq[Row]) => {
        rs.filter(r => c.getPredicates.asScala.exists(p => eval(p, r)))
      }, schema)

      case c: Negation => udf((rs: Seq[Row]) => {
        val p = c.getPredicate
        rs.filter(r => !eval(p, r))
      }, schema)

      case c: Predicate => udf((rs: Seq[Row]) => {
        rs.filter(r => eval(c, r))
      }, schema)
    }
  }

  val column: (extraction.Function) => Column = {

    case f: Time => UDF.parse(column(f.getText), lit(f.getPattern), lit(f.getTimezone))

    case f: TimeFormat => UDF.format(column(f.getTime), lit(f.getPattern), lit(f.getTimezone))

    case f: DiffTime => UDF.diffTime(column(f.firsTime), column(f.secondTime), lit(f.getUnit.name().toLowerCase()))

    case f: Now => UDF.now()

    case f: Cardinality => size(column(f.getArray))

    case f: ElementAt => column(f.getArray).getItem(f.getIndex)

    case f: Contains[_] => array_contains(column(f.getArray), f.getValue)

    case f: MaxOf => sort_array(column(f.getArray), asc = false).getItem(0)

    case f: MinOf => sort_array(column(f.getArray), asc = true).getItem(0)

    case f: Explode => explode(column(f.getArray))

    case f: ArrayOf => array(f.getElements.asScala.map(element => lit(column(element))): _*)

    case f: Filter => filter(f.getPredicate)(column(f.getArray))

    case f: Value => col(f.getAttribute)

    case f: Constant[_] => lit(f.getValue)
  }


  val invoke: (extraction.Function, Row) => Any = {
    case (f: Time, r: Row) => parse(invoke(f.getText, r).asInstanceOf[String], f.getPattern, f.getTimezone)

    case (f: TimeFormat, r: Row) => format(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getPattern, f.getTimezone)

    case (f: DiffTime, r: Row) => diffTime(invoke(f.firsTime, r).asInstanceOf[Timestamp], invoke(f.secondTime, r).asInstanceOf[Timestamp], f.getUnit.name().toLowerCase())

    case (f: Now, r: Row) => now()

    case (f: Cardinality, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].size

    case (f: ElementAt, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]](f.getIndex)

    case (f: Contains[_], r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].contains(f.getValue)

    case (f: MaxOf, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].reduceLeft(max)

    case (f: MinOf, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].reduceLeft(min)

    case (f: ArrayOf, r: Row) => f.getElements.asScala.map(element => invoke(element, r))

    case (f: Value, r: Row) => r.get(r.fieldIndex(f.getAttribute))

    case (f: Constant[_], r: Row) => f.getValue
  }


  val schema = {
    ArrayType(StructType(Seq(StructField("at", StringType), StructField("ev", StringType), StructField("meta", StructType(Seq(StructField("amount", DoubleType), StructField("episode", StringType), StructField("item", StringType), StructField("series", StringType)))))))
  }

}
