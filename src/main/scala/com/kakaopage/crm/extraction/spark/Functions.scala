package com.kakaopage.crm.extraction.spark

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.util.Date

import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.functions._
import com.kakaopage.crm.extraction.predicates._
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.JavaConverters._

object Functions {

  def parse: (String) => Timestamp = (text: String) => {
    new Timestamp(Date.from(Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(text))).getTime)
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
      case (a: String, b:String) => if (geq(a, b)) a else b
      case (a: Double, b:Double) => math.max(a, b)
      case (a: Long, b:Long) => math.max(a, b)
    }
  }

  private def min(x: Any, y: Any): Any = {
    (ov(x), ov(y)) match {
      case (a: String, b:String) => if (leq(a, b)) a else b
      case (a: Double, b:Double) => math.min(a, b)
      case (a: Long, b:Long) => math.min(a, b)
    }
  }

  private def sum(x: Any, y: Any): Any = {
    (ov(x), ov(y)) match {
      case (a: Double, b:Double) => a + b
      case (a: Long, b:Long) => a + b
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

  private def getAlias(ds: Dataset[_]) = ds.queryExecution.analyzed match {
    case SubqueryAlias(alias, _) => Some(alias)
    case _ => None
  }

  private def find(ds: Seq[DataFrame], alias: String): Option[DataFrame] = {
    ds.find(d => {
      getAlias(d) match {
        case Some(a) => a.equals(alias)
        case _ => false
      }
    })
  }

  val column: (extraction.Function, Seq[RelationDataset]) => Column = {

    case (f: Time, s: Seq[RelationDataset]) => UDF.parse(column(f.getText, s))

    case (f: TimeFormat, s: Seq[RelationDataset]) => UDF.format(column(f.getTime, s), lit(f.getPattern), lit(f.getTimezone))

    case (f: DiffTime, s: Seq[RelationDataset]) => UDF.diffTime(column(f.firsTime, s), column(f.secondTime, s), lit(f.getUnit.name().toLowerCase()))

    case (f: Now, s: Seq[RelationDataset]) => UDF.now()

    case (f: Cardinality, s: Seq[RelationDataset]) => size(column(f.getArray, s))

    case (f: ElementAt, s: Seq[RelationDataset]) => column(f.getArray, s).getItem(f.getIndex)

    case (f: Contains[_], s: Seq[RelationDataset]) => array_contains(column(f.getArray, s), f.getValue)

    case (f: MaxOf, s: Seq[RelationDataset]) => sort_array(column(f.getArray, s), asc = false).getItem(0)

    case (f: MinOf, s: Seq[RelationDataset]) => sort_array(column(f.getArray, s), asc = true).getItem(0)

    case (f: Explode, s: Seq[RelationDataset]) => explode(column(f.getArray, s))

    case (f: ArrayOf, s: Seq[RelationDataset]) => array(f.getElements.asScala.map(element => lit(column(element, s))): _*)

    case (f: Filter, s: Seq[RelationDataset]) => filter(f.getPredicate)(column(f.getArray, s))

    case (f: Constant[_], s: Seq[RelationDataset]) => lit(f.getValue)

    case (f: Value, s: Seq[RelationDataset]) => {
      s.find(ds => ds.name.equals(f.getDataset)) match {
        case Some(ds) => ds.df.col(f.getAttribute)
        case _ => col(f.getAttribute)
      }
    }
  }


  val invoke: (extraction.Function, Row) => Any = {
    case (f: Time, r: Row) => parse(invoke(f.getText, r).asInstanceOf[String])

    case (f: TimeFormat, r: Row) => format(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getPattern, f.getTimezone)

    case (f: DiffTime, r: Row) => diffTime(invoke(f.firsTime, r).asInstanceOf[Timestamp], invoke(f.secondTime, r).asInstanceOf[Timestamp], f.getUnit.name().toLowerCase())

    case (f: Now, r: Row) => now()

    case (f: Cardinality, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].size

    case (f: ElementAt, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]](f.getIndex)

    case (f: Contains[_], r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].contains(f.getValue)

    case (f: SumOf, r: Row) => invoke(f.getArray, r).asInstanceOf[Seq[_]].reduceLeft(sum)

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
