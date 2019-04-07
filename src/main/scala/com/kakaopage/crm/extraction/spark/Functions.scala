package com.kakaopage.crm.extraction.spark

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, TimeZone}

import com.amazonaws.services.glue.AWSGlueClientBuilder
import com.amazonaws.services.glue.model.GetTableVersionsRequest
import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.Predicate
import com.kakaopage.crm.extraction.functions._
import com.kakaopage.crm.extraction.predicates._
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
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
    }
  }

  def startOfDay: (Timestamp, String) => Timestamp = (time: Timestamp, timezone: String) => {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timezone))
    cal.setTimeInMillis(time.getTime)

    new Timestamp(DateUtils.truncate(cal, Calendar.DATE).getTimeInMillis)
  }

  def endOfDay: (Timestamp, String) => Timestamp = (time: Timestamp, timezone: String) => {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timezone))
    cal.setTimeInMillis(time.getTime)

    new Timestamp(DateUtils.addMilliseconds(DateUtils.ceiling(cal, Calendar.DATE), -1).getTime)
  }

  def add: (Timestamp, String, Int, String) => Timestamp = (time: Timestamp, unit: String, amount: Int, timezone: String) => {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(timezone))
    cal.setTimeInMillis(time.getTime)

    unit match {
      case "days" => cal.add(Calendar.DATE, amount)
      case "hours" => cal.add(Calendar.HOUR_OF_DAY, amount)
      case "minutes" => cal.add(Calendar.MINUTE, amount)
      case "seconds" => cal.add(Calendar.SECOND, amount)
      case "milliseconds" => cal.add(Calendar.MILLISECOND, amount)
    }

    new Timestamp(cal.getTimeInMillis)
  }

  def now: () => Timestamp = () => new Timestamp(System.currentTimeMillis)

  object UDF {

    def parse = udf(Functions.parse)

    def format = udf(Functions.format)

    def now = udf(Functions.now)

    def diffTime = udf(Functions.diffTime)

    def startOfDay = udf(Functions.startOfDay)

    def endOfDay = udf(Functions.endOfDay)

    def add = udf(Functions.add)
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

      case p: Comment =>
        if (p.isEnabled) true
        else eval(p.getPredicate, row)
    }
  }

  def is[T : Manifest](x: Any) = manifest.runtimeClass.isInstance(x)
  def as[T : Manifest](x: Any) : Option[T] = if (manifest.runtimeClass.isInstance(x)) Some(x.asInstanceOf[T]) else None

  def filter(condition: Predicate, database: String, table: String, field: String) = {
    condition match {
      case c: Conjunction => udf((rs: Seq[Row]) =>
        rs.filter(r => !c.getPredicates.asScala.exists(p => !eval(p, r))), schema(database, table, field))

      case c: Disjunction => udf((rs: Seq[Row]) =>
        rs.filter(r => c.getPredicates.asScala.exists(p => eval(p, r))), schema(database, table, field))

      case c: Negation => udf((rs: Seq[Row]) =>
        rs.filter(r => !eval(c.getPredicate, r)), schema(database, table, field))

      case c: Predicate => udf((rs: Seq[Row]) =>
        rs.filter(r => eval(c, r)), schema(database, table, field))
    }
  }

  def time(f: Time, rds: Seq[Bag]): Column = {
    UDF.parse(column(f.getText, rds))
  }

  def format(f: TimeFormat, rds: Seq[Bag]): Column = {
    UDF.format(column(f.getTime, rds), lit(f.getPattern), lit(f.getTimezone))
  }

  def diff(f: DiffTime, rds: Seq[Bag]): Column = {
    UDF.diffTime(column(f.firstTime, rds), column(f.secondTime, rds), lit(f.getUnit))
  }

  def startOfDay(f: StartOfDay, rds: Seq[Bag]): Column = {
    UDF.startOfDay(column(f.getTime, rds), lit(f.getTimezone))
  }

  def endOfDay(f: EndOfDay, rds: Seq[Bag]): Column = {
    UDF.endOfDay(column(f.getTime, rds), lit(f.getTimezone))
  }

  def add(f: TimeAdd, rds: Seq[Bag]): Column = {
    UDF.add(column(f.getTime, rds), lit(f.getUnit), lit(f.getAmount), lit(f.getTimezone))
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

  def filter(f: ArrayFilter, rds: Seq[Bag]): Column = {
    filter(f.getPredicate, f.getDatabase, f.getTable, f.getField)(column(f.getArray, rds))
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

  def alias(f: Alias, rds: Seq[Bag]): Column = {
    column(f.getFunction, rds).as(f.getName)
  }

  def isNull(f: Null, rds: Seq[Bag]): Column = {
    isnull(column(f.getValue, rds))
  }

  def isNotNull(f: NotNull, rds: Seq[Bag]): Column = {
    not(isnull(column(f.getValue, rds)))
  }

  def paste(f: Paste, rds: Seq[Bag]): Column = {
    val cols = f.getAttributes.asScala.map(a => column(a, rds))
    concat_ws(f.getSep, cols: _*)
  }

  def time(f: Time, r: Row): Any = {
    parse(invoke(f.getText, r).asInstanceOf[String])
  }

  def format(f: TimeFormat, r: Row): Any = {
    format(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getPattern, f.getTimezone)
  }

  def diff(f: DiffTime, r: Row): Any = {
    diffTime(
      invoke(f.firstTime, r).asInstanceOf[Timestamp],
      invoke(f.secondTime, r).asInstanceOf[Timestamp], f.getUnit)
  }

  def startOfDay(f: StartOfDay, r: Row): Any = {
    startOfDay(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getTimezone)
  }

  def endOfDay(f: EndOfDay, r: Row): Any = {
    endOfDay(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getTimezone)
  }

  def add(f: TimeAdd, r: Row): Any = {
    add(invoke(f.getTime, r).asInstanceOf[Timestamp], f.getUnit, f.getAmount, f.getTimezone)
  }

  def now(f: Now, r: Row): Any = {
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

  def constant(f: Constant[_], r: Row): Any = {
    f.getValue
  }

  def value(f: Value, r: Row): Any = {
    r.get(r.fieldIndex(f.getAttribute))
  }

  def isNull(f: Null, r: Row): Any = {
    invoke(f.getValue, r) == null
  }

  def isNotNull(f: NotNull, r: Row): Any = {
    invoke(f.getValue, r) != null
  }

  def paste(f: Paste, r: Row): Any = {
    f.getAttributes.asScala.map(a => invoke(a, r)).mkString(f.getSep)
  }

  val column: (extraction.Function, Seq[Bag]) => Column = (function: extraction.Function, ds: Seq[Bag]) => {
    function match {
      case f: Time => time(f, ds)
      case f: TimeFormat => format(f, ds)
      case f: DiffTime => diff(f, ds)
      case f: StartOfDay => startOfDay(f, ds)
      case f: EndOfDay => endOfDay(f, ds)
      case f: TimeAdd => add(f, ds)
      case f: Now => now(f, ds)
      case f: Cardinality => cardinality(f, ds)
      case f: ElementAt=> elementAt(f, ds)
      case f: Contains[_] => contains(f, ds)
      case f: MaxOf => maxOf(f, ds)
      case f: MinOf => minOf(f, ds)
      case f: Explode => explodeCol(f, ds)
      case f: ArrayOf => arrayOf(f, ds)
      case f: ArrayFilter => filter(f, ds)
      case f: Count => cnt(f, ds)
      case f: Sum => colsum(f, ds)
      case f: Max => colmax(f, ds)
      case f: Min => colmin(f, ds)
      case f: Collect => collect(f, ds)
      case f: Constant[_] => constant(f, ds)
      case f: Value => value(f, ds)
      case f: Alias => alias(f, ds)
      case f: Null => isNull(f, ds)
      case f: NotNull => isNotNull(f, ds)
      case f: Paste => paste(f, ds)
    }
  }

  val invoke: (extraction.Function, Row) => Any = (function: extraction.Function, row: Row) => {
    function match {
      case f: Time => time(f, row)
      case f: TimeFormat => format(f, row)
      case f: DiffTime => diff(f, row)
      case f: StartOfDay => startOfDay(f, row)
      case f: EndOfDay => endOfDay(f, row)
      case f: TimeAdd => add(f, row)
      case f: Now => now(f, row)
      case f: Cardinality => cardinality(f, row)
      case f: ElementAt => elementAt(f, row)
      case f: Contains[_] => contains(f, row)
      case f: SumOf => sumOf(f, row)
      case f: MaxOf => maxOf(f, row)
      case f: MinOf => minOf(f, row)
      case f: ArrayOf => arrayOf(f, row)
      case f: Constant[_] => constant(f, row)
      case f: Value => value(f, row)
      case f: Null => isNull(f, row)
      case f: NotNull => isNotNull(f, row)
      case f: Paste => paste(f, row)
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

  private def schema(database: String, table: String, field: String): DataType = {
    val glue = AWSGlueClientBuilder.defaultClient()
    val request = new GetTableVersionsRequest().withDatabaseName(database).withTableName(table)

    val results = glue.getTableVersions(request)
    val versions = results.getTableVersions
    val columns = versions.get(0).getTable.getStorageDescriptor.getColumns

    val column = columns.asScala.find(col => col.getName.equals(field))
    column match {
      case Some(v) => CatalystSqlParser.parseDataType(v.getType)
      case _ => throw new ExtractionException(s"Can't find schema information for the column '$database.$table.$field'")
    }
  }
}