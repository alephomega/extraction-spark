package com.kakaopage.crm.extraction.spark

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}
import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import com.kakaopage.crm.extraction
import com.kakaopage.crm.extraction.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

import scala.collection.JavaConverters._

object Functions {

  def formatter(pattern: String, timezone: String): DateTimeFormatter = {
    DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(timezone))
  }

  def parse = udf((text: String, pattern: String, timezone: String) => {
    new Timestamp(Date.from(Instant.from(formatter(pattern, timezone).parse(text))).getTime)
  })

  def format = udf((time: Timestamp, pattern: String, timezone: String) => {
    formatter(pattern, timezone).format(time.toInstant)
  })

  def now = udf(() => new Timestamp(System.currentTimeMillis))

  def diffTime = udf((_1: Timestamp, _2: Timestamp, unit: String) => {
    val duration = Duration.between(_1.toInstant, _2.toInstant)

    unit match {
      case "days" => duration.toDays
      case "hours" => duration.toHours
      case "minutes" => duration.toMinutes
      case "seconds" => duration.getSeconds
      case "milliseconds" => duration.toMillis
      case "microseconds" => duration.toMillis * 1000
      case "nanoseconds" => duration.toNanos
    }
  })

  def sumOf = udf((a: Seq[Double]) => a.sum)


  val invoke: (extraction.Function) => Column = {

    case f: Time => {
      parse(invoke(f.getText), lit(f.getPattern), lit(f.getTimezone))
    }

    case f: TimeFormat => {
      val pattern: String = f.getPattern
      val tz: String = f.getTimezone

      format(invoke(f.getTime), lit(pattern), lit(tz))
    }

    case f: DiffTime => {
      val _1 = invoke(f.firsTime)
      val _2 = invoke(f.secondTime)
      val unit: TimeUnit = f.getUnit

      diffTime(_1, _2, lit(unit.name().toLowerCase()))
    }

    case f: Now => {
      now()
    }

    case f: Cardinality => {
      size(invoke(f.getArray))
    }

    case f: ElementAt => {
      invoke(f.getArray).getItem(f.getIndex)
    }

    case f: Contains[_] => {
      array_contains(invoke(f.getArray), f.getValue)
    }

    case f: MaxOf => {
      sort_array(invoke(f.getArray), asc = false).getItem(0)
    }

    case f: MinOf => {
      sort_array(invoke(f.getArray), asc = true).getItem(0)
    }

    case f: Explode => {
      explode(invoke(f.getArray))
    }

    case f: ArrayOf => {
      val cols = f.getElements.asScala.map(element => lit(invoke(element))).toArray
      array(cols: _*)
    }

    case f: Value => {
      val attribute = f.getAttribute
      col(attribute)
    }

    case f: Constant[_] => {
      lit(f.getValue)
    }
  }




















  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("function-test").setMaster("local")

    val spark = SparkSession.builder.config(conf).getOrCreate
    val context = spark.sqlContext

    import spark.implicits._


    var df = Seq(
      ("2019-02-01T00:00:00+07:00", Array(1, 2, 3)),
      ("2019-02-02T00:00:00+07:00", Array(4, 0, 6)),
      ("2019-02-03T00:00:00+07:00", Array(7, -8, 9)),
      ("2019-02-04T00:00:00+07:00", Array(0, 0, 0))
    ).toDF("at", "values")

    df = df.withColumn("diff", lit(Functions.invoke(new DiffTime(new Time(new Value("at"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), new Now(), TimeUnit.DAYS))))
    df = df.withColumn("elem_2", lit(Functions.invoke(new ElementAt(new Value("values"), 1))))
    df = df.withColumn("max", Functions.invoke(new MaxOf(new Value("values"))))
    df = df.withColumn("min", Functions.invoke(new MinOf(new Value("values"))))
    df = df.withColumn("contains", Functions.invoke(new Contains[Int](new Value("values"), 0)))
    df = df.withColumn("pos", Functions.invoke(new MaxOf(new Value("values"))).gt(0))

    val elems = new util.ArrayList[extraction.Function]()
    elems.add(new Constant[String]("a"))
    elems.add(new Constant[String]("b"))
    elems.add(new Constant[String]("c"))
    df = df.withColumn("array", Functions.invoke(new ArrayOf(elems)))

//    df = df.filter(Predicates.run(new Equal(new DiffTime(new Time(new Value("at"), "yyyy-MM-dd'T'HH:mm:ssXXX", "Asia/Jakarta"), new Now(), TimeUnit.DAYS), new Constant[Int](28))))

//    df = df.withColumn(
//      "sum",
//      call(new SumOf(new Value("values")), df).asInstanceOf[Column]
//    )

    df.show(5, false)
  }
}
