package com.kakaopage.crm.extraction.spark

import java.util

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.kakaopage.crm.extraction._
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.spark_partition_id

import scala.collection.JavaConverters._


object ExtractionJob extends JobExecutor {

  override def run(id: String, steps: util.List[Step]): JobResult = {
    val ds = ExtractionJobExecutor(id, steps).execute()
    sink(ds)

    val target = ds.df.withColumn("partition", spark_partition_id).groupBy("partition").count.collect()
    result(target)
  }


  def sink(ds: Bag) = {

  }

  def result(rows: Array[Row]): JobResult = {
    val ps = rows.map(row => {
      val p = row.getAs[Int]("partition")
      val n = row.getAs[Int]("count")

      Partition.of(p, path(p), n)
    }).toSeq

    JobResult.of(Target.of(ps.asJava))
  }

  def path(partition: Int): String = {
    ""
  }

  private def get(args: Map[String, String], name: String, default: String = null): String = {
    args.get(name) match {
      case Some(v) => v
      case _ => {
        if (default != null)
          default
        else
          throw new RuntimeException("Required argument missing: " + name)
      }
    }
  }

  def main(sysArgs: Array[String]) {
    val glueContext: GlueContext = new GlueContext(new SparkContext())
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "description").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    run(get(args, "description"))
    Job.commit()
  }
}