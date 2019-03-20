package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.crm.extraction._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._


class ExtractionJob(val glueContext: GlueContext) extends JobExecutor {

  override def run(id: String, process: Process): ExtractionResult = {
    val dfs = ExtractionJobExecutor(glueContext, id, process).execute()
    run(id, dfs, ConfigFactory.load())
  }

  def run(id: String, dfs: Array[DataFrame], config: Config): ExtractionResult = {
    val partitionSize: Long = config.getLong("sink.partition-size")
    val base = "%s/%s".format(config.getString("sink.path.bucket"), config.getString("sink.path.base"))

    val ps = dfs.zipWithIndex.map {
      case (df: DataFrame, index: Int) => {
        val path = "%s/%s/%d".format(base, id, index)

        val dynamicFrame = DynamicFrame(df, glueContext)
        val count = dynamicFrame.count

        sink(dynamicFrame, calculateNumPartitions(count, partitionSize), path)
        Partition.of(index, path, count)
      }
    }

    ExtractionResult.`with`(Cohort.`with`(ps.toList.asJava))
  }

  def sink(dynamicFrame: DynamicFrame, partitions: Int, path: String) = {
    val ds = glueContext.getSinkWithFormat(
      connectionType = "s3",
      options = JsonOptions(f"""{"path": "s3://$path%s"}"""),
      format = "csv",
      formatOptions = JsonOptions("""{"writeHeader": false}"""))

    ds.writeDynamicFrame(dynamicFrame.repartition(partitions))
  }

  def calculateNumPartitions(count: Long, partitionSize: Long): Int = {
    val k = math.round(count.toDouble / partitionSize.toDouble).toInt
    if (k < 1) 1 else k
  }
}


object ExtractionJob {

  def main(sysArgs: Array[String]) {
    val glueContext: GlueContext = new GlueContext(new SparkContext())
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "description").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val res = ExtractionJob(glueContext).run(get(args, "description"))

    Job.commit()
  }

  def apply(glueContext: GlueContext): ExtractionJob = new ExtractionJob(glueContext)

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
}