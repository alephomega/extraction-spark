package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.crm.extraction._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._


class ExtractionJob(val glueContext: GlueContext) extends JobExecutor {

  override def run(job: String, execution: String, process: Process): ExtractionResult = {
    val config = ConfigFactory.load()
    val dfs = ExtractionJobExecutor(glueContext, process).execute()

    val basePath = config.getString("sink.basePath")
    val partitions = dfs.zipWithIndex.map {
      case (df: DataFrame, i: Int) => {
        val path = f"$basePath/$job/$execution/$i"

        val dynamicFrame = DynamicFrame(df, glueContext)
        val count = dynamicFrame.count

        sink(dynamicFrame, split(count, config.getLong("sink.partitionSize")), path)
        Partition.of(i, path, count)
      }
    }

    ExtractionResult.`with`(Cohort.`with`(process.getId, partitions.toList.asJava))
  }

  def sink(dynamicFrame: DynamicFrame, partitions: Int, path: String) = {
    val ds = glueContext.getSinkWithFormat(
      connectionType = "s3",
      options = JsonOptions(f"""{"path": "s3://$path%s"}"""),
      format = "csv",
      formatOptions = JsonOptions("""{"writeHeader": false}"""))

    ds.writeDynamicFrame(dynamicFrame.repartition(partitions))
  }

  def split(count: Long, partitionSize: Long): Int = {
    val k = math.round(count.toDouble / partitionSize.toDouble).toInt
    if (k < 1) 1 else k
  }
}


object ExtractionJob {
  def apply(glueContext: GlueContext): ExtractionJob = new ExtractionJob(glueContext)

  def main(sysArgs: Array[String]) {
    val glueContext: GlueContext = new GlueContext(new SparkContext())
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "description").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val rs = ExtractionJob(glueContext).run(get(args, "description"))

    Job.commit()
  }

  private def get(args: Map[String, String], name: String, default: String = null): String = {
    args.get(name) match {
      case Some(v) => v
      case _ =>
        if (default != null)
          default
        else
          throw new RuntimeException("Required argument missing: " + name)
    }
  }
}