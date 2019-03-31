package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.kakaopage.crm.extraction._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._


class ExtractionJob(val glueContext: GlueContext, val config: Config) extends JobExecutor {
  override def run(job: String, execution: String, process: Process): Cohort = {
    val dfs = ExtractionJobExecutor(glueContext, process).execute()

    val base = config.getString("sink.basePath")
    val partitions = dfs.zipWithIndex.map {
      case (df: DataFrame, i: Int) => {
        val path = f"$base/$job/$execution/$i"

        val dynamicFrame = DynamicFrame(df, glueContext)
        val count = dynamicFrame.count

        sink(dynamicFrame, split(count, config.getLong("sink.partitionSize")), path)
        Partition.of(i, path, count)
      }
    }

    Cohort.`with`(process.getName, process.isRepeated, partitions.toList.asJava)
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
  def apply(glueContext: GlueContext, config: Config): ExtractionJob = new ExtractionJob(glueContext, config)

  def main(args: Array[String]) {
    val glueContext = new GlueContext(new SparkContext())
    val config = ConfigFactory.load()
    val resolvedOptions = GlueArgParser.getResolvedOptions(args, config.getStringList("job.options").asScala.toArray)

    Job.init(resolvedOptions("JOB_NAME"), glueContext, resolvedOptions.asJava)
    ExtractionJob(glueContext, config).run(get(resolvedOptions, "description"), resolvedOptions.asJava)
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
