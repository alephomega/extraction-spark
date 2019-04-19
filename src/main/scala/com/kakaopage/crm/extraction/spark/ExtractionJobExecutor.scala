package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.{AWSGlueClientBuilder, DynamicFrame, GlueContext}
import com.kakaopage.crm.extraction._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._


class ExtractionJobExecutor(val glueContext: GlueContext, val config: Config) extends JobExecutor {
  override def run(job: String, execution: String, process: Process): Cohort = {
    val dfs = ProcessExecutor(glueContext, process).execute()

    val glue = AWSGlueClientBuilder.defaultClient()
    val base = config.getString("sink.base")

    val partitions = dfs.zipWithIndex.map {
      case (dataFrame: DataFrame, i: Int) => {
        val path = f"$base/job=$job/execution=$execution/split=$i/"

        val count = dataFrame.count
        val sink: Sink = process.getSink

        var df = dataFrame
        if (sink.needSampling()) {
          val size = sink.getSampling.getSize

          if (count > 0) {
            val fraction = size.toDouble / dfs.length / count
            if (fraction < 1.0) {
              df = dataFrame.sample(withReplacement = false, fraction)
            }
          }
        }

        save(DynamicFrame(df, glueContext), split(count, config.getLong("sink.partitionSize")), path)
        CatalogService.addPartition(glue, job, execution, i, path, config)

        Partition.of(i, path, count)
      }
    }

    Cohort.`with`(process.getName, process.getInterval, partitions.toList.asJava)
  }

  def save(dynamicFrame: DynamicFrame, partitions: Int, path: String) = {
    glueContext.getSinkWithFormat(
      connectionType = "s3",
      options = JsonOptions(f"""{"path": "s3://$path%s"}"""),
      format = "csv",
      formatOptions = JsonOptions("""{"writeHeader": false, "quoteChar": -1, "partitionKeys": ["job", "execution", "split"]}""")).writeDynamicFrame(dynamicFrame.repartition(partitions))
  }

  def split(count: Long, partitionSize: Long): Int = math.max(1, math.round(count.toDouble / partitionSize.toDouble).toInt)
}


object ExtractionJobExecutor {
  def apply(glueContext: GlueContext, config: Config): ExtractionJobExecutor = new ExtractionJobExecutor(glueContext, config)

  def main(args: Array[String]) {
    val glueContext = new GlueContext(new SparkContext())
    val config = ConfigFactory.load()
    val resolvedOptions = GlueArgParser.getResolvedOptions(args, config.getStringList("job.options").asScala.toArray)

    Job.init(resolvedOptions("JOB_NAME"), glueContext, resolvedOptions.asJava)
    ExtractionJobExecutor(glueContext, config).run(get(resolvedOptions, "description"), resolvedOptions.asJava)
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