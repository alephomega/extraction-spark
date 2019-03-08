package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Sink

import scala.collection.JavaConverters._

object SinkExecutor {
  def execute(ds: Bag, sink: Sink) = {
    val p = sink.getPartitioning

    val rs = ds.df.repartition(
      p.getNumPartitions,
      p.getColumns.asScala.map(f => Functions.column(f, Seq(ds))): _*)

    val count = ds.df.count()
  }
}