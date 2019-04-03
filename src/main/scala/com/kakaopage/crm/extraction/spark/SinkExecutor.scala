package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.Sink

object SinkExecutor {
  def execute(ds: Bag, sink: Sink) = {
    val p = sink.getPartitioning
    var k = p.getPartitions

    if (k <= 0) k = 1
    val r: Double = 1.0d / k

    ds.df.randomSplit(Array.fill[Double](k) { r })
  }
}