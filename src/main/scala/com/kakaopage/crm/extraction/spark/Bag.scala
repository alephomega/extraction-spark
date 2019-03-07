package com.kakaopage.crm.extraction.spark

import org.apache.spark.sql.DataFrame

case class Bag(df: DataFrame, name: String)