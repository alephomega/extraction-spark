package com.kakaopage.crm.extraction.spark

import org.apache.spark.sql.DataFrame

case class RelationDataset(df: DataFrame, name: String) {

}
