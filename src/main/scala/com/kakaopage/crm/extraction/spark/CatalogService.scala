package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.AWSGlue
import com.amazonaws.services.glue.model.{CreatePartitionRequest, GetTableRequest, PartitionInput, StorageDescriptor}
import com.typesafe.config.Config

import scala.collection.JavaConverters._

object CatalogService {
  def addPartition(glue: AWSGlue, job: String, execution: String, split: Int, path: String, config: Config) = {
    val database = config.getString("catalog.database")
    val table = config.getString("catalog.table")

    val sd = glue.getTable(
      new GetTableRequest()
        .withDatabaseName(database)
        .withName(table)).getTable.getStorageDescriptor

    val partitionInput =
      new PartitionInput()
        .withValues(job, execution, split.toString)
        .withStorageDescriptor(
          new StorageDescriptor()
            .withLocation(f"s3://$path%s")
            .withInputFormat(sd.getInputFormat)
            .withOutputFormat(sd.getOutputFormat)
            .withSerdeInfo(sd.getSerdeInfo)
            .withColumns(sd.getColumns)
            .withParameters(Map("classification" -> "csv", "typeOfData" -> "file").asJava)
            .withCompressed(false)
            .withNumberOfBuckets(-1)
            .withStoredAsSubDirectories(false))

    glue.createPartition(
      new CreatePartitionRequest()
        .withDatabaseName(database)
        .withTableName(table)
        .withPartitionInput(partitionInput))
  }
}
