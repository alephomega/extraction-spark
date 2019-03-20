package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction._
import com.kakaopage.crm.extraction.ra._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection._


class ExtractionJobExecutor(val glueContext: GlueContext, val id: String, val process: Process) {
  val sets = mutable.Map[String, Bag]()

  def dataSet(name: String) = {
    sets.get(name) match {
      case Some(ds) => ds
      case _ => throw new ExtractionException("There is no set with name '%s'".format(name))
    }
  }

  def nameOf(rel: Relation) = {
    rel.getName
  }


  def execute(): Array[DataFrame] = {
    process.getAssignments.asScala.foreach(assignment => execute(assignment))
    execute(process.getSink)
  }

  def execute(assignment: Assignment) = {
    assignment match {
      case assignment: Assignment => {
        val as = assignment.getVariable

        val ds = assignment.getOperation match {

          case selection: Selection => SelectionExecutor.execute(
            glueContext, selection, as)

          case projection: Projection => ProjectionExecutor.execute(
            dataSet(nameOf(projection.getRelation)), projection, as)

          case renaming: Renaming => RenamingExecutor.execute(
            dataSet(nameOf(renaming.getRelation)), renaming, as)

          case grouping: Grouping => GroupingExecutor.execute(
            dataSet(nameOf(grouping.getRelation)), grouping, as)

          case product: Product => ProductExecutor.execute(
            dataSet(nameOf(product.firstRelation)), dataSet(nameOf(product.secondRelation)), product, as)

          case join: LeftOuterJoin => LeftOuterJoinExecutor.execute(
            dataSet(nameOf(join.firstRelation)), dataSet(nameOf(join.secondRelation)), join, as)

          case join: RightOuterJoin => RightOuterJoinExecutor.execute(
            dataSet(nameOf(join.firstRelation)), dataSet(nameOf(join.secondRelation)), join, as)

          case join: FullOuterJoin => FullOuterJoinExecutor.execute(
            dataSet(nameOf(join.firstRelation)), dataSet(nameOf(join.secondRelation)), join, as)

          case join: Join => ThetaJoinExecutor.execute(
            dataSet(nameOf(join.firstRelation)), dataSet(nameOf(join.secondRelation)), join, as)

          case sorting: Sorting => SortingExecutor.execute(
            dataSet(nameOf(sorting.getRelation)), sorting, as)

          case distinction: DuplicateElimination => DuplicateEliminationExecutor.execute(
            dataSet(nameOf(distinction.getRelation)), distinction, as)
        }

        sets.put(as, ds)
      }
    }
  }

  def execute(sink: Sink): Array[DataFrame] = {
    SinkExecutor.execute(dataSet(nameOf(sink.getRelation)), sink)
  }
}

object ExtractionJobExecutor {
  def apply(glueContext: GlueContext, id: String, process: Process): ExtractionJobExecutor = new ExtractionJobExecutor(glueContext, id, process)
}
