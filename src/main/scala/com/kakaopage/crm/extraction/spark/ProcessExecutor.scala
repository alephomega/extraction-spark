package com.kakaopage.crm.extraction.spark

import com.amazonaws.services.glue.GlueContext
import com.kakaopage.crm.extraction._
import com.kakaopage.crm.extraction.ra._
import com.kakaopage.crm.extraction.ra.relations.Source
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection._


class ProcessExecutor(val glueContext: GlueContext, val process: Process) {
  val sets = mutable.Map[String, Bag]()

  def dataSet(name: String): Bag = {
    sets.get(name) match {
      case Some(ds) => ds
      case _ => throw new ExtractionException("There is no set with name '%s'".format(name))
    }
  }

  def nameOf(rel: Relation): String = {
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
          case selection: Selection => {
            val bag = selection.getRelation match {
              case source: Source => Bag(Loader.load(glueContext, source), source.getName)
              case _ => dataSet(nameOf(selection.getRelation))
            }

            SelectionExecutor.execute(bag, selection, as)
          }

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

          case union: Union =>
            UnionExecutor.execute(dataSet(nameOf(union.firstRelation)), dataSet(nameOf(union.secondRelation)), union, as)

          case intersection: Intersection =>
            IntersectionExecutor.execute(dataSet(nameOf(intersection.firstRelation)), dataSet(nameOf(intersection.secondRelation)), intersection, as)

          case difference: Difference =>
            DifferenceExecutor.execute(dataSet(nameOf(difference.firstRelation)), dataSet(nameOf(difference.secondRelation)), difference, as)
        }

        sets.put(as, ds)
      }
    }
  }

  def execute(sink: Sink): Array[DataFrame] = {
    SinkExecutor.execute(dataSet(nameOf(sink.getRelation)), sink)
  }
}

object ProcessExecutor {
  def apply(glueContext: GlueContext, process: Process): ProcessExecutor = new ProcessExecutor(glueContext, process)
}
