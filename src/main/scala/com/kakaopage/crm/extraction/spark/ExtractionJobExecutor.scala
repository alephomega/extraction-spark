package com.kakaopage.crm.extraction.spark

import java.util

import com.kakaopage.crm.extraction._
import com.kakaopage.crm.extraction.ra._

import scala.collection.JavaConverters._
import scala.collection._


class ExtractionJobExecutor(val description: String) {
  val sets = mutable.Map[String, Bag]()
  val steps: util.List[Step] = Serializer.serialize(Extraction.of(description))

  def datasetOf(name: String) = {
    sets.get(name) match {
      case Some(ds) => ds
      case _ => throw new ExtractionException("There is no set with name '%s'".format(name))
    }
  }

  def nameOf(rel: Relation) = {
    rel.getName
  }

  def executeStep(step: Step) = {
    step match {

      case assignment: Assignment => {
        val as = assignment.getVariable

        val ds = assignment.getOperation match {

          case selection: Selection => SelectionExecutor.execute(
            selection, as)

          case projection: Projection => ProjectionExecutor.execute(
            datasetOf(nameOf(projection.getRelation)), projection, as)

          case renaming: Renaming => RenamingExecutor.execute(
            datasetOf(nameOf(renaming.getRelation)), renaming, as)

          case grouping: Grouping => GroupingExecutor.execute(
            datasetOf(nameOf(grouping.getRelation)), grouping, as)

          case product: Product => ProductExecutor.execute(
            datasetOf(nameOf(product.firstRelation)), datasetOf(nameOf(product.secondRelation)), product, as)

          case join: LeftOuterJoin => LeftOuterJoinExecutor.execute(
            datasetOf(nameOf(join.firstRelation)), datasetOf(nameOf(join.secondRelation)), join, as)

          case join: RightOuterJoin => RightOuterJoinExecutor.execute(
            datasetOf(nameOf(join.firstRelation)), datasetOf(nameOf(join.secondRelation)), join, as)

          case join: FullOuterJoin => FullOuterJoinExecutor.execute(
            datasetOf(nameOf(join.firstRelation)), datasetOf(nameOf(join.secondRelation)), join, as)

          case join: Join => ThetaJoinExecutor.execute(
            datasetOf(nameOf(join.firstRelation)), datasetOf(nameOf(join.secondRelation)), join, as)

          case sorting: Sorting => SortingExecutor.execute(
            datasetOf(nameOf(sorting.getRelation)), sorting, as)

          case distinction: DuplicateElimination => DuplicateEliminationExecutor.execute(
            datasetOf(nameOf(distinction.getRelation)), distinction, as)
        }

        sets.put(as, ds)
      }

      case sink: Sink => {
        SinkExecutor.execute(datasetOf(nameOf(sink.getRelation)), sink)
      }
    }
  }

  def execute() = steps.asScala.foreach(executeStep)
}


object ExtractionJobExecutor {
  def main(args: Array[String]) = {

  }
}
