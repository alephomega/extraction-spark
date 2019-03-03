package com.kakaopage.crm.extraction.spark

import java.util

import com.kakaopage.crm.extraction._
import com.kakaopage.crm.extraction.ra._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection._


class ExtractionJobExecutor(val extraction: Extraction) {
  val sets = mutable.Map[String, DataFrame]()
  val steps: util.List[Step] = Serializer.serialize(extraction)

  def setOf(name: String) = {
    sets.get(name) match {
      case Some(df) => df.alias(name)
      case _ => throw new RuntimeException
    }
  }

  def nameOf(rel: Relation) = {
    rel.getName
  }

  def executeStep(step: Step) = {
    step match {

      case assignment: Assignment => {
        val variable = assignment.getVariable

        val ds = assignment.getOperation match {
          case selection: Selection => SelectionExecutor.execute(null, selection)

          case projection: Projection => ProjectionExecutor.execute(setOf(nameOf(projection.getRelation)), projection)

          case renaming: Renaming => RenamingExecutor.execute(setOf(nameOf(renaming.getRelation)), renaming)

          case grouping: Grouping => GroupingExecutor.execute(setOf(nameOf(grouping.getRelation)), grouping)

          case product: Product => ProductExecutor.execute(setOf(nameOf(product.firstRelation)), setOf(nameOf(product.secondRelation)), product)

          case join: LeftOuterJoin => LeftOuterJoinExecutor.execute(setOf(nameOf(join.firstRelation)), setOf(nameOf(join.secondRelation)), join)

          case join: RightOuterJoin => RightOuterJoinExecutor.execute(setOf(nameOf(join.firstRelation)), setOf(nameOf(join.secondRelation)), join)

          case join: FullOuterJoin => FullOuterJoinExecutor.execute(setOf(nameOf(join.firstRelation)), setOf(nameOf(join.secondRelation)), join)

          case sorting: Sorting => SortingExecutor.execute(setOf(nameOf(sorting.getRelation)), sorting)

          case distinction: DuplicateElimination => DuplicateEliminationExecutor.execute(setOf(nameOf(distinction.getRelation)), distinction)
        }

        sets.put(variable, ds)
      }

      case sink: Sink => {
        SinkExecutor.execute(setOf(nameOf(sink.getRelation)), sink)
      }
    }
  }

  def execute = steps.asScala.foreach(executeStep)
}


object ExtractionJobExecutor {
  def main(args: Array[String]) = {

  }
}
