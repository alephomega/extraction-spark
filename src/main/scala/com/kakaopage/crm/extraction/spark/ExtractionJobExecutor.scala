package com.kakaopage.crm.extraction.spark

import java.util

import com.kakaopage.crm.extraction.{Extraction, Serializer, Step}


class ExtractionJobExecutor(val extraction: Extraction) {
  val steps: util.List[Step] = Serializer.serialize(extraction)

  def execute() = {

  }
}


object ExtractionJobExecutor {

  def main(args: Array[String]) = {

  }
}
