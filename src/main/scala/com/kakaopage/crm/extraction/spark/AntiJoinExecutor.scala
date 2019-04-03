package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.AntiJoin

object AntiJoinExecutor extends JoinExecutor[AntiJoin] {
  override def joinType: String = "left_anti"
}
