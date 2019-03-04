package com.kakaopage.crm.extraction.spark

import com.kakaopage.crm.extraction.ra.Join

object ThetaJoinExecutor extends JoinExecutor[Join] {
  override def joinType = "inner"
}