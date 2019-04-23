package com.kakaopage.crm.extraction.spark

//import com.holdenkarau.spark.testing.DataFrameSuiteBase
//import com.kakaopage.crm.extraction.Pair
//import com.kakaopage.crm.extraction.functions._
//import com.kakaopage.crm.extraction.predicates._
//import com.kakaopage.crm.extraction.ra._
//import com.kakaopage.crm.extraction.ra.relations.TemporaryRelation
//import org.apache.spark.sql.DataFrame
//import org.scalatest.{BeforeAndAfter, FunSuite}


class RelationalOperatorsTest {//extends FunSuite with DataFrameSuiteBase with BeforeAndAfter {

//  var events: DataFrame = _
//  var history: DataFrame = _
//
//  before {
//    sqlContext.sparkContext.setLogLevel("WARN")
//
//    events = sqlContext.read.json(getClass.getResource("/events.json").getPath)
//    history = sqlContext.read.json(getClass.getResource("/history.json").getPath)
//
//    events.printSchema()
//    history.printSchema()
//  }
//
//  test("selection test") {
//    SelectionExecutor.execute(Bag(events, "s0"), new Selection(null, new TemporaryRelation("s0")), "s1").df.show(5, truncate = false)
//
//    val selection = new Selection(new Between(new Time(new Value(null, "at")), new Time(new Constant[String]("2018-12-31T17:20:00+00:00")), new Time(new Constant[String]("2018-12-31T17:25:00+00:00"))), new TemporaryRelation("s0"))
//    SelectionExecutor.execute(Bag(events, "s0"), selection, "s1").df.show(5, truncate = false)
//  }
//
//  test("projection test") {
//    val projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer"), new Alias(new Value(null, "name"), "event_name"), new Alias(new Value(null, "at"), "at")).asJava, new TemporaryRelation("s0"))
//    ProjectionExecutor.execute(Bag(events, "s0"), projection, "s1").df.show(5, truncate = false)
//  }
//
//  test("renaming test") {
//    val renaming = new Renaming(Seq(new Pair[String, String]("customer", "user")).asJava, new TemporaryRelation("s0"))
//    RenamingExecutor.execute(Bag(events, "s0"), renaming, "s1").df.show(5, truncate = false)
//  }
//
//  test("duplicate_elimination test") {
//    val projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0"))
//
//    val elimination = new DuplicateElimination(new TemporaryRelation("s1"))
//    val bag = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(events, "s0"), projection, "s1"), elimination, "s2")
//
//    bag.df.show(5, truncate = false)
//
//    println(events.count())
//    println(bag.df.count())
//  }
//
//  test("grouping test") {
//    val grouping = new Grouping(Seq(new Alias(new Value(null, "customer"), "user")).asJava, Seq(new Alias(new Count(new Constant[Int](1)), "count")).asJava, new TemporaryRelation("s0"))
//    GroupingExecutor.execute(Bag(events, "s0"), grouping, "s1").df.show(5, truncate = false)
//  }
//
//  test("sorting test") {
//    val sorting = new Sorting(Seq(new ColumnOrdering("customer", OrderBy.ASC), new ColumnOrdering("meta.series", OrderBy.DESC)).asJava, new TemporaryRelation("s0"))
//    SortingExecutor.execute(Bag(events, "s0"), sorting, "s1").df.show(5, truncate = false)
//  }
//
//  test("join test") {
//    val join = new Join(new Equals(new Value("s0_1", "customer"), new Value("s0_2", "customer")), false, new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    ThetaJoinExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), join, "s1").df.show(5, truncate = false)
//  }
//
//  test("left-outer-join test") {
//    val join = new LeftOuterJoin(new Equals(new Value("s0_1", "customer"), new Value("s0_2", "customer")), new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    LeftOuterJoinExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), join, "s1").df.show(5, truncate = false)
//  }
//
//  test("right-outer-join test") {
//    val join = new RightOuterJoin(new Equals(new Value("s0_1", "customer"), new Value("s0_2", "customer")), new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    RightOuterJoinExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), join, "s1").df.show(5, truncate = false)
//  }
//
//  test("full-outer-join test") {
//    val join = new FullOuterJoin(new Equals(new Value("s0_1", "customer"), new Value("s0_2", "customer")), new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    FullOuterJoinExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), join, "s1").df.show(5, truncate = false)
//  }
//
//  test("semi-join test") {
//    val join = new SemiJoin(new Equals(new Value("s0_1", "customer"), new Value("s0_2", "customer")), new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    SemiJoinExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), join, "s1").df.show(5, truncate = false)
//  }
//
//  test("anti-join test") {
//    val join = new AntiJoin(new Equals(new Value("s0_1", "customer"), new Value("s0_2", "customer")), new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    AntiJoinExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), join, "s1").df.show(5, truncate = false)
//  }
//
//  test("union test") {
//    var projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0_1"))
//    val elimination = new DuplicateElimination(new TemporaryRelation("s1_1"))
//    val b1 = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(events, "s0_1"), projection, "s1_1"), elimination, "s2_1")
//
//    projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0_2"))
//    val b2 = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(history, "s0_2"), projection, "s1_2"), elimination, "s2_2")
//
//    val union = new Union(new TemporaryRelation("s2_1"), new TemporaryRelation("s2_2"))
//    UnionExecutor.execute(b1, b2, union, "s3").df.show(5, truncate = false)
//
//  }
//
//  test("intersection test") {
//    var projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0_1"))
//    val elimination = new DuplicateElimination(new TemporaryRelation("s1_1"))
//    val b1 = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(events, "s0_1"), projection, "s1_1"), elimination, "s2_1")
//
//    projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0_2"))
//    val b2 = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(history, "s0_2"), projection, "s1_2"), elimination, "s2_2")
//
//    val intersection = new Intersection(new TemporaryRelation("s2_1"), new TemporaryRelation("s2_2"))
//    IntersectionExecutor.execute(b1, b2, intersection, "s3").df.show(5, truncate = false)
//  }
//
//  test("difference test") {
//    var projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0_1"))
//    val elimination = new DuplicateElimination(new TemporaryRelation("s1_1"))
//    val b1 = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(events, "s0_1"), projection, "s1_1"), elimination, "s2_1")
//
//    projection = new Projection(Seq(new Alias(new Value(null, "customer"), "customer")).asJava, new TemporaryRelation("s0_2"))
//    val b2 = DuplicateEliminationExecutor.execute(ProjectionExecutor.execute(Bag(history, "s0_2"), projection, "s1_2"), elimination, "s2_2")
//
//    val difference = new Difference(new TemporaryRelation("s2_1"), new TemporaryRelation("s2_2"))
//    DifferenceExecutor.execute(b1, b2, difference, "s3").df.show(5, truncate = false)
//  }
//
//  test("product test") {
//    val product = new Product(new TemporaryRelation("s0_1"), new TemporaryRelation("s0_2"))
//    ProductExecutor.execute(Bag(events, "s0_1"), Bag(history, "s0_2"), product, "s1").df.show(5, truncate = false)
//  }
}