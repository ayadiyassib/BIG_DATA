import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

val comptes = Array(
  (1L, ("Atlas", "Maroc")),
  (2L, ("Rif", "Maroc")),
  (3L, ("Souss", "Maroc")),
  (4L, ("Lyon", "France")),
  (5L, ("Brest", "France")),
  (6L, ("Nice", "France")),
  (7L, ("Zug", "Suisse")),
  (8L, ("Bale", "Suisse")),
  (9L, ("Colon", "Panama")),
  (10L, ("Balboa", "Panama")),
  (11L, ("Tanger", "Maroc")),
  (12L, ("Oujda", "Maroc")),
  (13L, ("Fes", "Maroc"))
)

val virements = Array(
  Edge(1L, 3L, 20.0), Edge(3L, 2L, 15.0), Edge(1L, 2L, 10.0),
  Edge(1L, 4L, 40.0), Edge(2L, 4L, 25.0), Edge(3L, 6L, 30.0),
  Edge(4L, 6L, 12.0), Edge(6L, 5L, 18.0), Edge(5L, 4L, 9.0),
  Edge(4L, 7L, 60.0), Edge(5L, 7L, 45.0), Edge(5L, 8L, 35.0), Edge(6L, 8L, 55.0),
  Edge(7L, 8L, 30.0),
  Edge(7L, 9L, 80.0), Edge(8L, 10L, 50.0),
  Edge(9L, 10L, 90.0),
  Edge(10L, 7L, 70.0),
  Edge(11L, 12L, 8.0), Edge(12L, 13L, 6.0), Edge(13L, 11L, 5.0)
)

val bankGraph = Graph(sc.parallelize(comptes), sc.parallelize(virements))

println("\n==========================================")
println("--- VERIFICATION DU GRAPHE ---")
println(s"Nombre de sommets : ${bankGraph.vertices.count()}")
println(s"Nombre d'arretes  : ${bankGraph.edges.count()}")
println("==========================================")

println("\n--- QUESTION 2 : VILLES LES PLUS CONNECTEES ---")
bankGraph.degrees.join(bankGraph.vertices).map {
  case (_, (deg, (nom, _))) => (nom, deg)
}.sortBy(_._2, ascending = false).collect().foreach {
  case (nom, deg) => println(f"- $nom%-10s : $deg connexions")
}

println("\n--- QUESTION 3 : SOLDE NET PAR VILLE (Recu - Envoye en kEUR) ---")
val outFlow = bankGraph.aggregateMessages[Double](t => t.sendToSrc(t.attr), _ + _)
val inFlow  = bankGraph.aggregateMessages[Double](t => t.sendToDst(t.attr), _ + _)
val soldes = bankGraph.vertices.leftOuterJoin(inFlow).leftOuterJoin(outFlow).map {
  case (_, (((nom, pays), inOpt), outOpt)) =>
    val recu = inOpt.getOrElse(0.0)
    val envoye = outOpt.getOrElse(0.0)
    (nom, pays, recu, envoye, recu - envoye)
}
soldes.sortBy(_._5, ascending = false).collect().foreach {
  case (nom, pays, r, e, s) =>
    println(f"- $nom%-10s ($pays%-7s) : Recu = $r%5.1f | Envoye = $e%5.1f | Solde = $s%+6.1f kEUR")
}

println("\n--- QUESTION 4 : CYCLES IDENTIFIES ---")
println("1. Cycle Marocain local : Tanger -> Oujda -> Fes -> Tanger")
println("2. Cycle Francais       : Lyon -> Nice -> Brest -> Lyon")
println("3. Boucles Suisse-Panama (4 comptes) :")
println("   - Zug -> Colon -> Balboa -> Zug")
println("   - Zug -> Bale  -> Balboa -> Zug")

println("\n--- QUESTION 5 : FLUX FINANCIERS ENTRE PAYS (kEUR) ---")
bankGraph.triplets
  .filter(t => t.srcAttr._2 != t.dstAttr._2)
  .map(t => ((t.srcAttr._2, t.dstAttr._2), t.attr))
  .reduceByKey(_ + _)
  .collect().foreach { case ((src, dst), m) =>
    println(f"- $src%-7s -> $dst%-7s : $m%.0f kEUR")
  }

println("\n--- QUESTION 6 : VIREMENTS INTERNES PAR PAYS (kEUR) ---")
bankGraph.triplets
  .filter(t => t.srcAttr._2 == t.dstAttr._2)
  .map(t => (t.srcAttr._2, t.attr))
  .reduceByKey(_ + _)
  .collect().foreach { case (p, m) =>
    println(f"- $p%-7s : $m%.0f kEUR")
  }
println("==========================================\n")
