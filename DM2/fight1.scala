import java.io.Serializable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.Option.Some

import scala.collection.mutable.ArrayBuffer
import scala.math.{pow, sqrt}


class Dice(
            var number: Int,
            var range: Int,
            var bonus: Int
          )extends Serializable

class Creature (
                 var name: String,
                 var ac: Int,
                 var maxhp: Int,
                 var hp: Int,
                 var melee_touch: Array[Int],
                 var melee_dmg: Array[Dice],
                 var melee_range: Int,
                 var ranged_touch: Array[Int],
                 var ranged_dmg: Array[Dice],
                 var ranged_range: Int,
                 var regen: Int,
                 var DR: Int,
                 var save_fortitude: Int,
                 var save_reflex: Int,
                 var save_will: Int,
                 var coords: Coords,
                 var movespeed: Int,
                 var raisableDead: Int,
                 var dead: Boolean,
                 var adjVector: Array[Int]
               )extends Serializable
{
  override def toString: String =
    s"$name"
}

class FightResult (
                    var hpmod: Int,
                    var newPos: Coords
                  )extends Serializable
class Coords(
              var x_coord: Int,
              var y_coord: Int
            ) extends Serializable

object fight1 {

  val random = scala.util.Random
  def diceroll(arg: Dice): Int ={
    var score = 0
    for (i <- 1 to arg.number){
      score += random.nextInt(arg.range)+1
    }
    score+arg.bonus
  }

  def dicetouch(touch_score: Int,ac_value: Int): Int ={
    var roll = random.nextInt(20)+1
    if (roll ==20){
      if (dicetouch(touch_score,ac_value)>0) 2
      else 1
    }else if(roll==1) 0
    else if ( (roll+touch_score) >= ac_value)  1
    else  0
  }

  def rollsave(dc: Int, bonus: Int): Boolean={
    var roll = random.nextInt(20)+1
    if( (roll+bonus) > dc) true
    else false
  }


  def distanceCalc(coords_src: Coords, coords_dist: Coords): Int= {
    var dist = 0D
    var src = new Coords(coords_src.x_coord,coords_src.y_coord)
    var dst = new Coords(coords_dist.x_coord,coords_dist.y_coord)
    while(dst.x_coord != src.x_coord || dst.y_coord !=src.y_coord){
      var dist_x = scala.math.abs(dst.x_coord - src.x_coord)
      var dist_y = scala.math.abs(dst.y_coord - src.y_coord)
      if(dist_x == dist_y){
        dist = dist + 1.5
        //modifier les deux coords
        if(src.x_coord > dst.x_coord) src.x_coord -= 1
        else src.x_coord += 1
        if(src.y_coord > dst.y_coord) src.y_coord -= 1
        else src.y_coord += 1
      }else if(dist_x > dist_y){
        dist = 1+dist
        //modif valeur x
        if(src.x_coord > dst.x_coord) src.x_coord -= 1
        else src.x_coord += 1
      }else{
        dist = 1+dist
        //modif valeur y
        if(src.y_coord > dst.y_coord) src.y_coord -= 1
        else src.y_coord += 1
      }

    }
    dist.toInt
  }

  def move(starting_coords:Coords, movespeed: Int, range:Int, destination: Coords): Coords={

    var distanceleft = movespeed.toDouble
    var mover = new Coords(starting_coords.x_coord,starting_coords.y_coord)
    var dest = new Coords(destination.x_coord,destination.y_coord)

    while(distanceCalc(mover,dest)> range && distanceleft >= 1){

      var dist_x = scala.math.abs(mover.x_coord - dest.x_coord)
      var dist_y = scala.math.abs(mover.y_coord - dest.y_coord)
      if(dist_x == dist_y){
        distanceleft = distanceleft - 1.5
        //modifier les deux coords
        if(mover.x_coord > dest.x_coord) mover.x_coord -= 1
        else mover.x_coord += 1
        if(mover.y_coord > dest.y_coord) mover.y_coord -= 1
        else mover.y_coord += 1
      }else if(dist_x > dist_y){
        distanceleft = distanceleft - 1
        //modif valeur x
        if(mover.x_coord > dest.x_coord) mover.x_coord -= 1
        else mover.x_coord += 1
      }else{
        distanceleft = distanceleft - 1
        //modif valeur y
        if(mover.y_coord > dest.y_coord) mover.y_coord -= 1
        else mover.y_coord += 1
      }
    }
    mover
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("fight1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.appName("Sparkfightsim").getOrCreate()

    /** For adjacency vector:
      * self is 0
      * enemies are 1
      * allies are 2
      */

    /**Solar*/
    var coords_solar = new Coords(random.nextInt(30)+1, random.nextInt(30)+1)
    val melee_dmgdice_solar = new Dice(3, 6, 18)
    val ranged_dmgdice_solar = new Dice(2, 6, 14)
    val solar = new Creature(
      "Solar",
      44,
      363,
      363,
      Array(35, 30, 25, 20),
      Array(melee_dmgdice_solar),
      2,
      Array(31, 26, 21, 16),
      Array(ranged_dmgdice_solar),
      22,
      15,
      15,
      25,14,23,
      coords_solar,
      10,
      5,
      false,
      Array(0,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
    )

    // Initialize the array for the creatures
    var id: Long = 0
    var creatures = Array((id, solar))
    // Map is 200x200 so we random coordinates accordingly

    /** Worg riders*/
    for (i <- 1 to 9) {
      val coord_rider = new Coords(random.nextInt(30)+1, random.nextInt(30)+1)
      val melee_dmgdice_rider = new Dice(1, 8, 2)
      val ranged_dmgdice_rider = new Dice(1, 6, 0)
      val rider = new Creature(
        "Worg Rider " + i,
        18,
        13,
        13,
        Array(6),
        Array(melee_dmgdice_rider),
        1,
        Array(4),
        Array(ranged_dmgdice_rider),
        12,
        0,
        0,
        2,2,1,
        coord_rider,
        4,
        0,
        false,
        Array(1,2,2,2,2,2,2,2,2,2,2,2,2,2,2)
      )
      id += 1
      rider.adjVector(id.toInt) = 0
      creatures :+= (id,rider)
    }

    /**Warlord*/
    val coord_warlord = new Coords(random.nextInt(30)+1, random.nextInt(30)+1)
    val melee_dmgdice_warlord1 = new Dice(1, 8, 10)
    val melee_dmgdice_warlord2 = new Dice(2, 6, 0)
    val ranged_dmgdice_warlord = new Dice(1, 6, 5)
    val warlord = new Creature(
      "Warlord",
      27,
      141,
      141,
      Array(24, 19, 14),
      Array(melee_dmgdice_warlord1, melee_dmgdice_warlord2),
      5,
      Array(19),
      Array(ranged_dmgdice_warlord),
      2,
      0,
      0,
      15,9,8,
      coord_warlord,
      6,
      0,
      false,
      Array(1,2,2,2,2,2,2,2,2,2,2,2,2,2,2)
    )
    id += 1
    warlord.adjVector(id.toInt) = 0
    creatures :+= (id,warlord)

    /** Barbares orcs*/
    for (i <- 1 to 4) {
      val coord_orc = new Coords(random.nextInt(30)+1, random.nextInt(30)+1)
      val melee_dmgdice_orc = new Dice(1, 8, 10)
      val ranged_dmgdice_orc = new Dice(1, 8, 6)
      val orc = new Creature(
        "Barbare Orc " + i,
        17,
        142,
        142,
        Array(19, 14, 9),
        Array(melee_dmgdice_orc),
        1,
        Array(16, 11, 6),
        Array(ranged_dmgdice_orc),
        22,
        0,
        3,
        14,9,9,
        coord_orc,
        8,
        0,
        false,
        Array(1,2,2,2,2,2,2,2,2,2,2,2,2,2,2)
      )
      id += 1
      orc.adjVector(id.toInt) = 0
      creatures :+= (id,orc)
    }

    println(creatures.mkString("\n"))

    /** Creer le graphe*/
    // Create a RDD for the vertices
    var creaRDD = sc.parallelize(creatures).cache()

    /*var test1 = new Coords(10,10)
    val test2 = new Coords(0,0)
    println(distanceCalc(test1,test2))
    test1 = move(test1,15,1,test2)
    print(test1.x_coord+","+test1.y_coord)
    println(distanceCalc(test1,test2))*/

    /**le combat*/
    var i = 1
    while(true){

      var creabroad = sc.broadcast(creaRDD.collect())
      println("in round "+i+" of battle")

      var messages: RDD[(VertexId, FightResult)] = creaRDD.flatMap(currentElement => {
        var currentCrea = currentElement._2

        var msgArray: Array[(Long,FightResult)] = null

        if (currentCrea.dead){
          //println("inside loop for "+currentCrea.name + " who is dead")
          None
        }
        else{
          //println("inside loop for "+currentCrea.name)

          /**seek nearest enemy*/
          var nearestElement = currentElement
          var enemydist:Long = 99999
          var possibleEnnemies: Array[(Long,(Long, Creature))] = Array((enemydist,currentElement))

          for(i<-currentCrea.adjVector.indices){
            possibleEnnemies = possibleEnnemies.sortBy(_._1)
            if (currentCrea.adjVector(i)==1 && !creabroad.value(i)._2.dead){
              val relativeDist:Long = distanceCalc(currentCrea.coords,creabroad.value(i)._2.coords)
              val elem = creabroad.value(i)

              if(possibleEnnemies.length < currentCrea.melee_touch.length){
                possibleEnnemies = possibleEnnemies ++ Array((relativeDist,elem))
              }else{
                if(relativeDist<possibleEnnemies(possibleEnnemies.length-1)._1){
                  possibleEnnemies(possibleEnnemies.length-1) = (relativeDist,elem)
                }
              }
            }
          }

          possibleEnnemies = possibleEnnemies.sortBy(_._1)
          var enemycount = possibleEnnemies.length
          nearestElement = possibleEnnemies(0)._2
          var nearestCrea = nearestElement._2
          var currentTarget = 0

          /**Go for damages on nearest enemy*/
          if(nearestCrea != currentCrea){

            /**Check if movement is needed*/
            val relativeDist = distanceCalc(currentCrea.coords,nearestCrea.coords)
            var newPosition = currentCrea.coords
            if(relativeDist > currentCrea.melee_range ){
              newPosition = move(currentCrea.coords,currentCrea.movespeed,currentCrea.melee_range,nearestCrea.coords)
            }


            /**Message permettant de check si y'a encore des ennemis, et de savoir si on a bougé
              * on y met aussi le score de regen naturelle*/
            var temp= Array((currentElement._1,new FightResult(currentCrea.regen,newPosition)))
            println(currentCrea.name+" regenerates "+currentCrea.regen+"hp and moves to position ("+newPosition.x_coord+","+newPosition.y_coord+")")


            var totaldmg = 0
            /**Permet de choisir entre le type d'attaques que le mob doit faire*/
            if(distanceCalc(newPosition,nearestCrea.coords) > currentCrea.melee_range && distanceCalc(newPosition,nearestCrea.coords) < currentCrea.ranged_range){
              for ( touch <- currentCrea.ranged_touch){
                var touchresult = dicetouch(touch,nearestCrea.ac )
                if (touchresult > 0){
                  var dmg = 0
                  for ( dmgdice <- currentCrea.ranged_dmg) {
                    if (touchresult >1)   dmg += diceroll(dmgdice) + diceroll(dmgdice)
                    else dmg += diceroll(dmgdice)
                  }
                  if (currentCrea.name=="Solar"){/**on rajoute les deadly arrows si c'est le solar qui tire*/
                    if(rollsave(20,nearestCrea.save_fortitude)) dmg +=50
                  }
                  dmg -= nearestCrea.DR
                  totaldmg+=dmg
                  /** On regarde si on a tué l'ennemi et si il reste d'autres ennemis*/
                  if(totaldmg > nearestCrea.hp && nearestCrea != possibleEnnemies(enemycount-1)._2._2 && currentCrea != possibleEnnemies(currentTarget+1)._2._2){
                    temp = temp ++ Array(( nearestElement._1,new FightResult(-totaldmg,new Coords(0,0)) ))
                    println( currentCrea.name+" attacks "+nearestCrea.name+" with his ranged weapon for "+totaldmg+" damages")
                    totaldmg =0
                    currentTarget +=1
                    nearestElement = possibleEnnemies(currentTarget)._2
                    nearestCrea = nearestElement._2
                  }
                }
              }
            }else{
              for ( touch <- currentCrea.melee_touch){
                var touchresult = dicetouch(touch,nearestCrea.ac )
                if (touchresult > 0){
                  var dmg = 0
                  for ( dmgdice <- currentCrea.melee_dmg){
                    if (touchresult >1)  dmg += diceroll(dmgdice) + diceroll(dmgdice)
                    else dmg += diceroll(dmgdice)
                  }
                  dmg -= nearestCrea.DR
                  totaldmg+=dmg
                  /** On regarde si on a tué l'ennemi et si il reste d'autres ennemis*/
                  if(totaldmg > nearestCrea.hp  && nearestCrea != possibleEnnemies(enemycount-1)._2._2){
                    /**On regarde si l'ennemi suivant est bien un enneùi et si il est à portée d'attaque*/
                    if(currentCrea.melee_range>distanceCalc(possibleEnnemies(currentTarget+1)._2._2.coords,currentCrea.coords) && currentCrea != possibleEnnemies(currentTarget+1)._2._2){
                      temp = temp ++ Array(( nearestElement._1,new FightResult(-totaldmg,new Coords(0,0)) ))
                      println( currentCrea.name+" melee attacks "+nearestCrea.name+" for "+totaldmg+" damages")
                      totaldmg =0
                      currentTarget +=1
                      nearestElement = possibleEnnemies(currentTarget)._2
                      nearestCrea = nearestElement._2
                    }
                  }
                }
              }
            }

            /**Message pour avertir l'ennemi qu'il a pris des dégats*/
            if (totaldmg>0) {
              temp = temp ++ Array(( nearestElement._1,new FightResult(-totaldmg,new Coords(0,0)) ))
              println(currentCrea.name+" deals "+totaldmg+" damages to "+nearestCrea.name)
              temp
            }
            else temp

          }else None
        }
      }).cache()

      if (messages.isEmpty()){
        println("return on round "+i+" of combat because no more messages")
        creaRDD.collect().foreach(elem => {
          if (!elem._2.dead){
            println(elem._2.name+" survived with "+elem._2.hp+"/"+elem._2.maxhp+"hp !")
          }
        } )
        return
      }

      //messages.collect().sortBy(_._1).foreach(msg => println("msg to "+msg._1+", ["+msg._2.hpmod+",("+msg._2.newPos.x_coord+","+msg._2.newPos.y_coord+")]"))

      var messages2 = messages.reduceByKey((a,b)=> {
        /** On a que 2 types de messages :
          * messages de dégats (envoyés par les ennemis) qui a une valeur de hpmod et (0,0) en newPos
          * un seul message de move et check d'ennemis (auto-envoyé) qui a une valeur de hpmod nulle et la nouvelle position du mob
          * donc si on additionne chaque element séparément on tombe forcément bien
          */
        var hp1: Int = a.hpmod
        var hp2: Int = b.hpmod
        var newhp: Int = hp1+hp2
        var newx1: Int = a.newPos.x_coord
        var newx2: Int = b.newPos.x_coord
        var newy1: Int = a.newPos.y_coord
        var newy2: Int = b.newPos.y_coord
        var newx: Int = newx1 + newx2
        var newy: Int = newy1 + newy2
        var msg = new FightResult(newhp,new Coords(newx,newy))
        //println("test hp: "+hp1+"+"+hp2+"="+newhp+" from "+a.hpmod+"+" +b.hpmod)

        msg
      })

      messages2.collect().sortBy(_._1).foreach(msg => println("round"+i+" total for crea"+msg._1+" : ["+msg._2.hpmod+",("+msg._2.newPos.x_coord+","+msg._2.newPos.y_coord+")]"))

      /**apply modifs to crea RDD*/
      creaRDD=  creaRDD.leftOuterJoin(messages2).map(currentElement=>{
        var crea = currentElement._2._1
        var fightres = currentElement._2._2.getOrElse(null)

        if (fightres != null) {
          /**gestion modifs hp*/
          val temphp = crea.hp + fightres.hpmod
          if (temphp < 0){
            crea.hp = temphp
            crea.dead = true
          }else if (temphp > crea.maxhp){
            crea.hp = crea.maxhp
          }else{
            crea.hp = temphp
          }

          /**gestion modifs distance*/
          crea.coords = fightres.newPos

        }

        (currentElement._1,crea)
      })
      i+=1
      println("")
      Thread.sleep(500)
    }
  }
}