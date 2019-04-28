import java.io.Serializable

import fight1.distanceCalc
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.Option.Some

import scala.collection.mutable.ArrayBuffer
import scala.math.{pow, sqrt}



object fight2 {

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

    val conf = new SparkConf().setAppName("fight2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.appName("Sparkfightsim").getOrCreate()

    /** For adjacency vector:
      * self is 0
      * enemies are 1
      * allies are 2
      */
    //team2 les gentils, team1 les méchants
    //Array(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
    /**Solar*/
    var coords_solar = new Coords(random.nextInt(50)+1, random.nextInt(100)+1)
    var hasmassheal = true;
    var hasstronkmiracle = true;
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
      Array(0,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
    )

    // Initialize the array for the creatures
    var id: Long = 0
    var creatures = Array((id, solar))
    // Map is 200x200 so we random coordinates accordingly

    /** Planetar*/
    for (i <- 1 to 2) {
      val coord_planetar = new Coords(random.nextInt(50)+1, random.nextInt(100)+1)
      val melee_dmgdice_planetar = new Dice(3, 6, 15)
      //val ranged_dmgdice_rider = new Dice(1, 6, 0)
      val planetar = new Creature(
        "Planetar " + i,
        32,
        229,
        229,
        Array(27,22,17),
        Array(melee_dmgdice_planetar),
        2,
        null,
        null,
        0,
        10,
        10,
        19,11,19,
        coord_planetar,
        6,
        3,
        false,
        Array(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
      )
      id += 1
      planetar.adjVector(id.toInt) = 0
      creatures :+= (id,planetar)
    }

    /** Movanic Deva*/
    for (i <- 1 to 2) {
      val coord_movanic = new Coords(random.nextInt(50)+1, random.nextInt(100)+1)
      val melee_dmgdice_movanic = new Dice(3, 6, 7)
      //val ranged_dmgdice_rider = new Dice(1, 6, 0)
      val movanic = new Creature(
        "Movanic Deva " + i,
        24,
        126,
        126,
        Array(17,12,7),
        Array(melee_dmgdice_movanic),
        1,
        null,
        null,
        0,
        0,
        10,
        12,11,9,
        coord_movanic,
        8,
        0,
        false,
        Array(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
      )
      id += 1
      movanic.adjVector(id.toInt) = 0
      creatures :+= (id,movanic)
    }

    /** Astral Deva*/
    for (i <- 1 to 5) {
      val coord_astral = new Coords(random.nextInt(50)+1, random.nextInt(100)+1)
      val melee_dmgdice_astral = new Dice(1, 8, 14)
      //val ranged_dmgdice_rider = new Dice(1, 6, 0)
      val astral = new Creature(
        "Astral Deva " + i,
        29,
        172,
        172,
        Array(26,21,16),
        Array(melee_dmgdice_astral),
        1,
        null,
        null,
        0,
        0,
        10,
        16,13,11,
        coord_astral,
        10,
        0,
        false,
        Array(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
      )
      id += 1
      astral.adjVector(id.toInt) = 0
      creatures :+= (id,astral)
    }

    /** Hound Archon*/
    for (i <- 1 to 100) {
      val coord_archon = new Coords(random.nextInt(50)+1, random.nextInt(100)+1)
      val melee_dmgdice_archon = new Dice(2, 6, 3)
      //val ranged_dmgdice_rider = new Dice(1, 6, 0)
      val archon = new Creature(
        "Hound Archon " + i,
        19,
        39,
        39,
        Array(9,4,3),
        Array(melee_dmgdice_archon),
        1,
        null,
        null,
        0,
        0,
        10,
        6,5,5,
        coord_archon,
        8,
        0,
        false,
        Array(2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
      )
      id += 1
      archon.adjVector(id.toInt) = 0
      creatures :+= (id,archon)
    }

    /**Red Dragon*/
    val coord_dragon = new Coords(random.nextInt(50)+51, random.nextInt(100)+1)
    val melee_dmgdice_dragon1 = new Dice(4, 8, 24)
    val melee_dmgdice_dragon2 = new Dice(4, 6, 16)
    val melee_dmgdice_dragon3 = new Dice(2, 8, 8)
    val dragon = new Creature(
      "Red Dragon Wyrm",
      39,
      449,
      449,
      Array(37, 35),
      Array(melee_dmgdice_dragon1, melee_dmgdice_dragon2, melee_dmgdice_dragon3),
      4,
      null,
      null,
      0,
      0,
      0,
      25,14,24,
      coord_dragon,
      8,
      0,
      false,
      Array(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2)

    )
    id += 1
    dragon.adjVector(id.toInt) = 0
    creatures :+= (id,dragon)

    /** young dragon*/
    for (i <- 1 to 4) {
      val coord_drake = new Coords(random.nextInt(50)+51, random.nextInt(100)+1)
      val melee_dmgdice_drake1 = new Dice(2, 8, 13)
      val melee_dmgdice_drake2 = new Dice(2, 6, 9)
      val melee_dmgdice_drake3 = new Dice(1, 8, 4)
      val drake = new Creature(
        "Young Red Dragon " + i,
        26,
        172,
        172,
        Array(22, 20),
        Array(melee_dmgdice_drake1,melee_dmgdice_drake2,melee_dmgdice_drake3),
        1,
        null,
        null,
        0,
        0,
        3,
        14,9,13,
        coord_drake,
        8,
        0,
        false,
        Array(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2)

      )
      id += 1
      drake.adjVector(id.toInt) = 0
      creatures :+= (id,drake)
    }

    /** Angel Slayer*/
    for (i <- 1 to 15) {
      val coord_slayer = new Coords(random.nextInt(50)+51, random.nextInt(100)+1)
      val melee_dmgdice_slayer = new Dice(1, 8, 7)
      val ranged_dmgdice_slayer = new Dice(1, 8, 6)
      val slayer = new Creature(
        "Angel slayer " + i,
        26,
        112,
        112,
        Array(21,21,16,11,11,1),
        Array(melee_dmgdice_slayer),
        1,
        Array(19,14,9),
        Array(ranged_dmgdice_slayer),
        22,
        0,
        0,
        13,17, 12,
        coord_slayer,
        8,
        0,
        false,
        Array(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2)

      )
      id += 1
      slayer.adjVector(id.toInt) = 0
      creatures :+= (id,slayer)
    }

    /** Orc Barbarian*/
    for (i <- 1 to 300) {
      val coord_orc = new Coords(random.nextInt(50)+51, random.nextInt(100)+1)
      val melee_dmgdice_orc = new Dice(1, 12, 10)
      val ranged_dmgdice_orc = new Dice(1, 6, 7)
      val orc = new Creature(
        "orc barbarian " + i,
        15,
        42,
        42,
        Array(11),
        Array(melee_dmgdice_orc),
        1,
        Array(5),
        Array(ranged_dmgdice_orc),
        2,
        0,
        0,
        6,2,3,
        coord_orc,
        6,
        0,
        true,
        Array(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2)

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
            if(currentCrea.name == "Solar" && currentCrea.raisableDead > 0){
              if(creabroad.value(1)._2.dead && creabroad.value(2)._2.dead && hasstronkmiracle){
                hasstronkmiracle=false
                for (k<-currentCrea.adjVector.indices){
                  if(currentCrea.adjVector(k)!=1){
                    temp = temp ++ Array(( creabroad.value(k)._1,new FightResult(9999999,new Coords(0,0)) ))
                  }
                }
                println("The Solar Mass Resurrects and heals every ally with his miracle")
                temp
              }
              else if (creabroad.value(1)._2.dead){
                temp = temp ++ Array(( creabroad.value(1)._1,new FightResult(999999,new Coords(0,0)) ))
                println("The Solar resurrects Planetar 1")
                temp
              }else if(creabroad.value(2)._2.dead){
                temp = temp ++ Array(( creabroad.value(2)._1,new FightResult(999999,new Coords(0,0)) ))
                println("The Solar resurrects Planetar 1")
                temp
              }else if(currentCrea.hp< currentCrea.maxhp/2 && hasmassheal){
                hasmassheal = false
                for (k<-currentCrea.adjVector.indices){
                  if(currentCrea.adjVector(k)!=1 && !creabroad.value(k)._2.dead){
                    temp = temp ++ Array(( creabroad.value(k)._1,new FightResult(250,new Coords(0,0)) ))
                  }
                }
                println("The Solar Mass Heals every living ally")
                temp
              }
            }
            if(currentCrea.name.contains("Planetar")&& currentCrea.raisableDead > 0){
              if (creabroad.value(0)._2.dead){
                temp = temp ++ Array(( creabroad.value(0)._1,new FightResult(8000,new Coords(0,0)) ))
                println(currentCrea.name+" resurrects the Solar")
                temp
              }
            }
            if(distanceCalc(newPosition,nearestCrea.coords) > currentCrea.melee_range && distanceCalc(newPosition,nearestCrea.coords) < currentCrea.ranged_range) {
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
            }else if(distanceCalc(newPosition,nearestCrea.coords) <= currentCrea.melee_range){
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

      messages.collect().sortBy(_._1)

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

      messages2.collect().sortBy(_._1).foreach(msg => println("round"+i+" total for crea "+msg._1+" : ["+msg._2.hpmod+",("+msg._2.newPos.x_coord+","+msg._2.newPos.y_coord+")]"))

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
            crea.dead = false
          }else{
            crea.hp = temphp
            crea.dead = false
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