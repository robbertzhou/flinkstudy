package com.zy.chapter08

import com.datastax.driver.mapping.annotations.{Column, Table}

@Table(keyspace = "mydb",name = "sensors")
class SensorReadingTable(
                          @Column(name="sensorid") var sensorid:String,
                            @Column(name = "temperature") var temperature:Double
                        ) {


  def this()={
    this("",0.0)
  }

  def getSensorid():String = sensorid
  def getTemperature():Double = temperature

  def setSensorid(sensorid:String) :Unit=this.sensorid = sensorid
  def setTemperature(temperature:Double):Unit = this.temperature = temperature


}
