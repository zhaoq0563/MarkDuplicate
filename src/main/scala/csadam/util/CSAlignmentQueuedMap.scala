package main.scala.csadam.util

import java.util.HashMap
import java.util.LinkedList
import java.util.Map
import java.util.Queue

/**
 *
 * Created by Qi Zhao on 2/7/16.
 */

class CSAlignmentQueuedMap[K, V](size: Int) {

  var MAX_SIZE : Int = 100000
  var mapsize : Int = size
  var values : Map[K, V] = new HashMap[K, V](this.mapsize)
  var keys : Queue[K] = new LinkedList[K]()

  def checkValid(size : Int) : Unit = {
    if(size <= 0){
      throw new IllegalArgumentException("Size can only be a positive Integer")
    }

    if(size > this.MAX_SIZE)
      throw new IllegalArgumentException("Size cannot be more than " + this.MAX_SIZE)
  }

  def addItem(key : K, value : V) = {
    if(key == null || value == null)
      throw new NullPointerException("Cannot insert a null for either key or value")

    // First see if we already have this key in our queue
    //if(this.keys.contains(key)){
      // Key found.
      // Simply replace the value in Map
      //this.values.put(key, value)
    //}else {
      // Key not found
      // Add value to both Queue and Map
    this.enqueue(key, value)
    //}
  }

  def getItem(key : K) : V = {
    if (key == null)
      null

    val result : V = this.values.get(key)
    result
  }

  def remove(key : K) : V = {
    if(key == null)
      throw new NullPointerException("Cannot remove a null key")

    val result : V = getItem(key)
    this.keys.remove(key)
    this.values.remove(key)
    result
  }

  def length() : Int = {
    this.keys.size()
  }

  def clear() {
    this.values.clear()
    this.keys.clear()
  }

  def enqueue(key : K, value : V) = {
    if (this.keys.size() < this.mapsize) {
      // We still have space in the queue
      // Add they entry in both queue and the Map
      if (this.keys.add(key)) {
        this.values.put(key, value)
      }
    } else {
      // Queue is full. Need to remove the Head
      // before we can add a new item.
      val old: K = this.keys.poll()
      if (old != null)
        this.values.remove(old)

      // Now add the new item to both queue and the map
      this.keys.add(key)
      this.values.put(key, value)
    }
  }
}
