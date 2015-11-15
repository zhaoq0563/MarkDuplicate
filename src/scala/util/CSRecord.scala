package scala.util

/**
 * Created by SpinyQ on 11/14/15.
 */
class CSAlignmentRecord {
  // Single alignment record for building frag/pair Sort

  // Flags needed for building frag/pair Sort
  var referenceIndex : Integer = 0
  var readNegativeStrandFlag : Boolean = false
  var unclippedEnd : Int = 0
  var unclippedStart : Int = 0
  var pairedFlag : Boolean = false
  var mateUnmappedFlag : Boolean = false
  var mateReferenceIndex : Integer = 0
  var readName : String = null
  var attribute : String = null
  var score : Short = 0
  var libraryId : Short = 0
  var index : Long = 0

  def getReferenceIndex = {this.referenceIndex}
  def getReadNegativeStrandFlag = {this.readNegativeStrandFlag}
  def getUnclippedEnd = {this.unclippedEnd}
  def getUnclippedStart = {this.unclippedStart}
  def getReadPairedFlag = {this.pairedFlag}
  def getMateUnmappedFlag = {this.mateUnmappedFlag}
  def getMateReferenceIndex = {this.mateReferenceIndex}
  def getReadName = {this.readName}
  def getAttribute = {this.attribute}
  def getScore = {this.score}
  def getLibraryId = {this.libraryId}
  def getIndex = {this.index}


}
