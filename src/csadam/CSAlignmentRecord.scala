package csadam

/**
 * Created by Qi Zhao on 11/14/15.
 */

class CSAlignmentRecord {
  // Single alignment record for building frag/pair Sort

  // Flags needed for building frag/pair Sort
  var readUnmappedFlag : Boolean = _
  var secondaryOrSupplementary : Boolean = _
  var referenceIndex : Integer = _
  var readNegativeStrandFlag : Boolean = _
  var unclippedEnd : Int = _
  var unclippedStart : Int = _
  var pairedFlag : Boolean = _
  var firstOfPair : Boolean = _
  var mateUnmappedFlag : Boolean = _
  var mateReferenceIndex : Integer = _
  var readName : String = _
  var attribute : String = _
  var score : Short = _
  var libraryId : Short = _
  var index : Long = _

  def getReadUnmappedFlag = {this.readUnmappedFlag}
  def getReferenceIndex = {this.referenceIndex}
  def getReadNegativeStrandFlag = {this.readNegativeStrandFlag}
  def getUnclippedEnd = {this.unclippedEnd}
  def getUnclippedStart = {this.unclippedStart}
  def getReadPairedFlag = {this.pairedFlag}
  def getFirstOfPairFlag = {this.firstOfPair}
  def getMateUnmappedFlag = {this.mateUnmappedFlag}
  def getMateReferenceIndex = {this.mateReferenceIndex}
  def getReadName = {this.readName}
  def getAttribute = {this.attribute}
  def getScore = {this.score}
  def getLibraryId = {this.libraryId}
  def getIndex = {this.index}

  def isSecondaryOrSupplementary = {this.secondaryOrSupplementary}

}
