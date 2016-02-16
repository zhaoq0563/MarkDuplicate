package main.scala.csadam.util

import java.io.{ObjectInputStream, ObjectOutputStream}

/**
 * Created by Qi Zhao on 11/14/15.
 */

class CSAlignmentRecord extends Serializable{
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
  // For Sort list
  var read1ReferenceIndex : Int = _
  var read2ReferenceIndex : Int = -1
  var read1Coordinate : Int = _
  var orientation : Byte = _
  var read1IndexInFile : Long = _
  var readGroup : Short = _
  var tile: Short = -1
  var x: Short = -1
  var y: Short = -1

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
  // For Sort list
  def getRead1ReferenceIndex = {this.read1ReferenceIndex}
  def getRead2ReferenceIndex = {this.read2ReferenceIndex}
  def getRead1Coordinate = {this.read1Coordinate}
  def getOrientation = {this.orientation}
  def getRead1IndexInFile = {this.read1IndexInFile}
  def getReadGroup = {this.readGroup}
  def getTile = {this.tile}
  def getX = {this.x}
  def getY = {this.y}
  def isPaired = {(this.read2ReferenceIndex != -1)}

  def isSecondaryOrSupplementary = {this.secondaryOrSupplementary}

  private def writeObject(out: ObjectOutputStream) {
    out.writeBoolean(readUnmappedFlag)
    out.writeBoolean(secondaryOrSupplementary)
    out.writeObject(referenceIndex)
    out.writeBoolean(readNegativeStrandFlag)
    out.writeInt(unclippedEnd)
    out.writeInt(unclippedStart)
    out.writeBoolean(pairedFlag)
    out.writeBoolean(firstOfPair)
    out.writeBoolean(mateUnmappedFlag)
    out.writeObject(mateReferenceIndex)
    out.writeObject(readName)
    out.writeObject(attribute)
    out.writeShort(score)
    out.writeShort(libraryId)
    out.writeLong(index)
    out.writeInt(read1ReferenceIndex)
    out.writeInt(read2ReferenceIndex)
    out.writeInt(read1Coordinate)
    out.writeByte(orientation)
    out.writeLong(read1IndexInFile)
    out.writeShort(readGroup)
    out.writeShort(tile)
    out.writeShort(x)
    out.writeShort(y)

  }

  private def readObject(in: ObjectInputStream) {
    readUnmappedFlag = in.readBoolean
    secondaryOrSupplementary = in.readBoolean
    referenceIndex = in.readObject().asInstanceOf[Integer]
    readNegativeStrandFlag = in.readBoolean
    unclippedEnd = in.readInt
    unclippedStart = in.readInt
    pairedFlag = in.readBoolean
    firstOfPair = in.readBoolean
    mateUnmappedFlag = in.readBoolean
    mateReferenceIndex = in.readObject().asInstanceOf[Integer]
    readName = in.readObject().asInstanceOf[String]
    attribute = in.readObject().asInstanceOf[String]
    score = in.readShort
    libraryId = in.readShort
    index = in.readLong
    read1ReferenceIndex = in.readInt()
    read2ReferenceIndex = in.readInt()
    read1Coordinate = in.readInt()
    orientation = in.readByte()
    read1IndexInFile = in.readLong()
    readGroup = in.readShort()
    tile = in.readShort()
    x = in.readShort()
    y = in.readShort()

  }

  private def readObjectNoData() {

  }

}
