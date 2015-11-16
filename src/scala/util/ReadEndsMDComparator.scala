package scala.util

import java.util._
import picard.sam.markduplicates.util.ReadEndsForMarkDuplicates

/**
 * Created by Qi Zhao on 11/16/15.
 */

class ReadEndsMDComparator extends Comparator[ReadEndsForMarkDuplicates] {

  def compare(lhs: ReadEndsForMarkDuplicates, rhs: ReadEndsForMarkDuplicates): Int = {
    var retval : Int = lhs.libraryId - rhs.libraryId
    if(retval == 0) {
      retval = lhs.read1ReferenceIndex - rhs.read1ReferenceIndex
    }

    if(retval == 0) {
      retval = lhs.read1Coordinate - rhs.read1Coordinate
    }

    if(retval == 0) {
      retval = lhs.orientation - rhs.orientation
    }

    if(retval == 0) {
      retval = lhs.read2ReferenceIndex - rhs.read2ReferenceIndex
    }

    if(retval == 0) {
      retval = lhs.read2Coordinate - rhs.read2Coordinate
    }

    if(retval == 0) {
      retval = (lhs.read1IndexInFile - rhs.read1IndexInFile).toInt
    }

    if(retval == 0) {
      retval = (lhs.read2IndexInFile - rhs.read2IndexInFile).toInt
    }

    retval
  }
}
