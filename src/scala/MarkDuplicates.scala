///////////////////////////////////////////////////////////////
//***                                                     ***//
//*** Mark Duplicate by using ADAM store structure        ***//
//***                                                     ***//
//***	Created by Qi @ Oct. 2015                           ***//
//***                                                     ***//
//***	CS Department, UCLA                                 ***//
//***                                                     ***//
///////////////////////////////////////////////////////////////

package scala

import htsjdk.samtools.SAMFileHeader
import htsjdk.samtools.util.{SortingLongCollection, SortingCollection}
import picard.sam.markduplicates.util.{LibraryIdGenerator, ReadEndsForMarkDuplicates}

//import picard.DuplicationMetrics;
//import picard.markduplicates.util.AbstractMarkDuplicatesCommandLineProgram;
//import picard.markduplicates.util.LibraryIdGenerator;
//import picard.markduplicates.util.ReadEnds;
//import picard.markduplicates.util.ReadEndsForMarkDuplicates;
//import picard.markduplicates.util.ReadEndsForMarkDuplicatesCodec;

//object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {
class MarkDuplicates{

    var pairSort = new SortingCollection[ReadEndsForMarkDuplicates]
    var fragSort = new SortingCollection[ReadEndsForMarkDuplicates]
    var dupliateIndexes = new SortingLongCollection(1000000000)
    var libraryIdGenerator = new LibraryIdGenerator(SAMFileHeader)

    def main(args:Array[String]) = {
      var i = args(0)
      var o = args(1)
      println("Input is " + i)
      println("Output is " + o)
    }
}
