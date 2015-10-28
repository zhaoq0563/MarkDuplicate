///////////////////////////////////////////////////////////////
//***                                                     ***//
//***   Mark Duplicate by using ADAM store structure      ***//
//***                                                     ***//
//***	Created by Qi @ Oct. 2015                         ***//
//***                                                     ***//
//***	CS Department, UCLA                               ***//
//***                                                     ***//
///////////////////////////////////////////////////////////////

package scala;

//import picard.DuplicationMetrics;
//import picard.markduplicates.util.AbstractMarkDuplicatesCommandLineProgram;
//import picard.markduplicates.util.LibraryIdGenerator;
//import picard.markduplicates.util.ReadEnds;
//import picard.markduplicates.util.ReadEndsForMarkDuplicates;
//import picard.markduplicates.util.ReadEndsForMarkDuplicatesCodec;

//object MarkDuplicates extends AbstractMarkDuplicatesCommandLineProgram {
object MarkDuplicates{    
    //var pairSort = new SortingCollection[ReadEndsForMarkDuplicates]
    //var fragSort = new SortingCollection[ReadEndsForMarkDuplicates]
    //var dupliateIndexes = new SortingLongCollection
    //var libraryIdGenerator = new LibraryIdGenerator

    def main(args:Array[String]) = {
        var i = args(0)
	var o = args(1)
	println("Input is " + i)
	println("Output is " + o)
    }
}
