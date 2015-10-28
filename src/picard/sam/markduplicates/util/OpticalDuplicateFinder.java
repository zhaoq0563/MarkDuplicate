/*
 * The MIT License
 *
 * Copyright (c) 2014 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package picard.sam.markduplicates.util;

import htsjdk.samtools.util.Log;
import picard.sam.util.ReadNameParsingUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Contains methods for finding optical duplicates.
 *
 * @author Tim Fennell
 * @author Nils Homer
 */
public class OpticalDuplicateFinder {

    public static final String DEFAULT_READ_NAME_REGEX = "[a-zA-Z0-9]+:[0-9]:([0-9]+):([0-9]+):([0-9]+).*".intern();

    public static final int DEFAULT_OPTICAL_DUPLICATE_DISTANCE = 100;

    public String readNameRegex;

    public int opticalDuplicatePixelDistance;

    private Pattern readNamePattern;

    private boolean warnedAboutRegexNotMatching = false;

    private final Log log;

    private static final Log logs = Log.getInstance(OpticalDuplicateFinder.class);

    public OpticalDuplicateFinder() {
        this(DEFAULT_READ_NAME_REGEX, DEFAULT_OPTICAL_DUPLICATE_DISTANCE);
    }

    public OpticalDuplicateFinder(final int opticalDuplicatePixelDistance) {
        this(DEFAULT_READ_NAME_REGEX, opticalDuplicatePixelDistance);
    }

    public OpticalDuplicateFinder(final String readNameRegex) {
        this(readNameRegex, DEFAULT_OPTICAL_DUPLICATE_DISTANCE);
    }

    public OpticalDuplicateFinder(final String readNameRegex, final int opticalDuplicatePixelDistance) {
        this(readNameRegex, opticalDuplicatePixelDistance, null);
    }

    public OpticalDuplicateFinder(final String readNameRegex, final int opticalDuplicatePixelDistance, final Log log) {
        this.readNameRegex = readNameRegex;
        this.opticalDuplicatePixelDistance = opticalDuplicatePixelDistance;
        this.log = log;
    }

    /**
     * Small interface that provides access to the physical location information about a cluster.
     * All values should be defaulted to -1 if unavailable.  ReadGroup and Tile should only allow
     * non-zero positive integers, x and y coordinates may be negative.
     */
    public static interface PhysicalLocation {
        public short getReadGroup();

        public void setReadGroup(short rg);

        public short getTile();

        public void setTile(short tile);

        public short getX();

        public void setX(short x);

        public short getY();

        public void setY(short y);

        public short getLibraryId();

        public void setLibraryId(short libraryId);
    }

    private final int[] tmpLocationFields = new int[10]; // for optimization of addLocationInformation
    /**
     * Method used to extract tile/x/y from the read name and add it to the PhysicalLocation so that it
     * can be used later to determine optical duplication
     *
     * @param readName the name of the read/cluster
     * @param loc the object to add tile/x/y to
     * @return true if the read name contained the information in parsable form, false otherwise
     */
    public boolean addLocationInformation(final String readName, final PhysicalLocation loc) {
        // Optimized version if using the default read name regex (== used on purpose):
        if (this.readNameRegex == this.DEFAULT_READ_NAME_REGEX) {
            final int fields = ReadNameParsingUtils.getRapidDefaultReadNameRegexSplit(readName, ':', tmpLocationFields);
            if (!(fields == 5 || fields == 7)) {
                if (null != log && !this.warnedAboutRegexNotMatching) {
                    this.log.warn(String.format("Default READ_NAME_REGEX '%s' did not match read name '%s'.  " +
                            "You may need to specify a READ_NAME_REGEX in order to correctly identify optical duplicates.  " +
                            "Note that this message will not be emitted again even if other read names do not match the regex.",
                            this.readNameRegex, readName));
                    this.warnedAboutRegexNotMatching = true;
                }
                return false;
            }
            final int offset = fields == 7 ? 2 : 0;
            loc.setTile((short) tmpLocationFields[offset + 2]);
            loc.setX((short) tmpLocationFields[offset + 3]);
            loc.setY((short) tmpLocationFields[offset + 4]);
            return true;
        } else if (this.readNameRegex == null) {
            return false;
        } else {
            // Standard version that will use the regex
            if (this.readNamePattern == null) this.readNamePattern = Pattern.compile(this.readNameRegex);

            final Matcher m = this.readNamePattern.matcher(readName);
            if (m.matches()) {
                loc.setTile((short) Integer.parseInt(m.group(1)));
                loc.setX((short) Integer.parseInt(m.group(2)));
                loc.setY((short) Integer.parseInt(m.group(3)));
                return true;
            } else {
                if (null != log && !this.warnedAboutRegexNotMatching) {
                    this.log.warn(String.format("READ_NAME_REGEX '%s' did not match read name '%s'.  Your regex may not be correct.  " +
                            "Note that this message will not be emitted again even if other read names do not match the regex.",
                            this.readNameRegex, readName));
                    warnedAboutRegexNotMatching = true;
                }
                return false;
            }
        }
    }

    /**
     * Finds which reads within the list of duplicates are likely to be optical duplicates of
     * one another.
     * <p/>
     * Note: this method will perform a sort() of the list; if it is imperative that the list be
     * unmodified a copy of the list should be passed to this method.
     *
     * @param list a list of reads that are determined to be duplicates of one another
     * @return a boolean[] of the same length as the incoming list marking which reads are optical duplicates
     */
    public boolean[] findOpticalDuplicates(final List<? extends PhysicalLocation> list) {
	//long t1,t2;
	//t1 = System.currentTimeMillis();
	final int length = list.size();
        final boolean[] opticalDuplicateFlags = new boolean[length];
	ArrayList<Integer> flaseflaglist = new ArrayList<Integer>();

	//if (number == 2525586) logs.info("Start to sort the OpticalDuplicates list...");
        Collections.sort(list, new Comparator<PhysicalLocation>() {
            public int compare(final PhysicalLocation lhs, final PhysicalLocation rhs) {
                int retval = lhs.getReadGroup() - rhs.getReadGroup();
                if (retval == 0) retval = lhs.getTile() - rhs.getTile();
                if (retval == 0) retval = lhs.getX() - rhs.getX();
                if (retval == 0) retval = lhs.getY() - rhs.getY();
                return retval;
            }
        });
	//if (number == 2525586) logs.info("Finish sorting list...");
	//if (number == 2525586) logs.info("Create flase flag list...");
	for (int i = 0; i < length; i++) {
	    if (!opticalDuplicateFlags[i]) flaseflaglist.add(i);
	}
	//if (number == 2525586) logs.info("Finish creating flase flag list...");

	//int minflaseflag = flaseflaglist.get(0);

	short leftY=-32768, rightY=32767;

        outer:
        for (int i = 0; i < length; ++i) {
            final PhysicalLocation lhs = list.get(i);
            if (lhs.getTile() < 0) continue;
	    /*if (i > 2000000) {
		t2 = System.currentTimeMillis();
		logs.info("The total time for 10% chunk is: " + (t2-t1) + " ms.");
	    }*/

	    /*if (number == 2525586) {
		logs.info("i = " + i);
		logs.info("Before inner loop: Minflaseflag = " + flaseflaglist.get(0));
	    }*/

	    //Check if can skip this i
	    if (leftY != -32768 || rightY != 32767) {
		if (leftY == -32768) {
		    if ((rightY-lhs.getY()) > this.opticalDuplicatePixelDistance) continue;
		} else if (rightY == 32767) {
		    if ((lhs.getY()-leftY) > this.opticalDuplicatePixelDistance) continue;
		} else {
		    if (((lhs.getY()-leftY) > this.opticalDuplicatePixelDistance) && ((rightY-lhs.getY()) > this.opticalDuplicatePixelDistance)) continue;
		}
	    }

	    //Update the least flase flag
	    if ((i + 1) > flaseflaglist.get(0)) {
		for (int find = 1; find < flaseflaglist.size(); find++) {
		    if (opticalDuplicateFlags[flaseflaglist.get(find)]) continue;
		    else {
			flaseflaglist.subList(0, find).clear();
			//flaseflaglist.removeRange(0, find);
			break;
		    }
		}
		//if (number == 2525586) logs.info("Need to find new minflaseflag: Minflaseflag = " + flaseflaglist.get(0) + "; i = " + i + ".");
		if (flaseflaglist.get(0) < (i + 1)) break outer;
	    }

	    //if (number == 2525586) logs.info("Go into inner loop!");
	    int alltrue = 1;
	    leftY=-32768;
	    rightY=32767;

	    Iterator<Integer> it = flaseflaglist.iterator();
	    while(it.hasNext()) {
		int flaseflag = it.next();
		final PhysicalLocation rhs = list.get(flaseflag);
		//if (number == 2525586) logs.info("Start inner loop at: j = " + flaseflag + "; minflaseflag = " + flaseflaglist.get(0) + "; outer i = " + i + "; flase size: " + flaseflaglist.size() + ".");

		if (opticalDuplicateFlags[flaseflag]) continue; //Could be deleted
                if (lhs.getReadGroup() != rhs.getReadGroup()) continue outer;
                if (lhs.getTile() != rhs.getTile()) continue outer;
                if (rhs.getX() > lhs.getX() + this.opticalDuplicatePixelDistance) continue outer;

		if (Math.abs(lhs.getY() - rhs.getY()) <= this.opticalDuplicatePixelDistance) {
                    opticalDuplicateFlags[flaseflag] = true;
		    //Integer del = new Integer(flaseflag);
                    it.remove();
		    //if (number == 2525586) logs.info("Change to true: j = " + flaseflag + "; minflaseflag = " + flaseflaglist.get(0) + "; outer i = " + i + "; flase size: " + flaseflaglist.size() + ".");
                } else {
		    alltrue = 0;
		    //Update the left and right Y flag
		    if (rhs.getY() > lhs.getY()) {
			if (rhs.getY() < rightY) rightY = rhs.getY();
		    } else {
			if (rhs.getY() > leftY) leftY = rhs.getY();
		    } 
                    //if (number == 2525586) logs.info("Not change to true: j = " + flaseflag + "; minflaseflag = " + flaseflaglist.get(0) + "; outer i = " + i + "; flase size: " + flaseflaglist.size() + ".");
                }
	    }
	    if (alltrue == 1) break outer;
            //if (number == 2525586) logs.info("After inner loop: i = " + i + "; Minflaseflag = " + flaseflaglist.get(0) + "; flase size: " + flaseflaglist.size() + ".");

            /*for (int j = 0; j < flaseflaglist.size(); ++j) {
		if (number == 2525586) logs.info("Start inner loop at: j = " + j + "; minflaseflag = " + flaseflaglist.get(0) + ".");
		final PhysicalLocation rhs = list.get(flaseflaglist.get(j));

                if (opticalDuplicateFlags[flaseflaglist.get(j)]) continue;
                if (lhs.getReadGroup() != rhs.getReadGroup()) continue outer;
                if (lhs.getTile() != rhs.getTile()) continue outer;
                if (rhs.getX() > lhs.getX() + this.opticalDuplicatePixelDistance) continue outer;

                if (Math.abs(lhs.getY() - rhs.getY()) <= this.opticalDuplicatePixelDistance) {
                    opticalDuplicateFlags[flaseflaglist.get(j)] = true;
		    if (flaseflaglist.get(j) == minflaseflag) minflaseflag = 0;
		    if (number == 2525586) logs.info("Change to true: j = " + j + "; minflaseflag = " + minflaseflag + ".");
                } else if ( minflaseflag >= j || minflaseflag == 0) {
		    minflaseflag = j;
		    alltrue = 0;
		    if (number == 2525586) logs.info("Not change to true: j = " + j + "; minflaseflag = " + minflaseflag + ".");
		}
            }
	    if (alltrue == 1) break outer;
	    if (number == 2525586) logs.info("After inner loop: Minflaseflag = " + minflaseflag);*/
        }
        return opticalDuplicateFlags;
    }
}
