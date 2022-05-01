package gr.aueb.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.BitOutput;

/**
 * Implements the Chimp128 time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class ChimpN {

    private int storedLeadingZeros = Integer.MAX_VALUE;
//    private long storedVal = 0;
    private long storedValues[];
    private boolean first = true;
    private int size;
    private int previousValuesLog2;
    private int threshold;

    public final static short[] leadingRepresentation = {0, 0, 0, 0, 0, 0, 0, 0,
			1, 1, 1, 1, 2, 2, 2, 2,
			3, 3, 4, 4, 5, 5, 6, 6,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7
		};

    public final static short[] leadingRound = {0, 0, 0, 0, 0, 0, 0, 0,
			8, 8, 8, 8, 12, 12, 12, 12,
			16, 16, 18, 18, 20, 20, 22, 22,
			24, 24, 24, 24, 24, 24, 24, 24,
			24, 24, 24, 24, 24, 24, 24, 24,
			24, 24, 24, 24, 24, 24, 24, 24,
			24, 24, 24, 24, 24, 24, 24, 24,
			24, 24, 24, 24, 24, 24, 24, 24
		};
//    public final static short FIRST_DELTA_BITS = 27;

    private BitOutput out;
	private int previousValues;

	private int setLsb;
	private int[] indices;
	private int index = 0;
	private int current = 0;

    // We should have access to the series?
    public ChimpN(BitOutput output, int previousValues) {
        out = output;
        size = 0;
        this.previousValues = previousValues;
        this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
        this.threshold = 6 + previousValuesLog2;
        this.setLsb = (int) Math.pow(2, threshold + 1) - 1;
        this.indices = new int[(int) Math.pow(2, threshold + 1)];
        this.storedValues = new long[previousValues];
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public void addValue(long value) {
        if(first) {
            writeFirst(value);
        } else {
            compressValue(value);
        }
    }

    /**
     * Adds a new double value to the series. Note, values must be inserted in order.
     *
     * @param value next floating point value in the series
     */
    public void addValue(double value) {
        if(first) {
            writeFirst(Double.doubleToRawLongBits(value));
        } else {
            compressValue(Double.doubleToRawLongBits(value));
        }
    }

    private void writeFirst(long value) {
    	first = false;
        storedValues[current] = value;
        out.writeBits(storedValues[current], 64);
        indices[(int) value & setLsb] = index;
        size += 64;
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public void close() {
    	addValue(Double.NaN);
        out.skipBit();
        out.flush();
    }

    private void compressValue(long value) {
    	int key = (int) value & setLsb;
    	long xor;
    	int previousIndex;
    	int trailingZeros;
    	int currIndex = indices[key];
    	if ((index - currIndex) < previousValues) {
    		long tempXor = value ^ storedValues[currIndex % previousValues];
    		trailingZeros = Long.numberOfTrailingZeros(tempXor);
    		if (trailingZeros > threshold) {
    			previousIndex = currIndex % previousValues;
    			xor = tempXor;
    		} else {
    			previousIndex =  index % previousValues;
    			xor = storedValues[previousIndex] ^ value;
    			trailingZeros = Long.numberOfTrailingZeros(xor);
    		}
    	} else {
    		previousIndex =  index % previousValues;
    		xor = storedValues[previousIndex] ^ value;
    		trailingZeros = Long.numberOfTrailingZeros(xor);
    	}

        if(xor == 0) {
            // Write 0
            out.skipBit();
            out.skipBit();
            out.writeBits(previousIndex, previousValuesLog2);
            size += 2 + previousValuesLog2;
            storedLeadingZeros = 65;
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);

            if (trailingZeros > threshold) {
                int significantBits = 64 - leadingRound[leadingZeros] - trailingZeros;
                out.writeBits(512 * (previousValues + previousIndex) + 64 * leadingRepresentation[leadingZeros] + significantBits, previousValuesLog2 + 11);
                out.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                size += 11 + significantBits + previousValuesLog2;
    			storedLeadingZeros = 65;
    		} else if (leadingRound[leadingZeros] == storedLeadingZeros) {
    			out.writeBit();
    			out.skipBit();
    			int significantBits = 64 - leadingRound[leadingZeros];
    			out.writeBits(xor, significantBits);
    			size += 2 + significantBits;
    		} else {
    			storedLeadingZeros = leadingRound[leadingZeros];
    			int significantBits = 64 - leadingRound[leadingZeros];
    			out.writeBits(16 + 8 + leadingRepresentation[leadingZeros], 5);
    			out.writeBits(xor, significantBits);
    			size += 5 + significantBits;
    		}
    	}
        current = ((current + 1) % previousValues);
        storedValues[current] = value;
		index++;
		indices[key] = index;

    }

    public int getSize() {
    	return size;
    }
}
