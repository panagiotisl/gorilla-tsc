package gr.aueb.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.BitOutput;

/**
 * Implements the Chimp time series compression. Value compression
 * is for floating points only.
 *
 * @author Panagiotis Liakos
 */
public class Chimp {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private long storedVal = 0;
    private boolean first = true;
    private int size;

    public final static int THRESHOLD = 6;

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

    // We should have access to the series?
    public Chimp(BitOutput output) {
        out = output;
        size = 0;
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
        storedVal = value;
        out.writeBits(storedVal, 64);
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
    	long xor = storedVal ^ value;
        if(xor == 0) {
            // Write 0
            out.skipBit();
            out.skipBit();
            size += 2;
            storedLeadingZeros = 65;
        } else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);

            if (trailingZeros > THRESHOLD) {
                int significantBits = 64 - leadingRound[leadingZeros] - trailingZeros;
                out.writeBits(64 * (8 + leadingRepresentation[leadingZeros]) + significantBits, 11);
                out.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR
                size += 11 + significantBits;
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
    			out.writeBits(24 + leadingRepresentation[leadingZeros], 5);
    			out.writeBits(xor, significantBits);
    			size += 5 + significantBits;
    		}
    	}
        storedVal = value;
    }

    public int getSize() {
    	return size;
    }
}
