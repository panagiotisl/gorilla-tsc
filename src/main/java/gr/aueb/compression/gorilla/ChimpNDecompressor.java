package gr.aueb.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput2;

/**
 * Decompresses a compressed stream created by the Compressor. Returns pairs of timestamp and floating point value.
 *
 * @author Michael Burman
 */
public class ChimpNDecompressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private long storedVal = 0;
    private long storedValues[];
    private int current = 0;
    private boolean first = true;
    private boolean endOfStream = false;

    private ByteBufferBitInput2 in;
	private int previousValues;
	private int previousValuesLog2;

	public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

    private final static long NAN_LONG = 0x7ff8000000000000L;

    public ChimpNDecompressor(ByteBufferBitInput2 input, int previousValues) {
        in = input;
        this.previousValues = previousValues;
        this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
        this.storedValues = new long[previousValues];
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Double readValue() {
        next();
        if(endOfStream) {
            return null;
        }
        return Double.longBitsToDouble(storedVal);
    }

    private void next() {
        if (first) {
        	first = false;
            storedVal = in.getLong(64);
            storedValues[current] = storedVal;
            if (storedValues[current] == NAN_LONG) {
            	endOfStream = true;
            	return;
            }

        } else {
        	nextValue();
        }
    }

    private void nextValue() {
        // Read value
    	int flag = (int) in.getLong(2);
    	if (flag == 3) {
    		storedLeadingZeros = leadingRepresentation[(int) in.getLong(3)];
    		int significantBits = 64 - storedLeadingZeros;
            if(significantBits == 0) {
                significantBits = 64;
            }
            long value = in.getLong(64 - storedLeadingZeros);
            value = storedVal ^ value;

            if (value == NAN_LONG) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
            	current = (current + 1) % previousValues;
    			storedValues[current] = storedVal;
            }
    	} else if (flag == 2) {
    		int significantBits = 64 - storedLeadingZeros;
            if(significantBits == 0) {
                significantBits = 64;
            }
            long value = in.getLong(64 - storedLeadingZeros);
            value = storedVal ^ value;

            if (value == NAN_LONG) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
            	current = (current + 1) % previousValues;
    			storedValues[current] = storedVal;
            }
		} else if (flag == 1) {
			int fill = previousValuesLog2 + 9;
        	int temp = (int) in.getLong(fill);
        	int index = temp >>> (fill -= previousValuesLog2) & (1 << previousValuesLog2) - 1;
        	storedLeadingZeros = leadingRepresentation[temp >>> (fill -= 3) & (1 << 3) - 1];
        	int significantBits = temp >>> (fill -= 6) & (1 << 6) - 1;
        	storedVal = storedValues[index];
        	if(significantBits == 0) {
                significantBits = 64;
            }
            storedTrailingZeros = 64 - significantBits - storedLeadingZeros;
            long value = in.getLong(64 - storedLeadingZeros - storedTrailingZeros);
            value <<= storedTrailingZeros;
            value = storedVal ^ value;
            if (value == NAN_LONG) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
    			current = (current + 1) % previousValues;
    			storedValues[current] = storedVal;
            }
		} else {
            // else -> same value as before
            int index = (int) in.getLong(previousValuesLog2);
            storedVal = storedValues[index];
            current = (current + 1) % previousValues;
    		storedValues[current] = storedVal;
		}
    }

}