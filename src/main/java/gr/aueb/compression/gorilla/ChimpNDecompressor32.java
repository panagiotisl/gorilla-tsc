package gr.aueb.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput2;

/**
 * Decompresses a compressed stream created by the Compressor. Returns pairs of timestamp and floating point value.
 *
 * @author Michael Burman
 */
public class ChimpNDecompressor32 {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private int storedVal = 0;
    private int storedValues[];
    private int current = 0;
    private boolean first = true;
    private boolean endOfStream = false;

    private ByteBufferBitInput2 in;
	private int previousValues;
	private int previousValuesLog2;

	public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

	private final static int NAN_INT = 0x7fc00000;

    public ChimpNDecompressor32(ByteBufferBitInput2 input, int previousValues) {
        in = input;
        this.previousValues = previousValues;
        this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
        this.storedValues = new int[previousValues];
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Float readValue() {
        next();
        if(endOfStream) {
            return null;
        }
        return Float.intBitsToFloat(storedVal);
    }

    private void next() {
        if (first) {
        	first = false;
            storedVal = in.getInt(32);
            storedValues[current] = storedVal;
            if (storedValues[current] == NAN_INT) {
            	endOfStream = true;
            	return;
            }

        } else {
        	nextValue();
        }
    }

    private void nextValue() {
        if (in.readBit()) {
            if (in.readBit()) {
                // New leading zeros
            	storedLeadingZeros = leadingRepresentation[in.getInt(3)];
            } else {
            }
            int significantBits = 32 - storedLeadingZeros;
            if(significantBits == 0) {
                significantBits = 32;
            }
            int value = in.getInt(32 - storedLeadingZeros);
            value = storedVal ^ value;

            if (value == NAN_INT) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
            	current = (current + 1) % previousValues;
    			storedValues[current] = storedVal;
            }

        } else if (in.readBit()) {
        	int fill = previousValuesLog2 + 8;
        	int temp = in.getInt(fill);
        	int index = temp >>> (fill -= previousValuesLog2) & (1 << previousValuesLog2) - 1;
        	storedLeadingZeros = leadingRepresentation[temp >>> (fill -= 3) & (1 << 3) - 1];
        	int significantBits = temp >>> (fill -= 5) & (1 << 5) - 1;
        	storedVal = storedValues[index];
        	if(significantBits == 0) {
                significantBits = 32;
            }
            storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
            int value = in.getInt(32 - storedLeadingZeros - storedTrailingZeros);
            value <<= storedTrailingZeros;
            value = storedVal ^ value;
            if (value == NAN_INT) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
    			current = (current + 1) % previousValues;
    			storedValues[current] = storedVal;
            }
        } else {
            // else -> same value as before
            int index = in.getInt(previousValuesLog2);
            storedVal = storedValues[index];
            current = (current + 1) % previousValues;
    		storedValues[current] = storedVal;
        }
    }

}