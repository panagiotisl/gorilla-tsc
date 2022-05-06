package gr.aueb.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.Value;

/**
 * Decompresses a compressed stream created by the Compressor. Returns pairs of timestamp and floating point value.
 *
 * @author Michael Burman
 */
public class ChimpDecompressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private long storedVal = 0;
    private boolean first = true;
    private boolean endOfStream = false;

    private BitInput in;

    private final static long NAN_LONG = 0x7ff8000000000000L;

	public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};

    public ChimpDecompressor(BitInput input) {
        in = input;
    }

    /**
     * Returns the next pair in the time series, if available.
     *
     * @return Pair if there's next value, null if series is done.
     */
    public Value readPair() {
        next();
        if(endOfStream) {
            return null;
        }
        return new Value(storedVal);
    }

    private void next() {
        if (first) {
        	first = false;
            storedVal = in.getLong(64);
            if (storedVal == NAN_LONG) {
            	endOfStream = true;
            	return;
            }

        } else {
        	nextValue();
        }
    }

    private void nextValue() {


        // Read value
        if (in.readBit()) {
            if (in.readBit()) {
                // New leading zeros
                storedLeadingZeros = getLeading((int) in.getLong(3));
            }
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
            }

        } else if (in.readBit()) {
        	storedLeadingZeros = getLeading((int) in.getLong(3));
        	byte significantBits = (byte) in.getLong(6);
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
            }
        }
        // else -> same value as before
    }

	private int getLeading(int bits) {
		switch (bits) {
		case 0:
			return 0;
		case 1:
			return 8;
		case 2:
			return 12;
		case 3:
			return 16;
		case 4:
			return 18;
		case 5:
			return 20;
		case 6:
			return 22;
		case 7:
			return 24;
		}
		return 0;

	}

}