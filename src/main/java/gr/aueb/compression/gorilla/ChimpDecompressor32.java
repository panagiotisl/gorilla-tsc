package gr.aueb.compression.gorilla;

import fi.iki.yak.ts.compression.gorilla.BitInput;

/**
 * Decompresses a compressed stream created by the Compressor. Returns pairs of timestamp and floating point value.
 *
 * @author Michael Burman
 */
public class ChimpDecompressor32 {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;
    private int storedVal = 0;
    private boolean first = true;
    private boolean endOfStream = false;

    private BitInput in;

    private final static int NAN_INT = 0x7fc00000;

	public final static short[] leadingRepresentation = {0, 8, 12, 16, 18, 20, 22, 24};
    
    public ChimpDecompressor32(BitInput input) {
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
            storedVal = (int) in.getLong(32);
            if (storedVal == NAN_INT) {
            	endOfStream = true;
            	return;
            }

        } else {
        	nextValue();
        }
    }

    private void nextValue() {

    	/*

    	if it.first {
		it.first = false

		// mark as finished if there were no values.
		if it.val == uvnan { // IsNaN
			it.finished = true
			return false
		}

		return true
	}

	// read compressed value
	var bit bool
	if it.br.CanReadBitFast() {
		bit = it.br.ReadBitFast()
	} else if v, err := it.br.ReadBit(); err != nil {
		it.err = err
		return false
	} else {
		bit = v
	}
	if !bit {
		var bit bool
		if it.br.CanReadBitFast() {
			bit = it.br.ReadBitFast()
		} else if v, err := it.br.ReadBit(); err != nil {
			it.err = err
			return false
		} else {
			bit = v
		}
		if !bit {
			it.val = it.val
		} else {
			bits, err := it.br.ReadBits(3)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = getLeadingBits(bits)
			bits, err = it.br.ReadBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := bits
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits

			sigbits, err := it.br.ReadBits(uint(mbits))
			if err != nil {
				it.err = err
				return false
			}

			vbits := it.val
			vbits ^= (sigbits << it.trailing)

			if vbits == uvnan { // IsNaN
				it.finished = true
				return false
			}
			it.val = vbits
		}
	} else {
		var bit bool
		if it.br.CanReadBitFast() {
			bit = it.br.ReadBitFast()
		} else if v, err := it.br.ReadBit(); err != nil {
			it.err = err
			return false
		} else {
			bit = v
		}
		if !bit {

            it.leading = it.leading

			mbits := 64 - it.leading
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 0
		} else {
			bits, err := it.br.ReadBits(3)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = getLeadingBits(bits)
			mbits := 64 - it.leading
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 0
		}

		mbits := uint(64 - it.leading - it.trailing)
		bits, err := it.br.ReadBits(mbits)
		if err != nil {
			it.err = err
			return false
		}

		vbits := it.val
		vbits ^= (bits << it.trailing)

		if vbits == uvnan { // IsNaN
			it.finished = true
			return false
		}
		it.val = vbits
	}


    	*/


        // Read value
        if (in.readBit()) {
            if (in.readBit()) {
                // New leading zeros
                storedLeadingZeros = leadingRepresentation[(int) in.getLong(3)];
            }
            int significantBits = 32 - storedLeadingZeros;
            if(significantBits == 0) {
                significantBits = 32;
            }
            int value = (int) in.getLong(32 - storedLeadingZeros);
            value = storedVal ^ value;
            if (value == NAN_INT) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
            }

        } else if (in.readBit()) {
        	storedLeadingZeros = leadingRepresentation[(int) in.getLong(3)];
        	byte significantBits = (byte) in.getLong(5);
        	if(significantBits == 0) {
                significantBits = 32;
            }
            storedTrailingZeros = 32 - significantBits - storedLeadingZeros;
            int value = (int) in.getLong(32 - storedLeadingZeros - storedTrailingZeros);
            value <<= storedTrailingZeros;
            value = storedVal ^ value;
            if (value == NAN_INT) {
            	endOfStream = true;
            	return;
            } else {
            	storedVal = value;
            }
        }
        // else -> same value as before
    }

}