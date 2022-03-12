package fi.iki.yak.ts.compression.gorilla;

/**
 * Pair is an extracted timestamp,value pair from the stream
 *
 * @author Michael Burman
 */
public class Pair32 {
    private long timestamp;
    private int value;

    public Pair32(long timestamp, int value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public float getFloatValue() {
        return Float.intBitsToFloat(value);
    }

    public int getIntValue() {
        return value;
    }
}
