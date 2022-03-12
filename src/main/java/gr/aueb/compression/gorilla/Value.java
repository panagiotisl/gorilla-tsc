package gr.aueb.compression.gorilla;

/**
 * Pair is an extracted timestamp,value pair from the stream
 *
 * @author Michael Burman
 */
public class Value {
    private int value;

    public Value(int value) {
        this.value = value;
    }

    public float getFloatValue() {
        return Float.intBitsToFloat(value);
    }

    public int getIntValue() {
        return value;
    }
}
