package gr.aueb.compression.gorilla;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 * @author Michael Burman
 */
public class Encode32Test {

    private void comparePairsToCompression(Value[] pairs) {
        ByteBufferBitOutput output = new ByteBufferBitOutput();
        Compressor32 c = new Compressor32(output);
        Arrays.stream(pairs).forEach(p -> c.addValue(p.getFloatValue()));
        c.close();
        System.out.println("Size: " + c.getSize());

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor32 d = new Decompressor32(input);

        // Replace with stream once decompressor supports it
        for(int i = 0; i < pairs.length; i++) {
            Value pair = d.readValue();
            assertEquals(pairs[i].getFloatValue(), pair.getFloatValue(), "Value did not match");
        }

        assertNull(d.readValue());
    }

    @Test
    void simpleEncodeAndDecodeTest() throws Exception {

        Value[] values = {
                new Value(Float.floatToRawIntBits((float) 1.0)),
                new Value(Float.floatToRawIntBits((float) -2.0)),
                new Value(Float.floatToRawIntBits((float) -2.5)),
                new Value(Float.floatToRawIntBits(65537)),
                new Value(Float.floatToRawIntBits((float) 2147483650.0)),
                new Value(Float.floatToRawIntBits(-16384)),
                new Value(Float.floatToRawIntBits((float) 2.8)),
                new Value(Float.floatToRawIntBits((float) -38.0)),
        };

        comparePairsToCompression(values);
    }

    @Test
    public void willItBlend() throws Exception {
        Value[] values = {
                new Value(69087),
                new Value(65640),
                new Value(58155),
                new Value(61025),
                new Value(91156),
                new Value(37516),
                new Value(93515),
                new Value(96226),
                new Value(23833),
                new Value(73186),
                new Value(96947),
                new Value(46927),
                new Value(77954),
                new Value(29302),
                new Value(6700),
                new Value(71971),
                new Value(8528),
                new Value(85321),
                new Value(83229),
                new Value(78298),
                new Value(87122),
                new Value(82055),
                new Value(75067),
                new Value(33680),
                new Value(17576),
                new Value(89701),
                new Value(21427),
                new Value(58255),
                new Value(3768),
                new Value(62086),
                new Value(66965),
                new Value(35801),
                new Value(72169),
                new Value(43089),
                new Value(31418),
                new Value(84781),
                new Value(36103),
                new Value(87431),
                new Value(7379),
                new Value(66919),
                new Value(30906),
                new Value(88630),
                new Value(27546),
                new Value(43813),
                new Value(2124),
                new Value(49399),
                new Value(94577),
                new Value(98459),
                new Value(49457),
                new Value(92838),
                new Value(15628),
                new Value(53916),
                new Value(90387),
                new Value(43176),
                new Value(18838),
                new Value(78847),
                new Value(39591),
                new Value(77070),
                new Value(56788),
                new Value(96706),
                new Value(20756),
                new Value(64433),
                new Value(45791),
                new Value(75028),
                new Value(55403),
                new Value(36991),
                new Value(92929),
                new Value(60416),
                new Value(55485),
                new Value(53525),
                new Value(96021),
                new Value(22705),
                new Value(89801),
                new Value(51975),
                new Value(86741),
                new Value(22440),
                new Value(51818),
                new Value(61965),
                new Value(19074),
                new Value(54521),
                new Value(59315),
                new Value(19171),
                new Value(98800),
                new Value(7086),
                new Value(60578),
                new Value(96828),
                new Value(83746),
                new Value(85481),
                new Value(22346),
                new Value(80976),
                new Value(43586),
                new Value(82500),
                new Value(13576),
                new Value(77871),
                new Value(60978),
                new Value(35264),
                new Value(79733),
                new Value(29140),
                new Value(7237),
                new Value(52866),
                new Value(88456),
                new Value(33533),
                new Value(96961),
                new Value(16389),
                new Value(31181),
                new Value(63282),
                new Value(92857),
                new Value(4582),
                new Value(46832),
                new Value(6335),
                new Value(44367),
                new Value(84640),
                new Value(36174),
                new Value(40075),
                new Value(80886),
                new Value(43784),
                new Value(25077),
                new Value(18617),
                new Value(72681),
                new Value(84811),
                new Value(90053),
                new Value(25708),
                new Value(57134),
                new Value(87193),
                new Value(66057),
                new Value(51404),
                new Value(90141),
                new Value(10434),
                new Value(29056),
                new Value(48160),
                new Value(96652),
                new Value(64141),
                new Value(22143),
                new Value(20561),
                new Value(66401),
                new Value(76802),
                new Value(37555),
                new Value(63169),
                new Value(45712),
                new Value(44751),
                new Value(98891),
                new Value(38122),
                new Value(46202),
                new Value(5875),
                new Value(17397),
                new Value(39994),
                new Value(82385),
                new Value(15598),
                new Value(36235),
                new Value(97536),
                new Value(28557),
                new Value(13985),
                new Value(64304),
                new Value(83693),
                new Value(6574),
                new Value(25134),
                new Value(50383),
                new Value(55922),
                new Value(73436),
                new Value(68235),
                new Value(1469),
                new Value(44315),
                new Value(95064),
                new Value(1997),
                new Value(17247),
                new Value(42454),
                new Value(73631),
                new Value(96890),
                new Value(43450),
                new Value(42042),
                new Value(83014),
                new Value(32051),
                new Value(69280),
                new Value(21425),
                new Value(93748),
                new Value(64151),
                new Value(38791),
                new Value(5248),
                new Value(92935),
                new Value(18516),
                new Value(98870),
                new Value(82244),
                new Value(65464),
                new Value(33801),
                new Value(18331),
                new Value(89744),
                new Value(98460),
                new Value(24709),
                new Value(8407),
                new Value(69451),
                new Value(51100),
                new Value(25309),
                new Value(16148),
                new Value(98974),
                new Value(80284),
                new Value(170),
                new Value(34706),
                new Value(39681),
                new Value(6140),
                new Value(64595),
                new Value(59862),
                new Value(53795),
                new Value(83493),
                new Value(90639),
                new Value(16777),
                new Value(11096),
                new Value(38512),
                new Value(52759),
                new Value(79567),
                new Value(48664),
                new Value(10710),
                new Value(25635),
                new Value(40985),
                new Value(94089),
                new Value(50056),
                new Value(15550),
                new Value(78823),
                new Value(9044),
                new Value(20782),
                new Value(86390),
                new Value(79444),
                new Value(84051),
                new Value(91554),
                new Value(58777),
                new Value(89474),
                new Value(94026),
                new Value(41613),
                new Value(64667),
                new Value(5160),
                new Value(45140),
                new Value(53704),
                new Value(68097),
                new Value(81137),
                new Value(59657),
                new Value(56572),
                new Value(1993),
                new Value(62608),
                new Value(76489),
                new Value(22147),
                new Value(92829),
                new Value(48499),
                new Value(89152),
                new Value(9191),
                new Value(49881),
                new Value(96020),
                new Value(90203),
                new Value(32217),
                new Value(94302),
                new Value(83111),
                new Value(75576),
                new Value(5973),
                new Value(5175),
                new Value(63350),
                new Value(44081)
        };

        comparePairsToCompression(values);
    }

    /**
     * Tests encoding of similar floats, see https://github.com/dgryski/go-tsz/issues/4 for more information.
     */
    @Test
    void testEncodeSimilarFloats() throws Exception {
        ByteBufferBitOutput output = new ByteBufferBitOutput();
        Compressor32 c = new Compressor32(output);

        ByteBuffer bb = ByteBuffer.allocate(5 * 2*Long.BYTES);

        bb.putFloat((float) 6.00065e+06);
        bb.putFloat((float) 6.000656e+06);
        bb.putFloat((float) 6.000657e+06);
        bb.putFloat((float) 6.000659e+06);
        bb.putFloat((float) 6.000661e+06);

        bb.flip();

        for(int j = 0; j < 5; j++) {
            c.addValue(bb.getFloat());
        }

        c.close();
        System.out.println("Size: " + c.getSize());

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor32 d = new Decompressor32(input);

        // Replace with stream once decompressor supports it
        for(int i = 0; i < 5; i++) {
            Value pair = d.readValue();
            assertEquals(bb.getFloat(), pair.getFloatValue(), "Value did not match");
        }
        assertNull(d.readValue());
    }

    /**
     * Tests writing enough large amount of datapoints that causes the included ByteBufferBitOutput to do
     * internal byte array expansion.
     */
    @Test
    void testEncodeLargeAmountOfData() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 100000;
        ByteBufferBitOutput output = new ByteBufferBitOutput();

        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putFloat((float) (i * Math.random()));
        }

        Compressor32 c = new Compressor32(output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getFloat());
        }

        c.close();
        System.out.println("Size: " + c.getSize());

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor32 d = new Decompressor32(input);

        for(int i = 0; i < amountOfPoints; i++) {
            double val = bb.getFloat();
            Value pair = d.readValue();
            assertEquals(val, pair.getFloatValue());
        }
        assertNull(d.readValue());
    }

    /**
     * Although not intended usage, an empty block should not cause errors
     */
    @Test
    void testEmptyBlock() throws Exception {
        ByteBufferBitOutput output = new ByteBufferBitOutput();

        Compressor32 c = new Compressor32(output);
        c.close();
        System.out.println("Size: " + c.getSize());

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor32 d = new Decompressor32(input);

        assertNull(d.readValue());
    }

    @Test
    void testLongEncoding() throws Exception {
        // This test should trigger ByteBuffer reallocation
        int amountOfPoints = 10000;
        ByteBufferBitOutput output = new ByteBufferBitOutput();

        ByteBuffer bb = ByteBuffer.allocateDirect(amountOfPoints * 2*Long.BYTES);

        for(int i = 0; i < amountOfPoints; i++) {
            bb.putInt((int) ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE));
        }

        Compressor32 c = new Compressor32(output);

        bb.flip();

        for(int j = 0; j < amountOfPoints; j++) {
            c.addValue(bb.getInt());
        }

        c.close();
        System.out.println("Size: " + c.getSize());

        bb.flip();

        ByteBuffer byteBuffer = output.getByteBuffer();
        byteBuffer.flip();

        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
        Decompressor32 d = new Decompressor32(input);

        for(int i = 0; i < amountOfPoints; i++) {
            long val = bb.getInt();
            Value pair = d.readValue();
            assertEquals(val, pair.getIntValue());
        }
        assertNull(d.readValue());
    }
}
