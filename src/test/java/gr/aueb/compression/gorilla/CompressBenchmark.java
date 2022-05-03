package gr.aueb.compression.gorilla;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput2;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Compressor;
import fi.iki.yak.ts.compression.gorilla.Decompressor;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 * @author Michael Burman
 */
public class CompressBenchmark {

	private class TimeseriesFileReader {
		private static final int DEFAULT_BLOCK_SIZE = 1_000;
		private static final String DELIMITER = ",";
		private static final int VALUE_POSITION = 2;
		BufferedReader bufferedReader;
		private int blocksize;

		public TimeseriesFileReader(InputStream inputStream) throws IOException {
			this(inputStream, DEFAULT_BLOCK_SIZE);
		}

		public TimeseriesFileReader(InputStream inputStream, int blocksize) throws IOException {
			InputStream gzipStream = new GZIPInputStream(inputStream);
			Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
			this.bufferedReader = new BufferedReader(decoder);
			this.blocksize = blocksize;
		}

		public double[] nextBlock() {
			double[] values = new double[DEFAULT_BLOCK_SIZE];
			String line;
			int counter = 0;
			try {
				while ((line = bufferedReader.readLine()) != null) {
					double value = Double.parseDouble(line.split(DELIMITER)[VALUE_POSITION]);
					values[counter++] = value;
					if (counter == blocksize) {
						return values;
					}
				}
			} catch (NumberFormatException | IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private static final int MINIMUM_TOTAL_BLOCKS = 50_000;

	@Test
	public void testSizeChimpForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Chimp compressor = new Chimp(output);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue(value);
			}
	        compressor.close();
	        encodingDuration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        ChimpDecompressor d = new ChimpDecompressor(input);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            fi.iki.yak.ts.compression.gorilla.Value pair = d.readPair();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.doubleValue(), pair.getDoubleValue(), "Value did not match");
	        }
	        assertNull(d.readPair());

		}
		System.out.println(String.format("Chimp: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testSizeChimp32ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Chimp32 compressor = new Chimp32(output);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue((float) value);
			}
	        compressor.close();
	        encodingDuration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        ChimpDecompressor32 d = new ChimpDecompressor32(input);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            Value pair = d.readPair();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.floatValue(), pair.getFloatValue(), "Value did not match");
	        }
	        assertNull(d.readPair());

		}
		System.out.println(String.format("Chimp32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testSizeChimpN32ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			ChimpN32 compressor = new ChimpN32(output, 128);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue((float) value);
			}
	        compressor.close();
	        encodingDuration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        byte[] arr = new byte[byteBuffer.remaining()];
	        byteBuffer.get(arr);
	        ByteBufferBitInput2 input = new ByteBufferBitInput2(new InputBitStream(arr));
	        ChimpNDecompressor32 d = new ChimpNDecompressor32(input, 128);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            Float pair = d.readValue();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.floatValue(), pair.floatValue(), "Value did not match");
	        }
	        assertNull(d.readValue());

		}
		System.out.println(String.format("ChimpN32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testSizeChimp128ForBaselTemp() throws IOException {
		String filename = "/home/panagiotis/Stocks-Germany.txt.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			ChimpN compressor = new ChimpN(output, 128);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue(value);
			}
	        compressor.close();
	        encodingDuration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        byte[] arr = new byte[byteBuffer.remaining()];
	        byteBuffer.get(arr);
	        ByteBufferBitInput2 input = new ByteBufferBitInput2(new InputBitStream(arr));
	        ChimpNDecompressor d = new ChimpNDecompressor(input, 128);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            Double pair = d.readValue();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.doubleValue(), pair.doubleValue(), "Value did not match");
	        }
	        assertNull(d.readValue());

		}
		System.out.println(String.format("ChimpN: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}


	@Test
	public void testSizeGorilla64ForBaselTemp() throws IOException {
		String filename = "/home/panagiotis/Stocks-Germany.txt.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Compressor compressor = new Compressor(output);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue(value);
			}
	        compressor.close();
	        encodingDuration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        Decompressor d = new Decompressor(input);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            fi.iki.yak.ts.compression.gorilla.Value pair = d.readPair();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.doubleValue(), pair.getDoubleValue(), "Value did not match");
	        }
	        assertNull(d.readPair());
		}
		System.out.println(String.format("Gorilla: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testSizeGorilla32ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Compressor32 compressor = new Compressor32(output);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue((float) value);
			}
	        compressor.close();
	        encodingDuration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        BitInput input = new ByteBufferBitInput(byteBuffer);
	        Decompressor32 d = new Decompressor32(input);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            Value pair = d.readValue();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.floatValue(), pair.getFloatValue(), "Value did not match");
	        }
	        assertNull(d.readValue());
		}
		System.out.println(String.format("Gorilla32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}


	@Test
	public void justTesting() {
	int test = (int) (15 + 2 * Math.pow(2, 6));
	System.out.println(test);
	System.out.println(bits(test, 26, 6));
	System.out.println(bits(test, 23, 3));

	}


	public static int bits(int n, int offset, int length) {
	    return n >> (32 - offset - length) & ~(-1 << length);
	}

}
