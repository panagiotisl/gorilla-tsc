package gr.aueb.compression.gorilla;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Compressor;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 * @author Michael Burman
 */
public class CompressTest {

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

		public Collection<Double> nextBlock() {
			Collection<Double> list = new ArrayList<>();
			String line;
			try {
				while ((line = bufferedReader.readLine()) != null) {
					double value = Double.parseDouble(line.split(DELIMITER)[VALUE_POSITION]);
					list.add(value);
					if (list.size() == blocksize) {
						return list;
					}
				}
			} catch (NumberFormatException | IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	@Test
	public void testSizeCompressor64ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		int totalSize = 0;
		float totalBlocks = 0;
		Collection<Double> values;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			Compressor compressor = new Compressor(new ByteBufferBitOutput());
			values.forEach(value -> compressor.addValue(value));
	        compressor.close();
	        totalSize += compressor.getSize();
	        totalBlocks += 1;
		}
		System.out.println(String.format("%s - Size 64: %d, Bits/value: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE)));
	}

	@Test
	public void testSizeCompressor32ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		int totalSize = 0;
		float totalBlocks = 0;
		Collection<Double> values;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			Compressor32 compressor = new Compressor32(new ByteBufferBitOutput());
			values.forEach(value -> compressor.addValue(value.floatValue()));
	        compressor.close();
	        totalSize += compressor.getSize();
	        totalBlocks += 1;
		}
		System.out.println(String.format("%s - Size 32: %d, Bits/value: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE)));
	}

	@Test
	public void shouldEncode32ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		Collection<Double> values;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Compressor32 compressor = new Compressor32(output);
			values.forEach(value -> compressor.addValue(value.floatValue()));
	        compressor.close();

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        Decompressor32 d = new Decompressor32(input);
	        for(Double value : values) {
	            Value pair = d.readValue();
	            assertEquals(value.floatValue(), pair.getFloatValue(), "Value did not match");
	        }
	        assertNull(d.readValue());
		}
	}

	@Test
	public void testPrecision32ForBaselTemp() throws IOException {
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		Collection<Double> values;
		double maxValue = Double.MIN_VALUE;
		double minValue = Double.MAX_VALUE;
		double maxPrecisionError = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Compressor32 compressor = new Compressor32(output);
			values.forEach(value -> compressor.addValue(value.floatValue()));
	        compressor.close();

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        Decompressor32 d = new Decompressor32(input);
	        for(Double value : values) {
	        	maxValue = value > maxValue ? value : maxValue;
	        	minValue = value < minValue ? value : minValue;
	            Value pair = d.readValue();
	            double precisionError = Math.abs(value.doubleValue() - pair.getFloatValue());
	            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
	            assertEquals(value.doubleValue(), pair.getFloatValue(), 0.00001, "Value did not match");
	        }
	        assertNull(d.readValue());
		}
		System.out.println(String.format("%s - Max precision error: %e, Range: %f, (error/range %%: %e)", filename, maxPrecisionError, (maxValue - minValue), maxPrecisionError / (maxValue - minValue)));
	}

	@Test
	public void testSizeCompressor64ForBaselWindSpeed() throws IOException {
		String filename = "/basel-wind-speed.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		int totalSize = 0;
		float totalBlocks = 0;
		Collection<Double> values;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			Compressor compressor = new Compressor(new ByteBufferBitOutput());
			values.forEach(value -> compressor.addValue(value));
	        compressor.close();
	        totalSize += compressor.getSize();
	        totalBlocks += 1;
		}
		System.out.println(String.format("%s - Size 64: %d, Bits/value: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE)));
	}

	@Test
	public void testSizeCompressor32ForWindSpeed() throws IOException {
		String filename = "/basel-wind-speed.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		int totalSize = 0;
		float totalBlocks = 0;
		Collection<Double> values;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			Compressor32 compressor = new Compressor32(new ByteBufferBitOutput());
			values.forEach(value -> compressor.addValue(value.floatValue()));
	        compressor.close();
	        totalSize += compressor.getSize();
	        totalBlocks += 1;
		}
		System.out.println(String.format("%s - Size 32: %d, Bits/value: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE)));
	}

	@Test
	public void shouldEncode32ForBaselWindSpeed() throws IOException {
		String filename = "/basel-wind-speed.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		Collection<Double> values;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Compressor32 compressor = new Compressor32(output);
			values.forEach(value -> compressor.addValue(value.floatValue()));
	        compressor.close();

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        Decompressor32 d = new Decompressor32(input);
	        for(Double value : values) {
	            Value pair = d.readValue();
	            assertEquals(value.floatValue(), pair.getFloatValue(), "Value did not match");
	        }
	        assertNull(d.readValue());
		}
	}

	@Test
	public void testPrecision32ForBaselWindSpeed() throws IOException {
		String filename = "/basel-wind-speed.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		Collection<Double> values;
		double maxValue = Double.MIN_VALUE;
		double minValue = Double.MAX_VALUE;
		double maxPrecisionError = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			Compressor32 compressor = new Compressor32(output);
			values.forEach(value -> compressor.addValue(value.floatValue()));
	        compressor.close();

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        Decompressor32 d = new Decompressor32(input);
	        for(Double value : values) {
	        	maxValue = value > maxValue ? value : maxValue;
	        	minValue = value < minValue ? value : minValue;
	            Value pair = d.readValue();
	            double precisionError = Math.abs(value.doubleValue() - pair.getFloatValue());
	            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
	            assertEquals(value.doubleValue(), pair.getFloatValue(), 0.00001, "Value did not match");
	        }
	        assertNull(d.readValue());
		}
		System.out.println(String.format("%s - Max precision error: %e, Range: %f, (error/range %%: %e)", filename, maxPrecisionError, (maxValue - minValue), maxPrecisionError / (maxValue - minValue)));
	}

	@Test
	public void testPrecisionLossy32ForBaselTemp() throws IOException {

		for (int logOfError = -10; logOfError < 10; logOfError++) {
			String filename = "/basel-temp.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			Collection<Double> values;
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			float totalTrailingDiff = 0;
			int totalCases0 = 0;
			int totalCases1 = 0;
			int totalCases2 = 0;
			while ((values = timeseriesFileReader.nextBlock()) != null) {
				ByteBufferBitOutput output = new ByteBufferBitOutput();
				LossyCompressor32 compressor = new LossyCompressor32(output, logOfError);
				values.forEach(value -> compressor.addValue(value.floatValue()));
		        compressor.close();
		        totalSize += compressor.getSize();
		        totalBlocks += 1;
		        totalTrailingDiff += compressor.getTrailingDiff();
		        totalCases0 += compressor.getCases()[0];
		        totalCases1 += compressor.getCases()[1];
		        totalCases2 += compressor.getCases()[2];
		        ByteBuffer byteBuffer = output.getByteBuffer();
		        byteBuffer.flip();
		        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		        Decompressor32 d = new Decompressor32(input);
		        for(Double value : values) {
		        	maxValue = value > maxValue ? value : maxValue;
		        	minValue = value < minValue ? value : minValue;
		            Value pair = d.readValue();
		            double precisionError = Math.abs(value.doubleValue() - pair.getFloatValue());
		            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
		            assertEquals(value.doubleValue(), pair.getFloatValue(), Math.pow(2, logOfError), "Value did not match");
		        }
		        assertNull(d.readValue());
			}
			float total = totalCases0 + totalCases1 + totalCases2;
			System.out.println(String.format("Lossy32 %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%), Avg. Unexploited Trailing: %.2f, Cases 0: %.2f, 10: %.2f, 11: %.2f",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue), totalTrailingDiff / totalCases1, totalCases0 / total, totalCases1 / total, totalCases2 / total));
		}
	}

	@Test
	public void testPrecisionLossy32ForBaselWindSpeed() throws IOException {

		for (int logOfError = -10; logOfError < 10; logOfError++) {
			String filename = "/basel-wind-speed.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			Collection<Double> values;
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			float totalTrailingDiff = 0;
			int totalCases0 = 0;
			int totalCases1 = 0;
			int totalCases2 = 0;
			while ((values = timeseriesFileReader.nextBlock()) != null) {
				ByteBufferBitOutput output = new ByteBufferBitOutput();
				LossyCompressor32 compressor = new LossyCompressor32(output, logOfError);
				values.forEach(value -> compressor.addValue(value.floatValue()));
		        compressor.close();
		        totalSize += compressor.getSize();
		        totalBlocks += 1;
		        totalTrailingDiff += compressor.getTrailingDiff();
		        totalCases0 += compressor.getCases()[0];
		        totalCases1 += compressor.getCases()[1];
		        totalCases2 += compressor.getCases()[2];
		        ByteBuffer byteBuffer = output.getByteBuffer();
		        byteBuffer.flip();
		        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		        Decompressor32 d = new Decompressor32(input);
		        for(Double value : values) {
		        	maxValue = value > maxValue ? value : maxValue;
		        	minValue = value < minValue ? value : minValue;
		            Value pair = d.readValue();
		            double precisionError = Math.abs(value.doubleValue() - pair.getFloatValue());
		            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
		            assertEquals(value.doubleValue(), pair.getFloatValue(), Math.pow(2, logOfError), "Value did not match");
		        }
		        assertNull(d.readValue());
			}
			float total = totalCases0 + totalCases1 + totalCases2;
			System.out.println(String.format("Lossy32 %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%), Avg. Unexploited Trailing: %.2f, Cases 0: %.2f, 10: %.2f, 11: %.2f",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue), totalTrailingDiff / totalCases1, totalCases0 / total, totalCases1 / total, totalCases2 / total));
		}
	}

}
