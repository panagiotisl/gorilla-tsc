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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Compressor;
import gr.aueb.compression.gorilla.PmcMR.Constant;
import gr.aueb.compression.gorilla.SwingFilter.SwingSegment;

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


		public double[] nextBlockArray() {
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

	@Test
	public void testSizeChimpForBaselTemp() throws IOException {
		System.out.println("CHIMP");
		for (int i=0; i<30; i++) {
			String filename = "/basel-temp.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			long totalSize = 0;
			float totalBlocks = 0;
			double[] values;
			long duration = 0;
			while ((values = timeseriesFileReader.nextBlockArray()) != null) {
				ByteBufferBitOutput output = new ByteBufferBitOutput();
				Chimp compressor = new Chimp(output);
				long start = System.nanoTime();
				for (double value : values) {
					compressor.addValue(value);
				}
		        compressor.close();
		        duration += System.nanoTime() - start;
		        totalSize += compressor.getSize();
		        totalBlocks += 1;

		        ByteBuffer byteBuffer = output.getByteBuffer();
		        byteBuffer.flip();
		        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
		        ChimpDecompressor d = new ChimpDecompressor(input);
		        for(Double value : values) {
		            fi.iki.yak.ts.compression.gorilla.Value pair = d.readPair();
		            assertEquals(value.doubleValue(), pair.getDoubleValue(), "Value did not match");
		        }
		        assertNull(d.readPair());

			}
			System.out.println(duration);
			System.out.println(String.format("%s - Size 64: %d, Bits/value: %.2f, Compression time per block: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), duration / totalBlocks));
		}
		System.out.println("CHIMP");
	}

	@Test
	public void testSizeChimp128ForBaselTemp() throws IOException {
		System.out.println("CHIMP128");
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long duration = 0;
		while ((values = timeseriesFileReader.nextBlockArray()) != null || totalBlocks < 50000) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
				values = timeseriesFileReader.nextBlockArray();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			ChimpN compressor = new ChimpN(output, 128);
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue(value);
			}
	        compressor.close();
	        duration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;

	        ByteBuffer byteBuffer = output.getByteBuffer();
	        byteBuffer.flip();
	        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
	        ChimpNDecompressor d = new ChimpNDecompressor(input, 128);
	        for(Double value : values) {
	            fi.iki.yak.ts.compression.gorilla.Value pair = d.readPair();
	            assertEquals(value.doubleValue(), pair.getDoubleValue(), "Value did not match");
	        }
	        assertNull(d.readPair());

		}
		System.out.println(duration);
		System.out.println(String.format("%s - Size 64: %d, Bits/value: %.2f, Compression time per block: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), duration / totalBlocks));
		System.out.println("CHIMP128");
	}


	@Test
	public void testSizeCompressor64ForBaselTemp() throws IOException {
		System.out.println("GORILLA");
		String filename = "/basel-temp.csv.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long duration = 0;
		while ((values = timeseriesFileReader.nextBlockArray()) != null || totalBlocks < 50000) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
				values = timeseriesFileReader.nextBlockArray();
			}
			Compressor compressor = new Compressor(new ByteBufferBitOutput());
			long start = System.nanoTime();
			for (double value : values) {
				compressor.addValue(value);
			}
	        compressor.close();
	        duration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;
		}
		System.out.println(duration);
		System.out.println(String.format("%s - Size 64: %d, Bits/value: %.2f, Compression time per block: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), duration / totalBlocks));
		System.out.println("GORILLA");
	}

	@Test
	public void testSizeCompressor64ForStocksDE() throws IOException {
		String filename = "/home/panagiotis/Stocks-Germany.txt.gz";
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
		int totalSize = 0;
		float totalBlocks = 0;
		Collection<Double> values;
		long duration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null) {
			Compressor compressor = new Compressor(new ByteBufferBitOutput());
			long start = System.nanoTime();
			values.forEach(value -> compressor.addValue(value));
	        compressor.close();
	        duration += System.nanoTime() - start;
	        totalSize += compressor.getSize();
	        totalBlocks += 1;
		}

		System.out.println(duration);
		System.out.println(totalBlocks);
		System.out.println(String.format("%s - Size 64: %d, Bits/value: %.2f, Compression time per block: %.2f", filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), duration / totalBlocks));
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

	@Test
	public void testPrecisionNewLossy32ForBaselTemp() throws IOException {

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
				NewLossyCompressor32 compressor = new NewLossyCompressor32(output, logOfError);
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
		        NewDecompressor32 d = new NewDecompressor32(input);
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
			System.out.println(String.format("NewLossy32 %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%), Avg. Unexploited Trailing: %.2f, Cases 0: %.2f, 10: %.2f, 11: %.2f",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue), totalTrailingDiff / totalCases1, totalCases0 / total, totalCases1 / total, totalCases2 / total));
		}
	}

	@Test
	public void testPrecisionNewLossy32ForBaselWindSpeed() throws IOException {

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
				NewLossyCompressor32 compressor = new NewLossyCompressor32(output, logOfError);
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
		        NewDecompressor32 d = new NewDecompressor32(input);
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
			System.out.println(String.format("NewLossy32 %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%), Avg. Unexploited Trailing: %.2f, Cases 0: %.2f, 10: %.2f, 11: %.2f",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue), totalTrailingDiff / totalCases1, totalCases0 / total, totalCases1 / total, totalCases2 / total));
		}
	}

	@Test
	public void testPrecisionRunLengthEncodingLossy32ForBaselTemp() throws IOException {

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
				RunLengthEncodingLossyCompressor32 compressor = new RunLengthEncodingLossyCompressor32(output, logOfError);
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
		        RunLengthEncodingDecompressor32 d = new RunLengthEncodingDecompressor32(input);
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
			System.out.println(String.format("RLELossy32 %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%), Avg. Unexploited Trailing: %.2f, Cases 0: %.2f, 10: %.2f, 11: %.2f",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue), totalTrailingDiff / totalCases1, totalCases0 / total, totalCases1 / total, totalCases2 / total));
		}
	}

	@Test
	public void testPrecisionRunLengthEncodingLossy32ForBaselWindSpeed() throws IOException {

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
				RunLengthEncodingLossyCompressor32 compressor = new RunLengthEncodingLossyCompressor32(output, logOfError);
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
		        RunLengthEncodingDecompressor32 d = new RunLengthEncodingDecompressor32(input);
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
			System.out.println(String.format("RLELossy32 %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%), Avg. Unexploited Trailing: %.2f, Cases 0: %.2f, 10: %.2f, 11: %.2f",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue), totalTrailingDiff / totalCases1, totalCases0 / total, totalCases1 / total, totalCases2 / total));
		}
	}

	@Test
	public void testPmcMRFilterForBaselWindSpeed() throws IOException {
		for (int logOfError = -10; logOfError < 10; logOfError++) {
			String filename = "/basel-wind-speed.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			Collection<Double> values;
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			int timestamp = 0;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			while ((values = timeseriesFileReader.nextBlock()) != null) {
				Collection<Point> points = new ArrayList<>();
				for (Double value : values) {
					points.add(new Point(timestamp++, value.floatValue()));
				}
				List<Constant> constants = new PmcMR().filter(points, ((float) Math.pow(2, logOfError)));

		        totalBlocks += 1;
		        totalSize += constants.size() * 2 * 32;

		        DecompressorPmcMr d = new DecompressorPmcMr(constants);

		        for(Double value : values) {
		        	maxValue = value > maxValue ? value : maxValue;
		        	minValue = value < minValue ? value : minValue;
		            Float decompressedValue = d.readValue();
		            double precisionError = Math.abs(value.doubleValue() - decompressedValue);
		            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
		            assertEquals(value.doubleValue(), decompressedValue, Math.pow(2, logOfError), "Value did not match");
		        }

			}
			System.out.println(String.format("PMC-MR %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%)",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue)));
		}

	}

	@Test
	public void testPmcMRFilterForBaselTemp() throws IOException {
		for (int logOfError = -10; logOfError < 10; logOfError++) {
			String filename = "/basel-temp.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			Collection<Double> values;
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			int timestamp = 0;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			while ((values = timeseriesFileReader.nextBlock()) != null) {
				Collection<Point> points = new ArrayList<>();
				for (Double value : values) {
					points.add(new Point(timestamp++, value.floatValue()));
				}
				List<Constant> constants = new PmcMR().filter(points, ((float) Math.pow(2, logOfError)));

		        totalBlocks += 1;
		        totalSize += constants.size() * 2 * 32;

		        DecompressorPmcMr d = new DecompressorPmcMr(constants);

		        for(Double value : values) {
		        	maxValue = value > maxValue ? value : maxValue;
		        	minValue = value < minValue ? value : minValue;
		            Float decompressedValue = d.readValue();
		            double precisionError = Math.abs(value.doubleValue() - decompressedValue);
		            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
		            assertEquals(value.doubleValue(), decompressedValue, Math.pow(2, logOfError), "Value did not match");
		        }

			}
			System.out.println(String.format("PMC-MR %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%)",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue)));
		}

	}

	@Test
	public void testSwingFilterForBaselWindSpeed() throws IOException {
		for (int logOfError = -10; logOfError < 10; logOfError++) {
			String filename = "/basel-wind-speed.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			Collection<Double> values;
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			int timestamp = 0;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			while ((values = timeseriesFileReader.nextBlock()) != null) {
				Collection<Point> points = new ArrayList<>();
				for (Double value : values) {
					points.add(new Point(timestamp++, value.floatValue()));
				}
				List<SwingSegment> segments = new SwingFilter().filter(points, ((float) Math.pow(2, logOfError)));

		        totalBlocks += 1;
		        totalSize += segments.size() * 3 * 32;

		        DecompressorSwingFilter d = new DecompressorSwingFilter(segments);

		        for(Double value : values) {
		        	maxValue = value > maxValue ? value : maxValue;
		        	minValue = value < minValue ? value : minValue;
		            Float decompressedValue = d.readValue();
		            double precisionError = Math.abs(value.doubleValue() - decompressedValue);
		            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
		            assertEquals(value.doubleValue(), decompressedValue, Math.pow(2, logOfError + 10), "Value did not match");
		        }

			}
			System.out.println(String.format("SwingFilter %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%)",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue)));
		}

	}

	@Test
	public void testSwingFilterForBaselTemp() throws IOException {
		for (int logOfError = -10; logOfError < 10; logOfError++) {
			String filename = "/basel-temp.csv.gz";
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(this.getClass().getResourceAsStream(filename));
			Collection<Double> values;
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			int timestamp = 0;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			while ((values = timeseriesFileReader.nextBlock()) != null) {
				Collection<Point> points = new ArrayList<>();
				for (Double value : values) {
					points.add(new Point(timestamp++, value.floatValue()));
				}
				List<SwingSegment> segments = new SwingFilter().filter(points, ((float) Math.pow(2, logOfError)));

		        totalBlocks += 1;
		        totalSize += segments.size() * 3 * 32;

		        DecompressorSwingFilter d = new DecompressorSwingFilter(segments);

		        for(Double value : values) {
		        	maxValue = value > maxValue ? value : maxValue;
		        	minValue = value < minValue ? value : minValue;
		            Float decompressedValue = d.readValue();
		            double precisionError = Math.abs(value.doubleValue() - decompressedValue);
		            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
		            assertEquals(value.doubleValue(), decompressedValue, Math.pow(2, logOfError), "Value did not match");
		        }

			}
			System.out.println(String.format("SwingFilter %s - Size : %d, Bits/value: %.2f, error: %f, Range: %.2f, (%.2f%%)",
					filename, totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), maxPrecisionError, (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue)));
		}

	}

	@Test
	public void testSwingFilterSimple() throws IOException {
		for (int logOfError = -10; logOfError < -9; logOfError++) {
			Collection<Double> values = new ArrayList<>();
			values.add(0.0);
			values.add(3.2399998);
			values.add(1.08);
			values.add(1.1384199);
			values.add(3.4152596);
			values.add(4.3349743);
			values.add(5.95906);
			values.add(5.495161);
			values.add(4.0249224);
			values.add(2.0991426);
			values.add(4.452954);
			values.add(7.0911775);
			values.add(6.9527545);
			values.add(6.379216);
			values.add(5.506941);
			values.add(2.5959969);
			values.add(2.8799999);
			values.add(3.6179552);
			values.add(4.1046314);
			values.add(10.086427);
			values.add(11.570515);
			values.add(11.212135);
			values.add(9.885262);
			values.add(8.049845);
			values.add(5.4477882);
			values.add(2.2768397);
			values.add(1.4399999);
			values.add(2.16);
			values.add(6.12);
			values.add(9.826088);
			values.add(15.778518);
			values.add(15.807239);
			values.add(16.75064);
			values.add(19.66536);
			values.add(19.930477);
			values.add(19.586117);
			values.add(18.448023);
			values.add(16.946787);
			values.add(16.09969);
			values.add(14.408997);
			values.add(12.074766);
			values.add(13.207634);
			values.add(11.966953);
			values.add(12.55879);
			values.add(11.435313);
			values.add(16.179987);
			values.add(19.826164);
			values.add(22.065973);
			values.add(20.929594);
			values.add(19.652176);

			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			int timestamp = 0;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			Collection<Point> points = new ArrayList<>();
			for (Double value : values) {
				points.add(new Point(timestamp++, value.floatValue()));
			}
			List<SwingSegment> lines = new SwingFilter().filter(points, ((float) Math.pow(2, logOfError)));

	        totalBlocks += 1;
	        totalSize += lines.size() * 3 * 32;

	        DecompressorSwingFilter d = new DecompressorSwingFilter(lines);

	        for(Double value : values) {
	        	maxValue = value > maxValue ? value : maxValue;
	        	minValue = value < minValue ? value : minValue;
	            Float decompressedValue = d.readValue();
	            double precisionError = Math.abs(value.doubleValue() - decompressedValue);
	            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
	            assertEquals(value.doubleValue(), decompressedValue, Math.pow(2, logOfError), "Value did not match");

			System.out.println(String.format("SwingFilter %s - Size : %d, Bits/value: %.2f",
					"simple", totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE)));
	        }
		}
	}

	//@Test
	public void testPmcMRfilterSimple() throws IOException {
		for (int logOfError = -10; logOfError < 10; logOfError++) {
			Collection<Double> values = new ArrayList<>();
			values.add(0.0);
			values.add(3.2399998);
			values.add(1.08);
			values.add(1.1384199);
			values.add(3.4152596);
			values.add(4.3349743);
			double maxValue = Double.MIN_VALUE;
			double minValue = Double.MAX_VALUE;
			int timestamp = 0;
			double maxPrecisionError = 0;
			int totalSize = 0;
			float totalBlocks = 0;
			Collection<Point> points = new ArrayList<>();
			for (Double value : values) {
				points.add(new Point(timestamp++, value.floatValue()));
			}
			List<Constant> constants = new PmcMR().filter(points, ((float) Math.pow(2, logOfError)));

	        totalBlocks += 1;
	        totalSize += constants.size() * 2 * 32;

	        DecompressorPmcMr d = new DecompressorPmcMr(constants);

	        for(Double value : values) {
	        	maxValue = value > maxValue ? value : maxValue;
	        	minValue = value < minValue ? value : minValue;
	            Float decompressedValue = d.readValue();
	            double precisionError = Math.abs(value.doubleValue() - decompressedValue);
	            maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
	            System.out.println(value.doubleValue() + " " + decompressedValue);
	            assertEquals(value.doubleValue(), decompressedValue, Math.pow(2, logOfError), "Value did not match");
	        }

			System.out.println(String.format("Lossy32 %s - Size : %d, Bits/value: %.2f",
					"simple", totalSize, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE)));
		}



	}

}
