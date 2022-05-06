package gr.aueb.compression.gorilla;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.brotli.BrotliCodec;
import org.apache.hadoop.hbase.io.compress.lz4.Lz4Codec;
import org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec;
import org.apache.hadoop.hbase.io.compress.xz.LzmaCodec;
import org.apache.hadoop.hbase.io.compress.zstd.ZstdCodec;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.jupiter.api.BeforeAll;
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

	private static final int MINIMUM_TOTAL_BLOCKS = 20_000;
	private static String FILENAME;
	
	@BeforeAll
	public static void setUp() {
		FILENAME = "/home/panagiotis/timeseries/Stocks-UK.txt.gz";
		FILENAME = "/home/panagiotis/timeseries/basel-temp.csv.gz";
		FILENAME = "/home/panagiotis/timeseries/foodprices.csv.gz";
		FILENAME = "/home/panagiotis/timeseries/poi-lat.csv.gz";
//		FILENAME = "/home/panagiotis/timeseries/poi-lon.csv.gz";
		//FILENAME = "/home/panagiotis/timeseries/bitcoin-transactions-output.csv.gz";
//		FILENAME = "/home/panagiotis/timeseries/SSD_HDD_benchmarks.csv.gz";
	}
	
	@Test
	public void testChimp() throws IOException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
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
	        byte[] arr = new byte[byteBuffer.remaining()];
	        byteBuffer.get(arr);
	        ByteBufferBitInput2 input = new ByteBufferBitInput2(new InputBitStream(arr));
	        ChimpDecompressor d = new ChimpDecompressor(input);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            fi.iki.yak.ts.compression.gorilla.Value pair = d.readPair();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.doubleValue(), pair.getDoubleValue(), "Value did not match");
	        }
	        assertNull(d.readPair());

		}
		System.out.println(String.format("Chimp: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testChimp32() throws IOException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
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
	        byte[] arr = new byte[byteBuffer.remaining()];
	        byteBuffer.get(arr);
	        ByteBufferBitInput2 input = new ByteBufferBitInput2(new InputBitStream(arr));
	        ChimpDecompressor32 d = new ChimpDecompressor32(input);
	        for(Double value : values) {
	        	start = System.nanoTime();
	            Value pair = d.readPair();
	            decodingDuration += System.nanoTime() - start;
	            assertEquals(value.floatValue(), pair.getFloatValue(), "Value did not match");
	        }
	        assertNull(d.readPair());

		}
		System.out.println(String.format("Chimp32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testChimpN32() throws IOException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBufferBitOutput output = new ByteBufferBitOutput();
			ChimpN32 compressor = new ChimpN32(output, 32);
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
		System.out.println(String.format("ChimpN32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testChimpN() throws IOException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
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
		System.out.println(String.format("ChimpN: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}


	@Test
	public void testGorilla64() throws IOException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
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
		System.out.println(String.format("Gorilla: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	@Test
	public void testGorilla32() throws IOException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
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
		System.out.println(String.format("Gorilla32: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}
	
	@Test
	public void testLZ4() throws IOException, InterruptedException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
			for(double d : values) {
			   bb.putDouble(d);
			}
			byte[] input = bb.array();
			
			CompressionCodec codec = new Lz4Codec();
			// We do this in Compression.java
		    ((Configurable) codec).getConf().setInt("io.file.buffer.size", 4 * 1024);
		    // Compress
		    long start = System.nanoTime();
		    org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    CompressionOutputStream out = codec.createOutputStream(baos, compressor);
		    out.write(input);
		    out.close();
		    encodingDuration += System.nanoTime() - start;
		    final byte[] compressed = baos.toByteArray();
		    totalSize += compressed.length * 8;
		    totalBlocks++;
		    
		    final byte[] plain = new byte[input.length];
		    org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
		    start = System.nanoTime();
		    CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
		    IOUtils.readFully(in, plain, 0, plain.length);
		    in.close();
		    double[] uncompressed = toDoubleArray(plain);
		    long duration = System.nanoTime() - start;
		    decodingDuration += duration;
		    // Decompressed bytes should equal the original
		    for(int i = 0; i < values.length; i++) {
	            assertEquals(values[i], uncompressed[i], "Value did not match");
	        }
		}
		System.out.println(String.format("LZ4: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}
	
	@Test
	public void testSnappy() throws IOException, InterruptedException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
			for(double d : values) {
			   bb.putDouble(d);
			}
			byte[] input = bb.array();
			
			Configuration conf = HBaseConfiguration.create();
		    // ZStandard levels range from 1 to 22.
		    // Level 22 might take up to a minute to complete. 3 is the Hadoop default, and will be fast.
		    conf.setInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_ZSTD_LEVEL_KEY, 3);
		    SnappyCodec codec = new SnappyCodec();
		    codec.setConf(conf);
			
		    // Compress
		    long start = System.nanoTime();
		    org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    CompressionOutputStream out = codec.createOutputStream(baos, compressor);
		    out.write(input);
		    out.close();
		    encodingDuration += System.nanoTime() - start;
		    final byte[] compressed = baos.toByteArray();
		    totalSize += compressed.length * 8;
		    totalBlocks++;
		    
		    final byte[] plain = new byte[input.length];
		    org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
		    start = System.nanoTime();
		    CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
		    IOUtils.readFully(in, plain, 0, plain.length);
		    in.close();
		    double[] uncompressed = toDoubleArray(plain);
		    decodingDuration += System.nanoTime() - start;
		    // Decompressed bytes should equal the original
		    for(int i = 0; i < values.length; i++) {
	            assertEquals(values[i], uncompressed[i], "Value did not match");
	        }
		}
		System.out.println(String.format("Snappy: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}
	
	@Test
	public void testZstd() throws IOException, InterruptedException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
			for(double d : values) {
			   bb.putDouble(d);
			}
			byte[] input = bb.array();
			
		    CompressionCodec codec = new ZstdCodec();
			
		    // Compress
		    long start = System.nanoTime();
		    org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    CompressionOutputStream out = codec.createOutputStream(baos, compressor);
		    out.write(input);
		    out.close();
		    encodingDuration += System.nanoTime() - start;
		    final byte[] compressed = baos.toByteArray();
		    totalSize += compressed.length * 8;
		    totalBlocks++;
		    
		    final byte[] plain = new byte[input.length];
		    org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
		    start = System.nanoTime();
		    CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
		    IOUtils.readFully(in, plain, 0, plain.length);
		    in.close();
		    double[] uncompressed = toDoubleArray(plain);
		    decodingDuration += System.nanoTime() - start;
		    // Decompressed bytes should equal the original
		    for(int i = 0; i < values.length; i++) {
	            assertEquals(values[i], uncompressed[i], "Value did not match");
	        }
		}
		System.out.println(String.format("Zstd: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	
	@Test
	public void testXz() throws IOException, InterruptedException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
			for(double d : values) {
			   bb.putDouble(d);
			}
			byte[] input = bb.array();
			
		    CompressionCodec codec = new LzmaCodec();
			
		    // Compress
		    long start = System.nanoTime();
		    org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    CompressionOutputStream out = codec.createOutputStream(baos, compressor);
		    out.write(input);
		    out.close();
		    encodingDuration += System.nanoTime() - start;
		    final byte[] compressed = baos.toByteArray();
		    totalSize += compressed.length * 8;
		    totalBlocks++;
		    
		    final byte[] plain = new byte[input.length];
		    org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
		    start = System.nanoTime();
		    CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
		    IOUtils.readFully(in, plain, 0, plain.length);
		    in.close();
		    double[] uncompressed = toDoubleArray(plain);
		    decodingDuration += System.nanoTime() - start;
		    // Decompressed bytes should equal the original
		    for(int i = 0; i < values.length; i++) {
	            assertEquals(values[i], uncompressed[i], "Value did not match");
	        }
		}
		System.out.println(String.format("Xz: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}
	
	@Test
	public void testBrotli() throws IOException, InterruptedException {
		TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
		long totalSize = 0;
		float totalBlocks = 0;
		double[] values;
		long encodingDuration = 0;
		long decodingDuration = 0;
		while ((values = timeseriesFileReader.nextBlock()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
			if (values == null) {
				timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(FILENAME)));
				values = timeseriesFileReader.nextBlock();
			}
			ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
			for(double d : values) {
			   bb.putDouble(d);
			}
			byte[] input = bb.array();
			
		    CompressionCodec codec = new BrotliCodec();
			
		    // Compress
		    long start = System.nanoTime();
		    org.apache.hadoop.io.compress.Compressor compressor = codec.createCompressor();
		    ByteArrayOutputStream baos = new ByteArrayOutputStream();
		    CompressionOutputStream out = codec.createOutputStream(baos, compressor);
		    out.write(input);
		    out.close();
		    encodingDuration += System.nanoTime() - start;
		    final byte[] compressed = baos.toByteArray();
		    totalSize += compressed.length * 8;
		    totalBlocks++;
		    
		    final byte[] plain = new byte[input.length];
		    org.apache.hadoop.io.compress.Decompressor decompressor = codec.createDecompressor();
		    start = System.nanoTime();
		    CompressionInputStream in = codec.createInputStream(new ByteArrayInputStream(compressed), decompressor);
		    IOUtils.readFully(in, plain, 0, plain.length);
		    in.close();
		    double[] uncompressed = toDoubleArray(plain);
		    decodingDuration += System.nanoTime() - start;
		    // Decompressed bytes should equal the original
		    for(int i = 0; i < values.length; i++) {
	            assertEquals(values[i], uncompressed[i], "Value did not match");
	        }
		}
		System.out.println(String.format("Brotli: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", FILENAME, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
	}

	
	public static double[] toDoubleArray(byte[] byteArray){
	    int times = Double.SIZE / Byte.SIZE;
	    double[] doubles = new double[byteArray.length / times];
	    for(int i=0;i<doubles.length;i++){
	        doubles[i] = ByteBuffer.wrap(byteArray, i*times, times).getDouble();
	    }
	    return doubles;
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
