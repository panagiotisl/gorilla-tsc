package gr.aueb.compression.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.xz.LzmaCodec;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.jupiter.api.Test;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 */
public class CompressBenchmarkXz32 {

	private static final int MINIMUM_TOTAL_BLOCKS = 20_000;
	private static String[] FILENAMES = {
	        "/home/panagiotis/timeseries/city_temperature-fixed.csv.gz",
	        "/home/panagiotis/timeseries/Stocks-UK.txt.gz",
	        "/home/panagiotis/timeseries/Stocks-USA.txt.gz",
	        "/home/panagiotis/timeseries/Stocks-Germany.txt.gz",
	        "/home/panagiotis/timeseries/basel-temp.csv.gz",
	        "/home/panagiotis/timeseries/basel-wind-speed.csv.gz",
	        "/home/panagiotis/timeseries/air-sensor-data.csv.gz",
	        "/home/panagiotis/timeseries/bird-migration-data.csv.gz",
	        "/home/panagiotis/timeseries/bitcoin-price-data.csv.gz",
	        "/home/panagiotis/timeseries/NEON_pressure-air_staPresMean.csv.gz",
	        "/home/panagiotis/timeseries/NEON_rel-humidity-buoy-dewTempMean.csv.gz",
	        "/home/panagiotis/timeseries/NEON_size-dust-particulate-PM10Median.csv.gz",
	        "/home/panagiotis/timeseries/NEON_temp-bio-bioTempMean.csv.gz",
	        "/home/panagiotis/timeseries/NEON_wind-2d_windDirMean.csv.gz",
	        "/home/panagiotis/timeseries/foodprices.csv.gz",
	        "/home/panagiotis/timeseries/poi-lat.csv.gz",
	        "/home/panagiotis/timeseries/poi-lon.csv.gz",
	        "/home/panagiotis/timeseries/bitcoin-transactions-output.csv.gz",
	        "/home/panagiotis/timeseries/SSD_HDD_benchmarks.csv.gz"
			};

	
	@Test
	public void testZstd() throws IOException, InterruptedException {
		for (String filename : FILENAMES) {
			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
			long totalSize = 0;
			float totalBlocks = 0;
			float[] values;
			long encodingDuration = 0;
			long decodingDuration = 0;
			while ((values = timeseriesFileReader.nextBlock32()) != null || totalBlocks < MINIMUM_TOTAL_BLOCKS) {
				if (values == null) {
					timeseriesFileReader = new TimeseriesFileReader(new FileInputStream(new File(filename)));
					values = timeseriesFileReader.nextBlock32();
				}
				ByteBuffer bb = ByteBuffer.allocate(values.length * 8);
				for(float d : values) {
				   bb.putFloat(d);
				}
				byte[] input = bb.array();
				
				Configuration conf = new Configuration();
			    // LZMA levels range from 1 to 9.
			    // Level 9 might take several minutes to complete. 3 is our default. 1 will be fast.
			    conf.setInt(LzmaCodec.LZMA_LEVEL_KEY, 3);
			    LzmaCodec codec = new LzmaCodec();
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
			    float[] uncompressed = toFloatArray(plain);
			    decodingDuration += System.nanoTime() - start;
			    // Decompressed bytes should equal the original
			    for(int i = 0; i < values.length; i++) {
		            assertEquals(values[i], uncompressed[i], "Value did not match");
		        }
			}
			System.out.println(String.format("Xz: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
		}
	}
	
	public static float[] toFloatArray(byte[] byteArray){
	    int times = Float.SIZE / Byte.SIZE;
	    float[] floats = new float[byteArray.length / times];
	    for(int i=0;i<floats.length;i++){
	        floats[i] = ByteBuffer.wrap(byteArray, i*times, times).getFloat();
	    }
	    return floats;
	}

}
