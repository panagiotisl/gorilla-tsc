package gr.aueb.compression.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import gr.aueb.compression.gorilla.Compressor32;
import gr.aueb.compression.gorilla.Decompressor32;
import gr.aueb.compression.gorilla.Value;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 */
public class CompressBenchmarkGorilla32 {

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
	public void testCorilla() throws IOException {
		for (String filename : FILENAMES) {
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
		        ByteBufferBitInput input = new ByteBufferBitInput(byteBuffer);
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
	}

}
