package gr.aueb.compression.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import gr.aueb.compression.gorilla.Chimp32;
import gr.aueb.compression.gorilla.ChimpDecompressor32;
import gr.aueb.compression.gorilla.Value;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 */
public class CompressBenchmarkChimp32 {

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
	public void testChimp32() throws IOException {
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
				Chimp32 compressor = new Chimp32();
				long start = System.nanoTime();
				for (double value : values) {
					compressor.addValue((float) value);
				}
		        compressor.close();
		        encodingDuration += System.nanoTime() - start;
		        totalSize += compressor.getSize();
		        totalBlocks += 1;

		        ChimpDecompressor32 d = new ChimpDecompressor32(compressor.getOut());
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
	}

}
