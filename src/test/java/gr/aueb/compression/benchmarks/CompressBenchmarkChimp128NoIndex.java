package gr.aueb.compression.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import gr.aueb.compression.gorilla.ChimpNDecompressor;
import gr.aueb.compression.gorilla.ChimpNNoIndex;

/**
 * These are generic tests to test that input matches the output after compression + decompression cycle, using
 * both the timestamp and value compression.
 *
 */
public class CompressBenchmarkChimp128NoIndex {

	private static final int MINIMUM_TOTAL_BLOCKS = 50_000;
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
	public void testChimp() throws IOException {
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
				ChimpNNoIndex compressor = new ChimpNNoIndex(128);
				long start = System.nanoTime();
				for (double value : values) {
					compressor.addValue(value);
				}
		        compressor.close();
		        encodingDuration += System.nanoTime() - start;
		        totalSize += compressor.getSize();
		        totalBlocks += 1;

				ChimpNDecompressor d = new ChimpNDecompressor(compressor.getOut(), 128);
				start = System.nanoTime();
				List<Double> uncompressedValues = d.getValues();
				decodingDuration += System.nanoTime() - start;
				for(int i=0; i<values.length; i++) {
		            assertEquals(values[i], uncompressedValues.get(i).doubleValue(), "Value did not match");
		        }
				

			}
			System.out.println(String.format("Chimp128-NoIndex: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f", filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE), encodingDuration / totalBlocks, decodingDuration / totalBlocks));
		}
	}

}
