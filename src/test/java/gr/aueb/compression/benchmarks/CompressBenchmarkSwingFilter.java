package gr.aueb.compression.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import gr.aueb.compression.gorilla.DecompressorSwingFilter;
import gr.aueb.compression.gorilla.Point;
import gr.aueb.compression.gorilla.SwingFilter;
import gr.aueb.compression.gorilla.SwingFilter.SwingSegment;

/**
 * These are generic tests to test that input matches the output after
 * compression + decompression cycle, using both the timestamp and value
 * compression.
 *
 */
public class CompressBenchmarkSwingFilter {

	private static final int MINIMUM_TOTAL_BLOCKS = 100_000;
	private static String[] FILENAMES = {
//			"/home/panagiotis/timeseries/city_temperature-fixed.csv.gz",
//			"/home/panagiotis/timeseries/Stocks-UK.txt.gz",
//			"/home/panagiotis/timeseries/Stocks-USA.txt.gz",
//			"/home/panagiotis/timeseries/Stocks-Germany.txt.gz",
//			"/home/panagiotis/timeseries/basel-temp.csv.gz",
//			"/home/panagiotis/timeseries/basel-wind-speed.csv.gz",
//			"/home/panagiotis/timeseries/air-sensor-data.csv.gz",
//			"/home/panagiotis/timeseries/bird-migration-data.csv.gz",
//			"/home/panagiotis/timeseries/bitcoin-price-data.csv.gz",
//			"/home/panagiotis/timeseries/NEON_pressure-air_staPresMean.csv.gz",
//			"/home/panagiotis/timeseries/NEON_rel-humidity-buoy-dewTempMean.csv.gz",
//			"/home/panagiotis/timeseries/NEON_size-dust-particulate-PM10Median.csv.gz",
//			"/home/panagiotis/timeseries/NEON_temp-bio-bioTempMean.csv.gz",
//			"/home/panagiotis/timeseries/NEON_wind-2d_windDirMean.csv.gz",
			"/home/panagiotis/timeseries/foodprices.csv.gz",
			"/home/panagiotis/timeseries/poi-lat.csv.gz",
			"/home/panagiotis/timeseries/poi-lon.csv.gz",
			"/home/panagiotis/timeseries/bitcoin-transactions-output.csv.gz",
			"/home/panagiotis/timeseries/SSD_HDD_benchmarks.csv.gz" 
			};

	@Test
	public void testSwing() throws IOException {

		for (String filename : FILENAMES) {
			for (int logOfError = -7; logOfError < 12; logOfError++) {
				TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(
						new FileInputStream(new File(filename)));
				double[] values;
				double maxValue = Double.MIN_VALUE;
				double minValue = Double.MAX_VALUE;
				int timestamp = 0;
				double maxPrecisionError = 0;
				long totalSize = 0;
				float totalBlocks = 0;
				double totalStdev = 0D;
				long encodingDuration = 0;
				long decodingDuration = 0;
				while ((values = timeseriesFileReader.nextBlock()) != null && totalBlocks < MINIMUM_TOTAL_BLOCKS) {
					Collection<Point> points = new ArrayList<>();
					for (Double value : values) {
						points.add(new Point(timestamp++, value.floatValue()));
					}
	
					long start = System.nanoTime();
					List<SwingSegment> constants = new SwingFilter().filter(points, ((float) Math.pow(2, logOfError)));
					encodingDuration += System.nanoTime() - start;
	
					totalStdev += TimeseriesFileReader.sd(points.stream().map(l -> l.getValue()).collect(Collectors.toList()));
					totalBlocks += 1;
					totalSize += constants.size() * ( 2 * 64 + 32);
	
					DecompressorSwingFilter d = new DecompressorSwingFilter(constants);
					for (Double value : values) {
						start = System.nanoTime();
						maxValue = value > maxValue ? value : maxValue;
						minValue = value < minValue ? value : minValue;
						Float decompressedValue = d.readValue();
						decodingDuration += System.nanoTime() - start;
						double precisionError = Math.abs(value.doubleValue() - decompressedValue);
						maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
//						assertEquals(value.floatValue(), decompressedValue.floatValue(), ((float) Math.pow(2, logOfError)), "Value did not match");
					}
				}
				System.out.println(String.format(
						"Swing: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f, Error: %.8f, STDEV: %.2f, Error/STDEV: %.2f, Range: %.2f (%.2f)",
						filename, totalSize / (totalBlocks * TimeseriesFileReader.DEFAULT_BLOCK_SIZE),
						encodingDuration / totalBlocks, decodingDuration / totalBlocks, maxPrecisionError, totalStdev / totalBlocks, maxPrecisionError / (totalStdev / totalBlocks), (maxValue - minValue), 100* maxPrecisionError / (maxValue - minValue)));
	
			}
		}
	}
	

//	@Test
//	public void testSwing2() throws IOException {
//
//		for (String filename : FILENAMES) {
//
//			TimeseriesFileReader timeseriesFileReader = new TimeseriesFileReader(
//					new FileInputStream(new File(filename)));
//			double[] values;
//			int timestamp = 0;
//			Collection<Point> points = new ArrayList<>();
//			while ((values = timeseriesFileReader.nextBlock()) != null && points.size() < 1_000_000) {
//				for (Double value : values) {
//					points.add(new Point(timestamp++, value.floatValue()));
//				}
//
//			}
//			double stdev = TimeseriesFileReader.sd(points.stream().map(l -> l.getValue()).collect(Collectors.toList()));
//			for (int logOfError = -7; logOfError < 12; logOfError++) {
//				double maxValue = Double.MIN_VALUE;
//				double minValue = Double.MAX_VALUE;
//
//				double maxPrecisionError = 0;
//				long totalSize = 0;
//				float totalBlocks = 0;
//				long encodingDuration = 0;
//				long decodingDuration = 0;
//
//				long start = System.nanoTime();
//				List<SwingSegment> constants = new SwingFilter().filter(points, ((float) Math.pow(2, logOfError)));
//				encodingDuration += System.nanoTime() - start;
//
//				totalBlocks += 1;
//				totalSize += constants.size() * ( 2 * 64 + 32);
//
//				DecompressorSwingFilter d = new DecompressorSwingFilter(constants);
//				for (Point point : points) {
//					start = System.nanoTime();
//					maxValue = point.getValue() > maxValue ? point.getValue() : maxValue;
//					minValue = point.getValue() < minValue ? point.getValue() : minValue;
//					Float decompressedValue = d.readValue();
//					decodingDuration += System.nanoTime() - start;
//					double precisionError = Math.abs(point.getValue() - decompressedValue);
//					maxPrecisionError = (precisionError > maxPrecisionError) ? precisionError : maxPrecisionError;
////					assertEquals((float) point.getValue(), decompressedValue.floatValue(), ((float) Math.pow(2, logOfError)),
////							"Value did not match");
//				}
//				System.out.println(String.format(
//						"PmcMr: %s - Bits/value: %.2f, Compression time per block: %.2f, Decompression time per block: %.2f, Error: %.8f, STDEV: %.2f, Error/STDEV: %.2f, Range: %.2f (%.2f)",
//						filename, totalSize / ((float) points.size()), encodingDuration / totalBlocks,
//						decodingDuration / totalBlocks, maxPrecisionError, stdev, maxPrecisionError / stdev,
//						(maxValue - minValue), 100 * maxPrecisionError / (maxValue - minValue)));
//
//			}
//		}
//	}
}
