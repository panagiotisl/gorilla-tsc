package gr.aueb.compression.benchmarks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

public class TimeseriesFileReader {
		public static final int DEFAULT_BLOCK_SIZE = 1_000;
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
