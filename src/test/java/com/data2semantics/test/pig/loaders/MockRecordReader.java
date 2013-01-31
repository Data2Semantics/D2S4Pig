package com.data2semantics.test.pig.loaders;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

@SuppressWarnings("rawtypes")
public class MockRecordReader extends RecordReader {

	private BufferedReader reader;
	private long key;
	private boolean linesLeft;

	/**
	 * call this to load the file
	 * 
	 * @param fileLocation
	 * @throws FileNotFoundException
	 */

	public MockRecordReader(String fileLocation) throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(fileLocation));
		key = 0;
		linesLeft = true;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Long getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		String line = reader.readLine();

		if (line != null) {
			key++;
		} else {
			linesLeft = false;
		}
		return new Text(line);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// dont need this for unit testing
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// not initializing anything during unit testing.

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return linesLeft;
	}

}