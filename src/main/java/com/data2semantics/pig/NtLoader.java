package com.data2semantics.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

/**
 * N-triples loader for pig
 */
public class NtLoader extends LoadFunc {
	private static final byte DOUBLE_QUOTE = '"';
	private static final byte FIELD_DEL = ' ';
	private static final byte RECORD_DEL = '.';
	private static final byte URI_START = '<';
	private static final byte URI_END = '>';
	private ArrayList<Object> protoTuple;
	@SuppressWarnings("rawtypes")
	protected RecordReader reader = null;
	protected ResourceFieldSchema[] fields = null;
	protected final Log log = LogFactory.getLog(getClass());
	private TupleFactory tupleFactory = TupleFactory.getInstance();

	/**
	 * Communicate to the loader the location of the object(s) being loaded. The location string passed to the LoadFunc here is the return
	 * value of {@link LoadFunc#relativeToAbsolutePath(String, Path)}. Implementations should use this method to communicate the location
	 * (and any other information) to its underlying InputFormat through the Job object.
	 * 
	 * This method will be called in the backend multiple times. Implementations should bear in mind that this method is called multiple
	 * times and should ensure there are no inconsistent side effects due to the multiple calls.
	 * 
	 * @param location
	 *            Location as returned by {@link LoadFunc#relativeToAbsolutePath(String, Path)}
	 * @param job
	 *            the {@link Job} object store or retrieve earlier stored information from the {@link UDFContext}
	 * @throws IOException
	 *             if the location is not valid.
	 */
	public void setLocation(String location, Job job) throws IOException {
		// Tell our input format where we will be reading from
		FileInputFormat.setInputPaths(job, location);
	}

	/**
	 * This will be called during planning on the front end. This is the instance of InputFormat (rather than the class name) because the
	 * load function may need to instantiate the InputFormat in order to control how it is constructed.
	 * 
	 * @return the InputFormat associated with this loader.
	 * @throws IOException
	 *             if there is an exception during InputFormat construction
	 */
	@SuppressWarnings({ "rawtypes" })
	public InputFormat getInputFormat() throws IOException {
		// We will use TextInputFormat, the default Hadoop input format for
		// text. It has a LongWritable key that we will ignore, and the value
		// is a Text (a string writable) that the JSON data is in.
		return new TextInputFormat();
	}

	/**
	 * This will be called on the front end during planning and not on the back end during execution.
	 * 
	 * @return the {@link LoadCaster} associated with this loader. Returning null indicates that casts from byte array are not supported for
	 *         this loader.
	 * @throws IOException
	 *             if there is an exception during LoadCaster
	 */
	public LoadCaster getLoadCaster() throws IOException {
		// We do not expect to do casting of byte arrays, because we will be
		// returning typed data.
		return null;
	}

	/**
	 * Initializes LoadFunc for reading data. This will be called during execution before any calls to getNext. The RecordReader needs to be
	 * passed here because it has been instantiated for a particular InputSplit.
	 * 
	 * @param reader
	 *            {@link RecordReader} to be used by this instance of the LoadFunc
	 * @param split
	 *            The input {@link PigSplit} to process
	 * @throws IOException
	 *             if there is an exception during initialization
	 */
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
		this.reader = reader;
	}

	/**
	 * Retrieves the next tuple to be processed. Implementations should NOT reuse tuple objects (or inner member objects) they return across
	 * calls and should return a different tuple object in each call.
	 * 
	 * @return the next tuple to be processed or null if there are no more tuples to be processed.
	 * @throws IOException
	 *             if there is an exception while retrieving the next tuple
	 */
	public Tuple getNext() throws IOException {
		protoTuple = new ArrayList<Object>();

		boolean inField = false;
		boolean inQuotedField = false;
		boolean afterQuotedField = false;
		boolean inUriField = false;

		try {
			if (!reader.nextKeyValue()) {
				return null;
			}
			Text value = (Text) reader.getCurrentValue();
			byte[] buf = value.getBytes();
			int len = value.getLength();

			ByteBuffer fieldBuffer = ByteBuffer.allocate(len);

			for (int i = 0; i < len; i++) {
				byte b = buf[i];
				inField = true;
				if (inQuotedField) {
					if (b == DOUBLE_QUOTE) {
						inQuotedField = false;
						afterQuotedField = true;
//						inField = false;
					}
					fieldBuffer.put(b);
				} else if (afterQuotedField) {
					//add lang tags and stuff
					if (b == FIELD_DEL || b == RECORD_DEL) {
						afterQuotedField = false;
						inField = false;
						readField(fieldBuffer);
					} else {
						fieldBuffer.put(b);
					}
				} else if (inUriField) {
					if (b == URI_END) {
						inUriField = false;
						inField = false;
					}
					fieldBuffer.put(b);
				} else if (b == URI_START) {
					inUriField = true;
					inField = false;
					fieldBuffer.put(b);
				} else if (b == DOUBLE_QUOTE) {
					inQuotedField = true;
					fieldBuffer.put(b);
				} else if (b == FIELD_DEL) {
					inField = false;
					readField(fieldBuffer); // end of the field
				} else if (b == RECORD_DEL) {
					//end of record. stop now
				} else {
					fieldBuffer.put(b);
				}
			}
			if (inField) {
				readField(fieldBuffer);
			}
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}

		Tuple t = tupleFactory.newTupleNoCopy(protoTuple);
		return t;
	}

	private void readField(ByteBuffer buf) {
		byte[] bytes = new byte[buf.position()];
		buf.rewind();
		buf.get(bytes, 0, bytes.length);
		if (protoTuple.size() < 3) { //gracefully handle tuples which arent -triples- (warning here?)
			protoTuple.add(new DataByteArray(bytes));
		}
		buf.clear();
	}

}