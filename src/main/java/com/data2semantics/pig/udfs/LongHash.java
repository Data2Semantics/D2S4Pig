package com.data2semantics.pig.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class LongHash extends EvalFunc<Long> {

	@Override
	public Long exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null)
			return hash("");
		try {
			return hash((String) input.get(0));
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row.", e);
		}
	}

	public static long hash(String string) {
		long h = 1125899906842597L; // prime
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + string.charAt(i);
		}
		return h;
	}
}