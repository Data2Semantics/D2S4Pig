package com.data2semantics.test.pig.loaders;

import static org.junit.Assert.*;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.data2semantics.pig.loaders.NtLoader;

public class NtLoaderTest {

	@Test
	public void testValidInput() throws Exception {
		MockRecordReader reader = new MockRecordReader("src/test/resources/test.nt");

		NtLoader custLoader = new NtLoader();

		custLoader.prepareToRead(reader, null);

		/**
		 * 1st line
		 */
		assertNull(custLoader.getNext()); //contains prefix. should be ignored
		
		/**
		 * 2nd line
		 */
		assertNull(custLoader.getNext()); //contains empty line. should be ignored
		
		/**
		 * 3d line
		 */
		Tuple line3 = custLoader.getNext();
		assertNotNull(line3);
		String sub = line3.get(0).toString();
		assertNotNull(sub);
		assertEquals("beforetabs", sub);
		String pred = line3.get(1).toString();
		assertEquals("<http://www.w3.org/2004/02/skos/core#exactMatch>", pred);
		String object = line3.get(2).toString();
		assertEquals("\"N Trip.les\"", object);
		assertEquals(3, line3.size());
		
		//4th line
		testObject(custLoader.getNext(), "\"N-Triples\"@en-US");

		//5th line
		testObject(custLoader.getNext(), "\"N-Triples\"@en-US");
		
		//6th line
		testObject(custLoader.getNext(), "<http://purl.obolibrary.org/obo/CHEBI_16643>");
		
		//7th line
		testObject(custLoader.getNext(), "<http://purl.obolibrary.org/obo/CHEBI_35812>");
		
		//8th line
		testObject(custLoader.getNext(), "\"foo\"^^<http://example.org/my/datatype>");
		
		//9th line
		testObject(custLoader.getNext(), "\"foo\"^^<http://example.org/my/datatype>");
		
		//10th line
		testObject(custLoader.getNext(), "\"Ted\"^^xsd:string");
		
	}
	
	private void testObject(Tuple line, String expected) throws ExecException {
		assertNotNull(line);
		String object = line.get(2).toString();
		assertEquals(expected, object);
	}

//	@Test(expected = IOException.class)
//	public void testInvalidInput() throws Exception {
//		MockRecordReader reader = new MockRecordReader("src/test/resources/valid1line_hit_data.tsv");
//
//		NtLoader custLoader = new NtLoader();
//
//		custLoader.prepareToRead(reader, null);
//
//		@SuppressWarnings("unused")
//		Tuple tuple = custLoader.getNext();
//	}

}