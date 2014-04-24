package com.data2semantics.test.pig.loaders;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.data2semantics.pig.loaders.NtLoader;

public class NtLoaderTest {

	@Test
	public void testValidInput() throws Exception {
		MockRecordReader reader = new MockRecordReader("src/test/resources/test.nt");

//		NtLoader custLoader = new NtLoader();
//
//		custLoader.prepareToRead(reader, null);
//
//		/**
//		 * 1st line
//		 */
////		assertNull(custLoader.getNext()); //contains prefix. should be ignored
//		custLoader.getNext(); //contains prefix. should be ignored
//		
//		/**
//		 * 2nd line
//		 */
//		assertNull(custLoader.getNext()); //contains empty line. should be ignored
//		
//		/**
//		 * 3d line
//		 */
//		Tuple line3 = custLoader.getNext();
//		assertNotNull(line3);
//		String sub = line3.get(0).toString();
//		assertNotNull(sub);
//		assertEquals("beforetabs", sub);
//		String pred = line3.get(1).toString();
//		assertEquals("<http://www.w3.org/2004/02/skos/core#exactMatch>", pred);
//		String object = line3.get(2).toString();
//		assertEquals("\"N Trip.les\"", object);
//		assertEquals(3, line3.size());
//		
//		//4th line
//		testObject(custLoader.getNext(), "\"N-Triples\"@en-US");
//
//		//5th line
//		testObject(custLoader.getNext(), "\"N-Triples\"@en-US");
//		
//		//6th line
//		testObject(custLoader.getNext(), "<http://purl.obolibrary.org/obo/CHEBI_16643>");
//		
//		//7th line
//		testObject(custLoader.getNext(), "<http://purl.obolibrary.org/obo/CHEBI_35812>");
//		
//		//8th line
//		testObject(custLoader.getNext(), "\"foo\"^^<http://example.org/my/datatype>");
//		
//		//9th line
//		testObject(custLoader.getNext(), "\"foo\"^^<http://example.org/my/datatype>");
//		
//		//10th line
//		testObject(custLoader.getNext(), "\"Ted\"^^xsd:string");
//		
//		//11th line
//		testObject(custLoader.getNext(), "\"bladiebla\\\"bla\"@en");
//		
//		//12th line
//		testObject(custLoader.getNext(), "\"\\\"bladieblabla\"@en");
//		
//		//13th line
//		custLoader.getNext();
//		
//		//14th line
//		testObject(custLoader.getNext(), "\"\\\"bladieblabla\\\\\"@en");
	}
	
	private void testObject(Tuple line, String expected) throws ExecException {
		assertNotNull(line);
		String object = line.get(2).toString();
		assertEquals(expected, object);
	}
	
	@Test
	public void testMissingTuple() throws Exception {
		MockRecordReader reader = new MockRecordReader("src/test/resources/testMissingTuple.nt");

		NtLoader custLoader = new NtLoader();

		custLoader.prepareToRead(reader, null);
		
		Tuple line1 = custLoader.getNext();
		assertNotNull(line1);
		assertEquals(3, line1.size());
		assertTrue(allTuplesFilled(line1));
		
		Tuple line2 = custLoader.getNext();
		assertNotNull(line2);
		assertEquals(3, line2.size());
		assertTrue(allTuplesFilled(line2));

		Tuple line3 = custLoader.getNext();
		assertNotNull(line3);
		assertEquals(3, line3.size());
		assertTrue(allTuplesFilled(line3));
		
		Tuple line4 = custLoader.getNext();
		assertNotNull(line4);
		assertEquals(3, line4.size());
		assertTrue(allTuplesFilled(line4));
		
		Tuple line5 = custLoader.getNext();
		assertNotNull(line5);
		assertEquals(3, line5.size());
		assertTrue(allTuplesFilled(line5));
		
		Tuple line6 = custLoader.getNext();
		assertNotNull(line6);
		assertEquals(3, line6.size());
		assertTrue(allTuplesFilled(line6));
		
	}
	
	private boolean allTuplesFilled(Tuple tuple) throws ExecException {
		boolean filled = true;
		for (int i = 0; i < tuple.size(); i++) {
			if (tuple.get(i).toString().trim().length() == 0) {
				filled = false;
				break;
			}
		}
		return filled;
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