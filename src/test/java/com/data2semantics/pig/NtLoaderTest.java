package com.data2semantics.pig;

import static org.junit.Assert.*;
import java.io.IOException;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class NtLoaderTest {

	@Test
	public void testValidInput() throws Exception {
		MockRecordReader reader = new MockRecordReader("src/test/resources/test.nt");

		NtLoader custLoader = new NtLoader();

		custLoader.prepareToRead(reader, null);

		/**
		 * read first line
		 */
		Tuple line1 = custLoader.getNext();

		assertNotNull(line1);
		String sub = line1.get(0).toString();
		assertNotNull(sub);
		assertEquals("<http://data.kasabi.com/dataset/chembl-rdf/molecule/m63632>", sub);
		String pred = line1.get(1).toString();
		assertEquals("<http://www.w3.org/2004/02/skos/core#exactMatch>", pred);
		String object = line1.get(2).toString();
		assertEquals("\"N Trip.les\"", object);
		
		assertEquals(3, line1.size());
		/**
		 * read second line
		 */
		Tuple line2 = custLoader.getNext();
		assertNotNull(line2);
		String object2 = line2.get(2).toString();
		assertEquals("\"N-Triples\"@en-US", object2);
		
		/**
		 * read third line
		 */
		Tuple line3 = custLoader.getNext();
		assertNotNull(line3);
		String object3 = line3.get(2).toString();
		assertEquals("\"N-Triples\"@en-US", object3);
		
		/**
		 * read fourth line
		 */
		Tuple line4 = custLoader.getNext();
		assertNotNull(line4);
		String object4 = line4.get(2).toString();
		assertEquals("<http://purl.obolibrary.org/obo/CHEBI_16643>", object4);
		
		/**
		 * read fifth line
		 */
		Tuple line5 = custLoader.getNext();
		assertNotNull(line5);
		String object5 = line5.get(2).toString();
		assertEquals("<http://purl.obolibrary.org/obo/CHEBI_35812>", object5);

	}

	@Test(expected = IOException.class)
	public void testInvalidInput() throws Exception {
		MockRecordReader reader = new MockRecordReader("src/test/resources/valid1line_hit_data.tsv");

		NtLoader custLoader = new NtLoader();

		custLoader.prepareToRead(reader, null);

		@SuppressWarnings("unused")
		Tuple tuple = custLoader.getNext();
	}

}