package com.data2semantics.test.pig.udfs;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class UdfTests extends PigTests {
	
	@Test
	public void LongHashTest() throws Exception {
		PigTest test = createPigTest(PigTests.RESOURCE_DIR + "longHashTest.pig");
		writeLinesToFile("input", "ladsljkasdglk", "lkadsljasgjskdjks", "aladlasdgjks");
		test.runScript();
		assertOutput(test, "data_out", "(2965709144312880472)", "(-6156827022215362651)",
				"(1610267363548260612)");
	}
	
	@Test
	public void NullToZeroTest() throws Exception {
		PigTest test = createPigTest(PigTests.RESOURCE_DIR + "nullToZeroTest.pig");
		writeLinesToFile("input", "111", "1.24");
		test.runScript();
		assertOutput(test, "data_out", "(2965709144312880472)", "(-6156827022215362651)",
				"(1610267363548260612)");
	}
}