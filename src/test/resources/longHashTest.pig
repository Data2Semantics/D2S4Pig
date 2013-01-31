register $JAR_PATH

define LONGHASH com.data2semantics.pig.udfs.LongHash();

data_in = LOAD 'input' as (val:chararray);

data_out = FOREACH data_in GENERATE LONGHASH(val) as val;

STORE data_out INTO 'output';