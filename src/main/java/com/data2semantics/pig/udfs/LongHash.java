package com.data2semantics.pig.udfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>long HashFNV(String string_to_hash, [int mod])</code>.</dd>
* </dl>
*/

public class LongHash extends EvalFunc<Long> {
    static final long FNV1_64_INIT = 0x811c9dc5;
    static final long FNV_64_PRIME = 1099511628211L;
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
        } catch (Exception e) {
          return null;
        }
    }

    public Long exec(Tuple input) throws IOException {
        if (input.size()!=1) {
            String msg = "HashFNV : Only 1 parameters are allowed.";
            throw new IOException(msg);
        }
        if (input.get(0)==null)
            return null;

        long v = hashFnv64((String)input.get(0));
        if (v < 0)
            v = -v;
        return v;
    }
    long hashFnv64Init(long init, String s)
    {
        long hval = init;

        byte[] bytes = null;
        try {
            bytes = s.getBytes("UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            // shall not happen
        }
        for (int i=0;i<bytes.length;i++)
        {
            /* multiply by the 64 bit FNV magic prime mod 2^64 */
            hval *= FNV_64_PRIME;
            hval ^= bytes[i];
        }
        return hval;
    }

    long hashFnv64(String s)
    {
        return hashFnv64Init(FNV1_64_INIT, s);
    }
    
}
