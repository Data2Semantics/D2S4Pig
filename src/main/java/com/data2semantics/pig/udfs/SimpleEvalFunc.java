package com.data2semantics.pig.udfs;
/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
 
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public abstract class SimpleEvalFunc<T> extends EvalFunc<T>
{
  Method m = null;

  public SimpleEvalFunc()
  {
    for (Method method : this.getClass().getMethods()) {
      if (method.getName() == "call")
        m = method;
    }
    if (m == null)
      throw new IllegalArgumentException(String.format("%s: couldn't find call() method in UDF.", getClass().getName()));
  }

  // Pig can't get the return type via reflection (as getReturnType normally tries to do), so give it a hand 
  @Override
  public Type getReturnType() 
  {
    return m.getReturnType();
  }

  private String _method_signature() 
  {
    StringBuilder sb = new StringBuilder(getClass().getName());
    Class<?> pvec[] = m.getParameterTypes();

    sb.append("(");
    for (int i=0; i < pvec.length; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(String.format("%s", pvec[i].getName()));
    }
    sb.append(")");

    return sb.toString();
  }
 
  @Override
  @SuppressWarnings("unchecked")
  public T exec(Tuple input) throws IOException
  {
    Class pvec[] = m.getParameterTypes();

    if (input == null || input.size() == 0)
      return null;
    
    // check right number of arguments
    if (input.size() != pvec.length) 
      throw new IOException(String.format("%s: got %d arguments, expected %d.", _method_signature(), input.size(), pvec.length));

    // pull and check argument types
    Object[] args = new Object[input.size()];
    for (int i=0; i < pvec.length; i++) {
      Object o = input.get(i);
      try {
        o = pvec[i].cast(o);
      }
      catch (ClassCastException e) {
        throw new IOException(String.format("%s: argument type mismatch [#%d]; expected %s, got %s", _method_signature(), i+1,
              pvec[i].getName(), o.getClass().getName()));
      }
      args[i] = o;
    }

    try {
      return (T) m.invoke(this, args);
    }
    catch (Exception e) {
        throw new IOException(String.format("%s: caught exception processing input.", _method_signature()), e);
    }
  }

  /**
   * Override outputSchema so we can verify the input schema at pig compile time, instead of runtime
   * @param inputSchema input schema
   * @return call to super.outputSchema in case schema was defined elsewhere
   */
  @Override
  public Schema outputSchema(Schema inputSchema)
  {
    if (inputSchema == null) {
      throw new IllegalArgumentException(String.format("%s: null schema passed to %s", _method_signature(), getClass().getName()));
    }

    // check correct number of arguments
    Class parameterTypes[] = m.getParameterTypes();
    if (inputSchema.size() != parameterTypes.length) {
      throw new IllegalArgumentException(String.format("%s: got %d arguments, expected %d.",
                                                       _method_signature(),
                                                       inputSchema.size(),
                                                       parameterTypes.length));
    }

    // check type for each argument
    for (int i=0; i < parameterTypes.length; i++) {
      try {
        byte inputType = inputSchema.getField(i).type;
        byte parameterType = DataType.findType(parameterTypes[i]);
        if (inputType != parameterType) {
          throw new IllegalArgumentException(String.format("%s: argument type mismatch [#%d]; expected %s, got %s",
                                                           _method_signature(),
                                                           i+1,
                                                           DataType.findTypeName(parameterType),
                                                           DataType.findTypeName(inputType)));
        }
      }
      catch (FrontendException fe) {
        throw new IllegalArgumentException(String.format("%s: Problem with input schema: ", _method_signature(), inputSchema), fe);
      }
    }

    // delegate to super to determine the actual outputSchema (if specified)
    return super.outputSchema(inputSchema);
  }
}
