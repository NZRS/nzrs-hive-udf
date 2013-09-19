
package nz.net.nzrs.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * GenericUDFDelta
 *
 */
@Description(name = "delta",
	    value = "_FUNC_(hash_key) - Returns the difference between two"
        + " consecutives values group by hash_key",
	    extended = "Example:\n"
	    + "  > SELECT _FUNC_(HASH(p1, p2), col_value) FROM (\n"
	    + "  > 		SELECT order_by_col1 FROM table \n"
	    + "  >      DISTRIBUTE BY HASH(p1,p2)\n"
	    + "  >      SORT BY p1,p2, order_by_col1 \n"
	    + "  > );\n\n"
	    )

@UDFType(deterministic = false, stateful = true)
public class GenericUDFDelta extends GenericUDF {
  private final LongWritable longPrevValue = new LongWritable();
  private final LongWritable longDelta = new LongWritable();
  private final DoubleWritable doublePrevValue = new DoubleWritable();
  private final DoubleWritable doubleDelta = new DoubleWritable();
  private ObjectInspector prevHashKeyOI, deltaOI, hashOI, valueOI;
  private Object prevHashKey;
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length!=2) {
        throw new UDFArgumentException("The function DELTA accepts 2 arguments.");
    }
    // Verify the arguments are primitive types
    
    for(int i=0; i < arguments.length; i++) {
        if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but " + arguments[i].getTypeName() + " is passed.");
        }
    }

    // Verify the second object provided is of numeric type, so the
    // "difference" operator is defined
    String arg_type = arguments[1].getTypeName();
    if (arg_type.equals(Constants.TINYINT_TYPE_NAME) ||
        arg_type.equals(Constants.SMALLINT_TYPE_NAME) ||
        arg_type.equals(Constants.INT_TYPE_NAME) ||
        arg_type.equals(Constants.BIGINT_TYPE_NAME) )
    {
        deltaOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if ( arg_type.equals(Constants.FLOAT_TYPE_NAME) ||
                arg_type.equals(Constants.DOUBLE_TYPE_NAME) ||
                arg_type.equals(Constants.DECIMAL_TYPE_NAME) )
    {
        deltaOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else {
        throw new UDFArgumentTypeException(1, "Only numeric type arguments are accepted but " + arguments[1].getTypeName() + " is provided.");
    }
    
    hashOI = arguments[0];
    valueOI = arguments[1];

    prevHashKeyOI = ObjectInspectorUtils.getStandardObjectInspector(hashOI, ObjectInspectorCopyOption.JAVA);

    return deltaOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
	  Object hash  = arguments[0].get();
	  Object value = arguments[1].get();
        boolean doDelta = true;
        int returnValue = 0;

	  if ( prevHashKey==null || ObjectInspectorUtils.compare(prevHashKey,prevHashKeyOI,hash,hashOI)!=0) {
          // the prevHashKey is not set, or it's different from current
          // value, can't calculate the delta
            // XXX: Return value is null
        doDelta = false;
        returnValue = 0;
	  }

        // Calculate the delta using the proper type
        Converter converter = ObjectInspectorConverters.getConverter(valueOI, deltaOI);
        if (deltaOI.getTypeName() == Constants.DOUBLE_TYPE_NAME) {
            DoubleWritable dwvalue = (DoubleWritable)converter.convert(value);
            if (doDelta) {
                doubleDelta.set( dwvalue.get() - doublePrevValue.get() );
                returnValue = 1;
            }
            // Overwrite the previous value
            doublePrevValue.set( dwvalue.get() );
        }
        else {      // Long Writable
            LongWritable lwvalue = (LongWritable)converter.convert(value);
            if (doDelta) {
                longDelta.set( lwvalue.get() - longPrevValue.get() );
                returnValue = 2;
            }
            // Overwrite the previous value
            longPrevValue.set( lwvalue.get() );
        }
        

        // Update the hash key
        prevHashKey=ObjectInspectorUtils.copyToStandardObject(hash, hashOI, ObjectInspectorCopyOption.JAVA);

        // Give the result back
        if (returnValue == 1) {
            return doubleDelta;
        }
        else if (returnValue == 2) {
            return longDelta;
        }
        else {
            return null;
        }

  }

  @Override
  public String getDisplayString(String[] children) {
	  return "delta(" + StringUtils.join(children, ',') + ")";
  }
}
