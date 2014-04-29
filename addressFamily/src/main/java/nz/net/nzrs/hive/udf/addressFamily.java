package nz.net.nzrs.hive.udf;

import com.googlecode.ipv6.IPv6Address;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/*
 * addressFamily
 * Given a string that represents a network address, will return 4 if
 * it's an IPv4 address, 6 if it's IPv6 address, 0 otherwise
 *
 */

public final class addressFamily extends UDF
{
    private String[] address;
    IPv6Address v6addr;
    InetAddressValidator v4validator;
    final IntWritable v4rv = new IntWritable(4);
    final IntWritable v6rv = new IntWritable(6);
    final IntWritable error_rv = new IntWritable(0);

    // Constructor
    public addressFamily() {
        v4validator = InetAddressValidator.getInstance();
    }

    public IntWritable evaluate(final Text a) { 
        if (a == null) { return error_rv; }
        try {
            // Is this a v4 address?
            if (v4validator.isValid(a.toString())) {
                return v4rv;
            }
            else {
                // Try as v6 address
                v6addr = IPv6Address.fromString(a.toString());
                // If it's not valid, no exception will be produced
                return v6rv;
            }
        } catch(Exception ex) {
            return error_rv;
        }
    }
}
