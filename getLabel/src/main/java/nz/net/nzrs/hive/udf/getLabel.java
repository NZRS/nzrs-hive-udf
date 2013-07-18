package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class getLabel extends UDF {
    private String[] labels;
    private String[] indexes_s;
    private String[] result;
    private int idx;
    private StringBuilder o;

    // Constructor
    public getLabel() {
        // This string is 255 characters long because it the maximum
        // length of a query name in the DNS
        o = new StringBuilder(255);
    }


    public Text evaluate(final Text s, final Text range) {
        if (s == null) { return null; }
        if (range == null) { return null; }
        // range can be the following
        // one number that will point the index of the string we want.
        // 1 represents the TLD
        // 2 represents the SLD and so on
        // a sequence of numbers, like "3,2,1"
        try {
            labels = s.toString().split("\\.");
            indexes_s = range.toString().split("\\,");
            result = new String[ indexes_s.length ];
            for(int i=0; i<indexes_s.length; i++) {
                idx = Integer.valueOf(indexes_s[i], 10).intValue();
                result[i] = labels[ labels.length - 1 - idx ];
            }
        } catch(Exception ex) {
            return null;
        }
        // Validate range
        // there are only numbers
        // the numbers are valid indexes
        // Wipe contents of o
        o.setLength(0);
        for(int i=0; i < indexes_s.length-1; i++) {
            o.append(result[i]);
            o.append(".");
        }
        o.append(result[ indexes_s.length-1 ]);
        return new Text(o.toString().toLowerCase());
    }
}
