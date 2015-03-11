package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class extractDomain extends UDF {
    private String[] labels;
    private int cut = 0;
    private StringBuilder o;
    private static final Set<String> SLD = new HashSet<String>(Arrays.asList(new String[]{"ac", "co", "net", "org", "govt", "parliament", "health", "iwi", "maori", "kiwi", "gen", "school", "cri", "mil", "geek", "xn--mori-qsa"}));

    // Constructor
    public extractDomain() {
        // This string is 255 characters long because it the maximum
        // length of a query name in the DNS
        o = new StringBuilder(255);
    }


    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        try {
            labels = s.toString().toLowerCase().split("\\.");

            if (labels.length < 2)
                return s;

            if (SLD.contains(labels[labels.length-2])) {
                cut=labels.length-3;
            } else {
                cut=labels.length-2;
            }

            if (cut < 0)
                return s;
        } catch(Exception ex) {
            return null;
        }
        // Validate range
        // there are only numbers
        // the numbers are valid indexes
        // Wipe contents of o
        o.setLength(0);
        o.append(labels[cut]);
        for(int i=cut+1; i < labels.length; i++) {
            o.append(".").append(labels[i]);
        }
        return new Text(o.toString().toLowerCase());
    }
}
