package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public final class calcRank extends UDF
{
    private int counter;
    private int long_counter;
    private Text last_key;
    private LongWritable long_last_key;

    public int evaluate(final Text key) {
        if ( !key.equals(this.last_key) ) {
            this.counter = 0;
            this.last_key = key;
        }
        return ++this.counter;
    }

    public int evaluate(final LongWritable key) {
        if ( !key.equals(this.long_last_key) ) {
            this.long_counter = 0;
            this.long_last_key = key;
        }
        return ++this.counter;
    }
}
