package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class calcRank extends UDF
{
    private int counter;
    private Text last_key;

    public int evaluate(final Text key) {
        if ( !key.equals(this.last_key) ) {
            this.counter = 0;
            this.last_key = key;
        }
        return ++this.counter;
    }
}
