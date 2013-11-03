package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import uk.ac.shef.wit.simmetrics.similaritymetrics.*;

import org.apache.hadoop.io.Text;

public final class MongeElkanDistance extends UDF {
    private AbstractStringMetric metric;

    public MongeElkanDistance() {
        metric = new MongeElkan();
    }

    public float evaluate(final Text a, final Text b) {
        return metric.getSimilarity(a.toString(), b.toString());
    }
}

