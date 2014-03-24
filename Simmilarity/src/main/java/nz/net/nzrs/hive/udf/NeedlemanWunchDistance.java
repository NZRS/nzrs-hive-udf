package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import uk.ac.shef.wit.simmetrics.similaritymetrics.*;

import org.apache.hadoop.io.Text;

public final class NeedlemanWunchDistance extends UDF {
    private AbstractStringMetric metric;

    public NeedlemanWunchDistance() {
        metric = new NeedlemanWunch();
    }

    public float evaluate(final Text a, final Text b) {
        return metric.getSimilarity(a.toString(), b.toString());
    }
}

