package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MongeElkanDistanceTest extends TestCase {
    final private Text text1 = new Text("home");
    final private Text text2 = new Text("casa");
    final private double epsilon = 0.0001;

    public void test1() {
        final double res = new MongeElkanDistance().evaluate(text1, text2);
        final double expected = 0.15;
        assertEquals(expected, res, epsilon);
    }
}
