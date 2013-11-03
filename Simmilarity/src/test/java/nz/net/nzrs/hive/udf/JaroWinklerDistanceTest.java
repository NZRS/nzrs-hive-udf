package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JaroWinklerDistanceTest extends TestCase {
    final private Text text1 = new Text("home");
    final private Text text2 = new Text("homer");
    final private Text text3 = new Text("arthur");
    final private double epsilon = 0.0001;

    public void test1() {
        final double res = new JaroWinklerDistance().evaluate(text1, text2);
        final double expected = 0.96;
        assertEquals(expected, res, epsilon);
    }

    public void test2() {
        final double res = new JaroWinklerDistance().evaluate(text2, text3);
        final double expected = 0.0;
        assertEquals(expected, res, epsilon);
    }

    public void test3() {
        final double res = new JaroWinklerDistance().evaluate(text3, text3);
        final double expected = 1.0;
        assertEquals(expected, res, epsilon);
    }
}
