package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class calcRankTest extends TestCase {
    final private Text key1 = new Text("key1");
    final private Text key2 = new Text("key2");
    final private calcRank calc_rank= new calcRank();

    /*
     * Simple, one value test
     */
    public void testSingleValue()
    {
        final int rank = calc_rank.evaluate(key1);

        assertEquals(1, rank);
    }

    public void testDoubleValue()
    {
        int rank = calc_rank.evaluate(key1);
        rank = calc_rank.evaluate(key1);

        assertEquals(2, rank);
    }

    public void testNewValue()
    {
        final int rank = calc_rank.evaluate(key2);

        assertEquals(1, rank);
    }

    public void testSecondNewValue()
    {
        final int rank = calc_rank.evaluate(key2);

        assertEquals(1, rank);
    }

}
