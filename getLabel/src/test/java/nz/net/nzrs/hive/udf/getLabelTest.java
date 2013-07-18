package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for getLabel
 */
public class getLabelTest 
    extends TestCase
{
    final private Text domain = new Text();
    final private Text domain1 = new Text("nzrs.net.nz");
    final private Text label  = new Text();

    final private Text label0 = new Text("0");
    final private Text label0_1 = new Text("0,1");
    final private Text label1_0 = new Text("1,0");
    final private Text label2_1_0 = new Text("2,1,0");
    final private Text label2_1 = new Text("2,1");
    final private Text label_inv_pos = new Text("3");
    final private Text inv_label = new Text("3.14");
    

    /**
     * Test for a normal label
     */
    public void testNormalLabel()
    {
        domain.set("nzrs.net.nz");
        label.set("0");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals("nz", result.toString());
    }

    /**
     * Two labels
     */
    public void testTwoLabels()
    {
        domain.set("nzrs.net.nz");
        label.set("1,0");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals("net.nz", result.toString());
    }

    /**
     * Labels in reverse order
     */
    public void testReverseTwoLabels()
    {
        domain.set("nzrs.net.nz");
        label.set("0,1");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals("nz.net", result.toString());
    }

    public void testThreeLabels()
    {
        domain.set("nzrs.net.nz");
        label.set("2,1,0");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals("nzrs.net.nz", result.toString());
    }

    public void testPrefixLabel()
    {
        domain.set("nzrs.net.nz");
        label.set("2,1");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals("nzrs.net", result.toString());
    }

    public void testInvalidLabelPos()
    {
        domain.set("nzrs.net.nz");
        /* There is no position 3 in nzrs.net.nz */
        label.set("3");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals(null, result);
    }

    public void testInvalidLabel()
    {
        domain.set("nzrs.net.nz");
        /* 3.14 is not an integer */
        label.set("3.14");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals(null, result);
    }

    public void testMalformedDomain()
    {
        domain.set("nzrs..net.nz");
        /* 3.14 is not an integer */
        label.set("2,1");
        final Text result = new getLabel().evaluate(domain, label);
        assertEquals(".net", result.toString());
    }

}
