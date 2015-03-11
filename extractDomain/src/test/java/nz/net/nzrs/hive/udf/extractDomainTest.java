package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

import static java.lang.System.*;

/**
 * Unit test for extractDomain
 */
public class extractDomainTest
    extends TestCase
{
    final private Text domain = new Text();

    /**
     * Test for a normal label
     */
    public void testThirdLevelHost()
    {
        domain.set("www.nzrs.net.nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals("nzrs.net.nz", result.toString());
    }

    /**
     * Two labels
     */
    public void testSecondLevelHost()
    {
        domain.set("www.nzrs.nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals("nzrs.nz", result.toString());
    }

    /**
     * Labels in reverse order
     */
    public void testThirdLevelDomain()
    {
        domain.set("nzrs.net.nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals("nzrs.net.nz", result.toString());
    }

    public void testSecondLevelDomain()
    {
        domain.set("nzrs.nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals("nzrs.nz", result.toString());
    }

    public void testValidSecondLevelDomain()
    {
        domain.set("net.nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals("net.nz", result.toString());
    }

    public void testFirstLevelDomain()
    {
        domain.set("nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals("nz", result.toString());
    }

    public void testMalformedDomain()
    {
        domain.set("nzrs..net.nz");
        final Text result = new extractDomain().evaluate(domain);
        out.println("Parameter = " + domain.toString() + "; Result = " + result.toString());
        assertEquals(".net.nz", result.toString());
    }
}
