package nz.net.nzrs.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class isValidTldTest extends TestCase
{
    private boolean result;
    private String tldlist = "src/test/resources/tld-list.txt";
    private String badtldlist = "src/test/resources/tld-list.dat";

    public void testValidTld()
    {
        try {
            result = (new isValidTld().evaluate("nz", tldlist));
        }
        catch(HiveException e) {
            System.out.println("Test failed!");
            assertTrue( false );  // Force the test to fail
        }
        assertEquals(result, true);
    }

    public void testValidMixedCaseTld()
    {
        try {
            result = (new isValidTld().evaluate("Aero", tldlist));
        }
        catch(HiveException e) {
            System.out.println("Test failed!");
            assertTrue( false );  // Force the test to fail
        }
        assertEquals(result, true);
    }

    public void testValidUpperCaseTld()
    {
        try {
            result = (new isValidTld().evaluate("NZ", tldlist));
        }
        catch(HiveException e) {
            System.out.println("Test failed!");
            assertTrue( false );  // Force the test to fail
        }
        assertEquals(result, true);
    }

    public void testInvalidTld()
    {
        try {
            result = (new isValidTld().evaluate("local", tldlist));
        }
        catch(HiveException e) {
            System.out.println("Test failed!");
            assertTrue( false );  // Force the test to fail
        }
        assertEquals(result, false);
    }

    public void testSkipComments()
    {
        try {
            result = (new isValidTld().evaluate("# x", tldlist));
        }
        catch(HiveException e) {
            System.out.println("Test failed!");
            assertTrue( false );  // Force the test to fail
        }
        assertEquals(result, false);
    }

    public void testNonExistentFile()
    {
        try {
            result = (new isValidTld().evaluate("local", badtldlist));
        }
        catch(HiveException e) {
            assertEquals(e.getMessage().substring(0,12), "FILENOTFOUND");
        }
    }

}

