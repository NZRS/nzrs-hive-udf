package nz.net.nzrs.hive.udf;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.lang.String;

/**
 * Unit test for addressFamily
 */
public class addressFamilyTest 
    extends TestCase
{
    
    /**
     * All tests are described in a YAML file, to avoid have to
     * recompile
     */
    public void testV4addressFamily()
    {
        IntWritable result = new IntWritable(0);
        Yaml yaml_h = new Yaml();
        try {
            InputStream input = new FileInputStream(new
                File("src/test/resources/v4-test-cases.yaml"));
            List<Map<String, Object>> test_cases = (List<Map<String,
            Object>>) yaml_h.load(input);
            Iterator<Map<String, Object>> tc_it = test_cases.iterator();
            while(tc_it.hasNext()) {
                Map<String, Object> elem = tc_it.next();
                System.out.println("Testing " + (String)elem.get("description"));
                result = (new addressFamily().evaluate(
                            new Text((String)elem.get("address"))));
                System.out.println("  Expected " + elem.get("result") + " got " + result);
                assertEquals(elem.get("result"), result.toString());
            }
        }
        catch(FileNotFoundException e) {
            System.out.println("Test case file not found!");
            assertTrue( false );  // Will cause test to fail
        }
        catch(Exception e) {
            System.out.println("** Caught exception " + e.getMessage());
            assertTrue( false );  // Will cause test to fail
        }
    }
}
