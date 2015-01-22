package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.lang.String;

public class GeoIPLookupUDFTest extends TestCase {


    /* All tests stored in a YAML file */

    private void runGeoIPLookupTest(String test_file, String data_file) {
        Text result;
        Yaml yaml_h = new Yaml();
        try {
            InputStream input = new FileInputStream(new File(test_file));
            List<Map<String, Object>> test_cases = (List<Map<String, Object>>) yaml_h.load(input);
            Iterator<Map<String, Object>> tc_it = test_cases.iterator();
            while(tc_it.hasNext()) {
                Map<String, Object> elem = tc_it.next();
                System.out.println("Testing " + elem.get("description"));
                result = new GeoIPLookupUDF().evaluate(
                        new Text((String)elem.get("address")),
                        new Text((String)elem.get("field")), new Text(data_file));
                System.out.println("  Expected " + elem.get("result") + " got " + result);
                if (result == null) {
                    assertEquals(elem.get("result"), "NULL");
                }
                else {
                    assertEquals(elem.get("result"), result.toString());
                }
            }
        }
        catch (FileNotFoundException e) {
            System.out.println("Test case file not found!");
            assertTrue( false );
        }
        catch (Exception e) {
            System.out.println("Test failed by exception! " + e );
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.out.println(sw.toString());
            assertTrue( false );
        }
    }

    public void testGeoIPLookup() {
        runGeoIPLookupTest("src/test/resources/ip-test-cases.yaml", "src/test/resources/GeoLite2-City.mmdb");
    }
}
