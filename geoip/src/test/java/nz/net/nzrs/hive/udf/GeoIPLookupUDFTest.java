package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.lang.String;

public class GeoIPLookupUDFTest extends TestCase {

    final private Text geoip_file = new Text("src/test/resources/GeoIPCity.dat");

    /* All tests stored in a YAML file */

    public void testGeoIPLookupUDF() {
        Text result = null;
        Yaml yaml_h = new Yaml();
        try {
            InputStream input = new FileInputStream(new
                File("src/test/resources/test-cases.yaml"));
            List<Map<String, Object>> test_cases = (List<Map<String,
            Object>>) yaml_h.load(input);
            Iterator<Map<String, Object>> tc_it = test_cases.iterator();
            while(tc_it.hasNext()) {
                Map<String, Object> elem = tc_it.next();
                System.out.println("Testing " + (String)elem.get("description"));
                result = new GeoIPLookupUDF().evaluate(
                        new Text((String)elem.get("address")),
                        new Text((String)elem.get("field")), geoip_file);
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
            System.out.println("Test failed by exception! " + e.getMessage() );
            assertTrue( false );
        }
    }
}
