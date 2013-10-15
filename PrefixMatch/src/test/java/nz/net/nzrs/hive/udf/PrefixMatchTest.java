package nz.net.nzrs.hive.udf;

import org.apache.hadoop.io.Text;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.List;
import java.util.Iterator;
import java.util.Map;


public class PrefixMatchTest extends TestCase {
    final private Text v4_addr_1 = new Text("192.168.2.129");
    final private Text v4_addr_2 = new Text("0.1.2.3");
    final private Text v4_addr_bad = new Text("256.0.0.1");

    final private Text v4_mask_1 = new Text("192.168.2.0/24");
    final private Text v4_mask_2 = new Text("192.168.2.0/25");
    final private Text v4_default = new Text("0.0.0.0/0");
    final private Text v4_wrong_mask = new Text("0.0.0.0/33");
    final private Text v4_host_mask = new Text("192.168.2.129/32");
    final private Text v4_bad_mask = new Text("0.0.0.0");

    public void testV4Match() {
        final boolean result = new PrefixMatch().evaluate(v4_addr_1, v4_mask_1);
        assertEquals(true, result);
    }

    public void testV4NotMatch() {
        final boolean result = new PrefixMatch().evaluate(v4_addr_1, v4_mask_2);
        assertEquals(false, result);
    }

    public void testV4BadAddress() {
        final boolean result = new PrefixMatch().evaluate(v4_addr_bad, v4_mask_1);
        assertEquals(false, result);
    }

    public void testV4BadMask() {
        final boolean result = new PrefixMatch().evaluate(v4_addr_1, v4_bad_mask);
        assertEquals(false, result);
    }

    public void testV4HostMask() {
        final boolean result = new PrefixMatch().evaluate(v4_addr_1,
        v4_host_mask);
        assertEquals(true, result);
    }

    public void testV4Default() {
        final boolean result = new PrefixMatch().evaluate(v4_addr_1,
        v4_default);
        assertEquals(true, result);
    }
    
    public void testV6FromYaml() {
        Boolean result = new Boolean(false);
        Yaml yaml_h = new Yaml();
        try {
            InputStream input = new FileInputStream(new
                File("src/test/resources/v6-test-cases.yaml"));
            List<Map<String, Object>> test_cases = (List<Map<String,
            Object>>) yaml_h.load(input);
            Iterator<Map<String, Object>> tc_it = test_cases.iterator();
            while(tc_it.hasNext()) {
                Map<String, Object> elem = tc_it.next();
                System.out.println("Testing " + (String)elem.get("description"));
                result = Boolean.valueOf(new PrefixMatch().evaluate(
                                new Text((String)elem.get("address")),
                                new Text((String)elem.get("prefix"))));
                assertEquals((Boolean)elem.get("result"), result);
            }
        }
        catch(FileNotFoundException e) {
            System.out.println("Test case file not found!");
        }
    }
}
