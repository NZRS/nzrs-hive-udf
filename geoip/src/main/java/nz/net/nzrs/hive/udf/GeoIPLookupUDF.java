package nz.net.nzrs.hive.udf;

import com.maxmind.geoip.*;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.Description;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.*;

/**
 * GeoIPLookupUDF is a Hive User Defined Function that allows you to lookup
 * database information on a given ip.
 * argument 0 should be an IP string
 * argument 1 should be one of the following values:
 * COUNTRY_NAME, COUNTRY_CODE, CITY, LATITUDE, LONGITUDE, REGION
 * argument 2 should be the filename for you geo-ip database
 *
 * NOTE: This version implements the UDF interface, rather than the
 * GenericUDF interface
 * 
 * <pre>
 * Usage:
 * add file GeoIP.dat;
 * add jar hive-udf-geo-ip.jar;
 * create temporary function geoip as 'nz.net.nzrs.hive.udf.GeoIPLookupUDF';
 * select geoip(first, 'COUNTRY_NAME',  './GeoIP.dat' ) from a;
 * </pre>
 * @author secastro
 */

@Description(
  name = "geoip",
  value = "_FUNC_(ip,property,database) - loads database into GEO-IP lookup "+
  "service, then looks up 'property' of ip. "
)

public final class GeoIPLookupUDF extends UDF {

  private String ipString = null;
  private Long ipLong = null;
  private String property;
  private LookupService ls;
  private String lookupVal = null;

  private static final String COUNTRY_NAME = "COUNTRY_NAME";
  private static final String COUNTRY_CODE = "COUNTRY_CODE";
  private static final String AREA_CODE    = "AREA_CODE";
  private static final String CITY         = "CITY";
  private static final String DMA_CODE     = "DMA_CODE";
  private static final String LATITUDE     = "LATITUDE";
  private static final String LONGITUDE    = "LONGITUDE";
  private static final String METRO_CODE   = "METRO_CODE";
  private static final String POSTAL_CODE  = "POSTAL_CODE";
  private static final String REGION       = "REGION";
  private static final String ORG          = "ORG";
  private static final String ID           = "ID";

    public GeoIPLookupUDF() {
        ls = null;
    }

    public Text evaluate(final Text ip, final Text field, final Text datafile) throws HiveException {
        if (ip == null) { 
            return new Text("NOIP");
        }
        else if (field == null) {
            return new Text("NOFIELD");
        }
        else if (datafile == null) {
            return new Text("NODATAFILE");
        }
        try {
            if (ls == null) {
                ls = new LookupService(datafile.toString(), LookupService.GEOIP_MEMORY_CACHE);
            }
            Location l = ls.getLocation(ip.toString());
            if (l == null) {
                return null;
            }
            lookupVal = field.toString();
            if (lookupVal.equals(COUNTRY_NAME)) {
                return new Text(l.countryName);
            }
            else if (lookupVal.equals(COUNTRY_CODE)) {
                return new Text(l.countryCode);
            }
            else if (lookupVal.equals(REGION)) {
                if (l.region == null) {
                    return null;
                } else {
                    return new Text(l.region);
                }
            }
            else if (lookupVal.equals(CITY)) {
                if (l.city == null) {
                    return null;
                } else {
                    return new Text(l.city);
                }
            }
            else {
                throw new HiveException("INVALID FIELD");
            }
        }
        catch (IOException e) {
            throw new HiveException(e);
        }
    }

}
