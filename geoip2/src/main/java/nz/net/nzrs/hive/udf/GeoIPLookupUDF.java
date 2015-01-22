package nz.net.nzrs.hive.udf;

import com.maxmind.geoip2.*;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

import java.net.InetAddress;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.*;

/**
 * GeoIPLookupUDF is a Hive User Defined Function that allows you to lookup
 * database information on a given ip.
 * argument 0 should be an IP string
 * argument 1 should be one of the following values:
 * COUNTRY_NAME, COUNTRY_CODE, CITY
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
  name = "geoip2",
  value = "_FUNC_(ip,property,database) - loads database into GEO-IP lookup "+
  "service, then looks up 'property' of ip. "
)

public final class GeoIPLookupUDF extends UDF {

    private File database;
    private DatabaseReader reader;
    private String lookupVal = null;
    private InetAddress ipaddr = null;

    private static final String COUNTRY_NAME = "COUNTRY_NAME";
    private static final String COUNTRY_CODE = "COUNTRY_CODE";
    private static final String CITY = "CITY";
    private static final String ASN = "ASN";
    private static final String ORG = "ORG";

    public GeoIPLookupUDF() {
        database = null;
        reader = null;
    }

    public Text evaluate(final Text ip, final Text field, final Text datafile) throws HiveException {
        if (ip == null) {
            return new Text("NOIP");
        } else if (field == null) {
            return new Text("NOFIELD");
        } else if (datafile == null) {
            return new Text("NODATAFILE");
        }
        try {
            if (database == null) {
                database = new File(datafile.toString());
                reader = new DatabaseReader.Builder(database).build();
            }
            lookupVal = field.toString();
            ipaddr = InetAddress.getByName(ip.toString());
            if (lookupVal.equals(ORG) || lookupVal.equals(ASN)) {
                IspResponse response = reader.isp(ipaddr);

                if (lookupVal.equals(ASN)) {
                    return new Text(response.getAutonomousSystemNumber().toString());
                } else {
                    return new Text(response.getOrganization());
                }
            } else if (lookupVal.equals(COUNTRY_CODE) || lookupVal.equals(COUNTRY_NAME) || lookupVal.equals(CITY)) {
                CityResponse response = reader.city(ipaddr);
                if (lookupVal.equals(COUNTRY_NAME)) {
                    Country country = response.getCountry();
                    if (country.getName() == null) {
                        return null;
                    } else {
                        return new Text(country.getName());
                    }
                } else if (lookupVal.equals(COUNTRY_CODE)) {
                    Country country = response.getCountry();
                    if (country.getIsoCode() == null) {
                        return null;
                    } else {
                        return new Text(country.getIsoCode());
                    }
                } else if (lookupVal.equals(CITY)) {
                    City city = response.getCity();
                    if (city.getName() == null) {
                        return null;
                    } else {
                        return new Text(city.getName());
                    }
                }
            } else {
                throw new HiveException("INVALID FIELD");
            }
        } catch (IOException e) {
            throw new HiveException(e);
        } catch (AddressNotFoundException e) {
            return null;
        } catch (GeoIp2Exception e) {
            throw new HiveException(e);
        }

        return null;
    }
}
