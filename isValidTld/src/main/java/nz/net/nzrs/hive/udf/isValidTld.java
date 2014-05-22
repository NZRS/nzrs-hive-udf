package nz.net.nzrs.hive.udf;

/* isValidTld
 * Given a string, it will look-up in a internal list of valid string,
 * and return true if the string is on that list, false otherwise
 *
 */

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Description(
    name="isValidTld",
    value="returns true if the string provided is within the list of "+
    "known strings read from the configuration file, false otherwise"
)

public final class isValidTld extends UDF
{
    private Map<String, Boolean> validTld;

    public Boolean evaluate(String tld, String tldFile) throws HiveException {
        if (validTld == null) {
            validTld = new HashMap<String, Boolean>();
            try {
                BufferedReader lineReader = new BufferedReader(new
FileReader(tldFile));

                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    if (!line.substring(0,1).equals("#")) {
                        validTld.put(line.toLowerCase(), true);
                    }
                }
            } catch (FileNotFoundException e) {
                throw new HiveException("FILENOTFOUND: "+ tldFile);
            } catch (IOException e) {
                throw new HiveException("BADFORMAT: " + tldFile);
            }
        }

        if (validTld.containsKey(tld.toLowerCase())) {
            return true;
        }

        return false;
    }
}
