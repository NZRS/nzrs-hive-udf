package nz.net.nzrs.hive.udf;

import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.lang.Byte;
import java.lang.Long;
import java.math.BigInteger;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;

import org.apache.hadoop.io.Text;

/*
@Description(name = "PrefixMatch", value = "_FUNC_(address, prefix) - " +
  "Returns true or false if the address is covered by the prefix")
*/
public final class PrefixMatch extends UDF {
    private String[] p_elem;
    private int mask_bits;
    private InetAddress address, network;
    byte[] addr_oct, net_oct;

    public PrefixMatch() {

    }

    public boolean evaluate(final Text a, final Text p) {
        if ( (a == null) || (p == null) ) { return false; }
        String[] p_elem = p.toString().split("/");
        if (p_elem.length != 2) { return false; }

        mask_bits = Integer.parseInt(p_elem[1]);


        try {
            address = InetAddress.getByName(a.toString());
            network = InetAddress.getByName(p_elem[0].toString());
            addr_oct  = address.getAddress();
            net_oct   = network.getAddress();

            // The address and network provided are not of the same
            // family
            if (addr_oct.length != net_oct.length) {
                throw new Exception();
            }

            if ( net_oct.length == 4) { // IPv4 
                // Validate mask is between 0 and 32
                if ( (mask_bits < 0) || (mask_bits > 32) ) {
                    throw new Exception();
                }
                int int_network = getIntFromAddress(network);
                int mask = getIntFromAddress(InetAddress.getByName("255.255.255.255"));
                for(int i = 0; i < 32 - mask_bits; i++)
                    mask <<= 1;
                int int_addr = getIntFromAddress(address);
                if ((int_addr & mask) == int_network) { return true; }
            }
            else if ( net_oct.length == 16 ) { // IPv6 
                if ( (mask_bits < 0) || (mask_bits > 128) ) {
                    throw new Exception();
                }
                BigInteger int_network = new BigInteger(net_oct);
                BigInteger int_addr = new BigInteger(addr_oct);
                BigInteger mask = new BigInteger("ffffffffffffffffffffffffffffffff", 16);
                for(int i = 0; i < 128 - mask_bits; i++) {
                    mask = mask.clearBit(i);
                }
                return compareBigIntegerAsLong(int_network,
                                    int_addr.and(mask));
            }
            else {
                return false;
            }
        }
        catch (UnknownHostException e) {
            return false;
        }
        catch (Exception e) {
            return false;
        }

        return false;
    }

    private static int getIntFromAddress(InetAddress in) {
        return ByteBuffer.wrap(in.getAddress()).getInt();
    }

    private static boolean compareBigIntegerAsLong(BigInteger a, BigInteger b) {
        boolean cmp = true;
        // Compare the lower bits
        cmp = a.shiftRight(64).longValue() == b.shiftRight(64).longValue();
        cmp &= a.longValue() == b.longValue();

        return cmp;
    }

}

