package lbms.plugins.mldht.kad;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.*;

import lbms.plugins.mldht.kad.utils.BitVector;

import static java.lang.Math.*;


public class BloomFilter implements Comparable<BloomFilter>, Cloneable {

	private final static int m = 256 * 8;
	private final static int k = 2; 

	
	MessageDigest sha1;
	BitVector filter;
	
	public BloomFilter() {
		filter = new BitVector(m);
		
		try
		{
			sha1 = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
	}
	
	public BloomFilter(byte[] serializedFilter) {
		filter = new BitVector(m,serializedFilter);
	}
	
    public void insert(InetAddress addr) {
        
        byte[] hash = sha1.digest(addr.getAddress());
        
        int index1 = (hash[0]&0xFF) | (hash[1]&0xFF) << 8;
        int index2 = (hash[2]&0xFF) | (hash[3]&0xFF) << 8;

        // truncate index to m (11 bits required)
        index1 %= m;
        index2 %= m;

        // set bits at index1 and index2
        filter.set(index1);
        filter.set(index2);
    }
	
	
	protected BloomFilter clone() {
		BloomFilter newFilter = null;
		try
		{
			newFilter = (BloomFilter) super.clone();
		} catch (CloneNotSupportedException e)
		{
			// never happens
		}
		newFilter.filter = new BitVector(filter);		
		return newFilter;		
	}
	
	public int compareTo(BloomFilter o) {
		return (int) (size()-o.size());
	}

	
	public int size() {
		// number of expected 0 bits = m * (1 − 1/m)^(k*size)

		double c = filter.bitcount();
		double size = log1p(-c/m) / (k * logB());
		return (int) size;
	}
	
	public static int unionSize(Collection<BloomFilter> filters)
	{
		BitVector[] vectors = new BitVector[filters.size()];
		int i = 0;
		for(BloomFilter f : filters)
			vectors[i++] = f.filter;
		
		double c = BitVector.unionAndCount(vectors);
		return (int) (log1p(-c/m) / (k * logB()));
	}
	
	public byte[] serialize() {
		return filter.getSerializedFormat();
	}

	
	// the logarithm of the base used for various calculations
	private static double logB() {
		return log1p(-1.0/m);
	}

	
	public static void main(String[] args) throws Exception {
		
		BloomFilter bf = new BloomFilter();
		//2001:DB8::
		for(int i=0;i<1000;i++)
		{
			bf.insert(InetAddress.getByAddress(new byte[] {0x20,0x01,0x0D,(byte) 0xB8,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,(byte) (i>>8 & 0xFF),(byte) (i & 0xFF)}));
		}
		
		for(int i=0;i<256;i++)
		{
			bf.insert(InetAddress.getByAddress(new byte[] {(byte) 192,0,2,(byte) i}));
		}
		
		System.out.println(bf.filter.toString());
		System.out.println(bf.filter.bitcount());
		System.out.println(bf.size());
	
	}

	
	
	
}
