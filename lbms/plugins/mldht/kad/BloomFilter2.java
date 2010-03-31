package lbms.plugins.mldht.kad;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.*;

import static java.lang.Math.*;


public class BloomFilter2 implements Comparable<BloomFilter>, Cloneable {

	// bits, must be a power of 2
	private final int m; // = 1024 * 8;
	// expected entries
	private final int n; // = 2000;

	// number of hashes (bits to set per entry)
	private final int k; 
	private final int hashBits;
	
	MessageDigest sha1;
	BitSet filter = new BitSet();
	double entries = -1;
	

	
	
	public BloomFilter2(int m, int n) {
		this.m = m;
		this.n = n;
		k = (int) Math.max(1, Math.round(m * 1.0 / n * Math.log(2)));
		hashBits = (int) (Math.log(m)/Math.log(2)); 
		
		try
		{
			sha1 = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
	}
	
	
	public void insert(byte[] data) {
		BigInteger hash = new BigInteger(sha1.digest(data));
		sha1.reset();
		for(int i=0;i<k;i++)
		{
			BigInteger index = new BigInteger(0,new byte[0]);
			for(int j=0;j<hashBits;j++)
			{
				if(i*hashBits + j >= 160)
					System.out.println("error, bit index overflow");
				if(hash.testBit(i*hashBits + j))
					index = index.setBit(j);
			}
			filter.set(index.intValue());
		}
		
	}
	
	protected BloomFilter2 clone() {
		BloomFilter2 newFilter = null;
		try
		{
			newFilter = (BloomFilter2) super.clone();
		} catch (CloneNotSupportedException e)
		{
			// never happens
		}
		newFilter.filter = (BitSet) filter.clone();		
		return newFilter;		
	}
	
	public int compareTo(BloomFilter o) {
		return (int) (size()-o.size());
	}
	
	public double size() {
		return entries >= 0 ? entries : sizeEstimate();
	}
	
	public double sizeEstimate() {
		// number of expected 0 bits = m * (1 − 1/m)^(k*size)

		double c = filter.cardinality();
		double size = log1p(-c/m) / (k * logB());
		return size;
	}
	
	public double intersect2(BloomFilter2 other) {
		BitSet intersected = (BitSet) filter.clone();
		intersected.and(other.filter);
		
		double c = m - intersected.cardinality();
		double s1 = m - filter.cardinality();
		double s2 = m - other.filter.cardinality();
		
		double s = m;
		s /= s1 * s2;
		s *= s1 + s2 - c;
		
		
		s = log(s);
		s /= -k * log1p(-1.0/m);
		

		return  s;		
	}

	public static double intersect3(BloomFilter2 f1, BloomFilter2 f2, BloomFilter2 f3) {
		int m = f1.m;
		int k = f1.k;
		
		BitSet temp;
		
		temp = (BitSet) f1.filter.clone();
		temp.and(f2.filter);
		
		double i12 = m - temp.cardinality();
		
		temp.and(f3.filter);
		double i123 = m - temp.cardinality();
		
		temp = (BitSet) f2.filter.clone();
		temp.and(f3.filter);
		
		double i23 = m - temp.cardinality();
		
		
		
		temp = (BitSet) f1.filter.clone();
		temp.and(f3.filter);
		
		double i13 = m - temp.cardinality();
		double s1 = m - f1.filter.cardinality();
		double s2 = m - f2.filter.cardinality();
		double s3 = m - f3.filter.cardinality();
		
		double s = 1;
		s *= s1/m * s2/m * s3/m;
		
		
		s /=  (s1 + s2 + s3
			- (i12 + i23 + i13)
			+ i123) / m;
		
		//s= sqrt(s);
		
		double sb = s;
		
		s = log(s);
		
		s /= k * log1p(-1.0/m);
		
		
		System.out.println(
			nf.format(s1)+"\t"+
			nf.format(s2)+"\t"+
			nf.format(s3)+"\t"+
			nf.format(i12)+"\t"+
			nf.format(i13)+"\t"+
			nf.format(i23)+"\t"+
			nf.format(i123)+"\t"+
			k+"\t"+
			m+"\t"+
			nf.format(s)+"\t"+
			nf.format(sb)
		);
		

		return  s;		
	}
	
	
	public static double intersect4(BloomFilter2 f1, BloomFilter2 f2, BloomFilter2 f3) {
		int m = f1.m;
		int k = f1.k;
		
		BitSet temp;
		
		temp = (BitSet) f1.filter.clone();
		temp.and(f2.filter);
		
		double i12 = m - temp.cardinality();
		
		temp.and(f3.filter);
		double i123 = m - temp.cardinality();
		
		temp = (BitSet) f2.filter.clone();
		temp.and(f3.filter);
		
		double i23 = m - temp.cardinality();
		
		
		
		temp = (BitSet) f1.filter.clone();
		temp.and(f3.filter);
		
		double i13 = m - temp.cardinality();
		double s1 = m - f1.filter.cardinality();
		double s2 = m - f2.filter.cardinality();
		double s3 = m - f3.filter.cardinality();
		
		double s = m;

		s *=  s1 + s2 + s3
			- (i12 + i23 + i13)
			+ i123;

		
		s /= s1 * s2 * s3;
		
		s = log(s);
		
		s /= -k * log1p(-1.0/m);

		return  s;		
	}
	
	
	static NumberFormat nf = NumberFormat.getInstance(Locale.GERMANY);
	
	static {
		nf.setMaximumFractionDigits(5);
		nf.setGroupingUsed(false);
	}

	
	public BloomFilter2 intersect(BloomFilter2 other) {
		BloomFilter2 intersected = this.clone();
		intersected.filter.and(other.filter);
		
		double c = intersected.filter.cardinality();
		double s1 = size();
		double s2 = other.size();
		
		double s = s1 + s2 - (log(c/m - 1.0 + pow(b(),k*s1) + pow(b(),k*s2))/(k*logB()));
		
		//intersected.entries = -1;
		intersected.entries = min(min(size(),other.size()),max(0, s));

		return intersected;
	}
	
	// the base for size estimates, occurs in various calculations
	private double b() {
		return 1.0 - 1.0 / m;
	}
	
	// the logarithm of the base used for various calculations
	private double logB() {
		return log1p(-1.0/m);
	}
	
	public static int inclusionExclusionUnion(List<BloomFilter2> filters) {
		double[] count = new double[1];
		LinkedList<Integer> indices = new LinkedList<Integer>();
		inExRec(0, -1, indices, filters, count);

		return (int) count[0];
	}
	
	public static int fuzzyUnion(List<BloomFilter2> filters) {
		int m = filters.get(0).m;
		int k = filters.get(0).k;
		double[] bits = new double[m];
		
		
		
		for(BloomFilter2 filter : filters)
		{
			
			for(int i=0;i<m;i++)
			{
				double currentCount = 0;
				for(int j=0;j<m;j++)
					currentCount += bits[j];
				
				double currentFP = pow(currentCount/m,k);
				/*
				if(filter.filter.get(i) && bits[i] == 0.0)
					bits[i] =  m - currentCount > 1.0 ? 1.0 : 1.0 - currentFP / m;
				else if(filter.filter.get(i))
					bits[i] += (1.0 - bits[i]) * (1.0 - currentFP);
				*/
				
				if(filter.filter.get(i) && bits[i] == 0.0)
					bits[i] =  m - currentCount > 1.0 ? 1.0 : 0.5;
				else if(filter.filter.get(i) && bits[i] < 1.0)
					bits[i] += (1.0 - bits[i]) * (1.0 - currentFP) ;//* 1.0/ filters.size() ;

			}

		}
		
		double c = 0;
		for(int i=0;i<m;i++)
			c += bits[i] ;

		double size = log1p(-c/m) / (k * log1p(-1.0/m));
		
		return (int) size;
	}
	
	private static void inExRec(int depth, int parentIdx, LinkedList<Integer> indices, List<BloomFilter2> filters,double[] count)
	{
		String sign = (depth % 2 == 0 ? "+" : "-");
		for(int i=parentIdx+1;i<filters.size();i++)
		{
			indices.add(i);
			
			List<BloomFilter2> toInterSect = new ArrayList<BloomFilter2>();
			for(int j : indices)
				toInterSect.add(filters.get(j));
			BloomFilter2 f = combiningIntersect(toInterSect);
			count[0] += (depth % 2 == 0 ? 1.0 : -1.0) * f.size();
			System.out.println(indices + " " + sign + " " + f.size());
			if(f.size() > 0)
				inExRec(depth+1, i, indices, filters, count);
			indices.removeLast();
		}
	}
	
	private static BloomFilter2 combiningIntersect(List<BloomFilter2> filters) {
		if(filters.size() == 1)
			return filters.get(0);
		//Collections.sort(filters);
		//Collections.shuffle(filters);
		List<BloomFilter2> nextF = new ArrayList<BloomFilter2>();
		for(int i=0;i<filters.size()/2;i++)
		{
			nextF.add(filters.get(i).intersect(filters.get(filters.size()-i-1)));
		}
		if(filters.size() % 2 == 1)
			nextF.add(filters.get(filters.size()/2));
		return combiningIntersect(nextF);
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Random rand = new SecureRandom();
		
		System.out.println("y \t s1 \t s2 \t s3 \t i12 \t i13 \t i23 \t i123 \t k \t m \t s \t sb");
		
		List<byte[]> values = new ArrayList<byte[]>();
		
		for(int j=0;j<100000;j++) {
			byte[] data = new byte[6];
			rand.nextBytes(data);
			values.add(data);
		}
		
		for(int i=0;i<5000;i++)
		{
			int m = 64*1024;
			int n = 20000;
			
			Collections.shuffle(values);
			
			
			List<BloomFilter2> filters = new ArrayList<BloomFilter2>();
			List<Set<byte[]>> actual = new ArrayList<Set<byte[]>>();

			filters.add(new BloomFilter2(m, n));
			filters.add(new BloomFilter2(m, n));
			filters.add(new BloomFilter2(m, n));
			
			actual.add(new HashSet<byte[]>());
			actual.add(new HashSet<byte[]>());
			actual.add(new HashSet<byte[]>());
			

			int lim1 = 0+rand.nextInt(1);
			int lim2 = 0+rand.nextInt(1);
			int lim3 = 0+rand.nextInt(80000);
			
			for(int j=0;j<lim1;j++)
			{
				byte[] shared2 = values.get(j);
				
				filters.get(0).insert(shared2);
				filters.get(1).insert(shared2);
				actual.get(0).add(shared2);
				actual.get(1).add(shared2);
			}
			
			Collections.shuffle(values);
			
			for(int j=0;j<lim2;j++)
			{
				byte[] shared3 = values.get(j);
				
				filters.get(0).insert(shared3);
				filters.get(1).insert(shared3);
				filters.get(2).insert(shared3);
				actual.get(0).add(shared3);
				actual.get(1).add(shared3);
				actual.get(2).add(shared3);
			}
			
			for(int j=0;j<filters.size();j++)
			{
				Collections.shuffle(values);
				int limit = rand.nextInt(lim3+1);
				for(int k=0;k<limit;k++)
				{
					byte[] notshared = values.get(k);
					filters.get(j).insert(notshared);
					actual.get(j).add(notshared);
				}
			}
				
			
			Collections.shuffle(filters);
			
			
			
			
			Set<byte[]> s1 = actual.get(0);
			Set<byte[]> s2 = actual.get(1);
			Set<byte[]> s3 = actual.get(2);
			
			BloomFilter2 f1 = filters.get(0);
			BloomFilter2 f2 = filters.get(1);
			BloomFilter2 f3 = filters.get(2);
			
			
			

			//System.out.println(f1.sizeEstimate()+" "+s1.size());
			
			s1.retainAll(s2);
			
			//System.out.println(s1.size()+" "+f1.intersect2(f2));
			
			s1.retainAll(s3);
			
			//System.out.println(s1.size()+"\t"+intersect3(f1, f2, f3));
			System.out.print(s1.size()+"\t");
			intersect3(f1, f2, f3);
		}
		
		/*
		 	  
		  log(s1/m * s2/m * s3/m / (( s1 + s2 + s3 -(i12 + i23 + i13) + i123) / m)) / (k * log(1.0-1.0/m))
		  
		  log(s1/m * s2/m * s3/m)
		  
		  log((s1 + s2 + s3 -(i12 + i23 + i13) + i123) / m) 
		  
		  log(i123/m) / (k * log(1.0-1.0/m))
		  
		  ---------------
		  
		  
1 / (k * log(1.0-1.0/m))
s1*s2*s3/(m*m*m)
(s1+s2+s3)/m
(i12+i13+i23)/m
(i12*i13*i23)/(m*m*m)
log(i123/m)
log(i123/m) / (k * log(1.0-1.0/m))
		 
		 
		 */
		
		
		
		
		//System.out.println(inclusionExclusionUnion(filters) +" actual:"+added.size()+" reference:"+reference.sizeEstimate());

				
				
	}

	
	
	
}
