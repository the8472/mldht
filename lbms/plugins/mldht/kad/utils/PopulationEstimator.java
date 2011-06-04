package lbms.plugins.mldht.kad.utils;

import java.io.PrintStream;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.*;

import lbms.plugins.mldht.kad.DHT;
import lbms.plugins.mldht.kad.Key;
import lbms.plugins.mldht.kad.Prefix;
import lbms.plugins.mldht.kad.DHT.LogLevel;
import lbms.plugins.mldht.kad.Key.DistanceOrder;

/**
 * @author The_8472, Damokles
 *
 */
public class PopulationEstimator {

	static final int					KEYSPACE_BITS					= Key.KEY_BITS;
	static final double					KEYSPACE_SIZE					= Math.pow(2, KEYSPACE_BITS);
	
	
	static final double					DISTANCE_WEIGHT_INITIAL			= 0.3;
	static final double					DISTANCE_WEIGHT					= 0.003;
	private int							updateCount						= 0;
	static final int					INITIAL_UPDATE_COUNT			= 25;
	
	
	static final int					MAX_RAW_HISTORY					= 40;
	LinkedList<Double> 					rawDistances					= new LinkedList<Double>();

	
	private double						averageNodeDistanceExp2			= KEYSPACE_BITS;
	private List<PopulationListener>	listeners						= new ArrayList<PopulationListener>(1);
	private static final int			MAX_RECENT_LOOKUP_CACHE_SIZE	= 40;
	private Deque<Prefix>				recentlySeenPrefixes			= new LinkedList<Prefix>();

	public long getEstimate () {
		return (long) (Math.pow(2, KEYSPACE_BITS - averageNodeDistanceExp2));

	}

	
	public double getRawDistanceEstimate() {
		return averageNodeDistanceExp2;
	}

	public void setInitialRawDistanceEstimate(double initialValue) {
		averageNodeDistanceExp2 = initialValue;
		if(averageNodeDistanceExp2 > KEYSPACE_BITS)
			averageNodeDistanceExp2 = KEYSPACE_BITS;
	}
	
	private double distanceToDouble(Key a, Key b) {
		byte[] rawDistance = a.distance(b).getHash();
		double distance = 0;
		
		int nonZeroBytes = 0;
		for (int j = 0; j < Key.SHA1_HASH_LENGTH; j++) {
			if (rawDistance[j] == 0) {
				continue;
			}
			if (nonZeroBytes == 8) {
				break;
			}
			nonZeroBytes++;
			distance += (rawDistance[j] & 0xFF)
					* Math.pow(2, KEYSPACE_BITS - (j + 1) * 8);
		}
		
		return distance;
	}

	double toLog2(double value)
	{
		return Math.log(value)/Math.log(2);
	}
	
	long estimate(double avg) {
		return (long) (Math.pow(2, KEYSPACE_BITS - avg ));
	}
	
	
	private double median()
	{
		double values[] = new double[rawDistances.size()];
		int i=0;
		for(Double d : rawDistances)
			values[i++] = d;
		Arrays.sort(values);
		// use a weighted 2-element median for max. accuracy 
		double middle = (values.length - 1.0) / 2.0 ;
		int idx1 = (int) Math.floor(middle);
		int idx2 = (int) Math.ceil(middle);
		double middleWeight = middle - idx1;
		return values[idx1] * (1.0 - middleWeight) + values[idx2] * middleWeight;
	}
	
	private double mean(double[] values)
	{
		double result = 0.0;
		for(int i=0;i<values.length;i++)
			result += values[i];
		return result / values.length;
	}
	
	public void update (Set<Key> neighbors, Key target) {
		DHT.log("Estimator: new node group of "+neighbors.size(), LogLevel.Debug);
		Prefix prefix = Prefix.getCommonPrefix(neighbors);
		
		synchronized (recentlySeenPrefixes)
		{
			for(Prefix oldPrefix : recentlySeenPrefixes)
			{
				if(oldPrefix.isPrefixOf(prefix))
				{
					/*
					 * displace old entry, narrower entries will also replace
					 * wider ones, to clean out accidents like prefixes covering
					 * huge fractions of the keyspace
					 */
					recentlySeenPrefixes.remove(oldPrefix);
					recentlySeenPrefixes.addLast(prefix);
					return;
				}
				// new prefix is wider than the old one, return but do not displace
				if(prefix.isPrefixOf(oldPrefix))
					return;
			}

			// no match found => add
			recentlySeenPrefixes.addLast(prefix);
			if(recentlySeenPrefixes.size() > MAX_RECENT_LOOKUP_CACHE_SIZE)
				recentlySeenPrefixes.removeFirst();
		}
		
		
		ArrayList<Key> found = new ArrayList<Key>(neighbors);
		Collections.sort(found,new Key.DistanceOrder(target));

		synchronized (PopulationEstimator.class)
		{

			for(int i=1;i<found.size();i++)
			{
				rawDistances.add(distanceToDouble(target, found.get(i)) - distanceToDouble(target, found.get(i-1)));
			}

			// distances are exponentially distributed. since we're taking the median we need to compensate here
			double median = median()/Math.log(2);

			// work in log2 space for better averaging
			median = toLog2(median);

			DHT.log("Estimator: distance value: " + median + " avg:" + averageNodeDistanceExp2, LogLevel.Debug);


			double weight = updateCount++ < INITIAL_UPDATE_COUNT ? DISTANCE_WEIGHT_INITIAL : DISTANCE_WEIGHT;

			// exponential average of the mean value
			averageNodeDistanceExp2 = median * weight  + averageNodeDistanceExp2 * (1. - weight);
			
			
			while(rawDistances.size() > MAX_RAW_HISTORY)
				rawDistances.remove();
		}
		DHT.log("Estimator: new estimate:"+getEstimate(), LogLevel.Info);
		
		fireUpdateEvent();

	}

	

	public void addListener (PopulationListener l) {
		listeners.add(l);
	}

	public void removeListener (PopulationListener l) {
		listeners.remove(l);
	}

	private void fireUpdateEvent () {
		long estimated = getEstimate();
		for (int i = 0; i < listeners.size(); i++) {
			listeners.get(i).populationUpdated(estimated);
		}
	}
	

	
	public static void main(String[] args) throws Exception {
		PrintStream out;
		NumberFormat formatter = NumberFormat.getNumberInstance(Locale.GERMANY);

		int keyspaceSize = 1000000;

		out = new PrintStream("dump.txt");
		
		Random rand = new Random();
		
		formatter.setMaximumFractionDigits(30);
		
		PopulationEstimator estimator = new PopulationEstimator();
		
		
		
		System.out.println(160-Math.log(keyspaceSize)/Math.log(2)); 
		
		Key[] keyspace = new Key[keyspaceSize];
		for(int i = 0;i< keyspaceSize;i++)
			keyspace[i] = Key.createRandomKey();
		Arrays.sort(keyspace);
		
		Key[] targetSet = keyspace.clone();
		
		for(int i=0;i<1000;i++)
		{
				Key target = Key.createRandomKey();
				
				Arrays.sort(targetSet, new Key.DistanceOrder(target));
				

				int sizeGoal = 8;
				
				TreeSet<Key> closestSet = new TreeSet<Key>();

				for(int j=0;j<sizeGoal;j++)
					closestSet.add(targetSet[j]);
				
				//estimator.update(closestSet);
				estimator.update(closestSet,target);
		}
		
	}
}
