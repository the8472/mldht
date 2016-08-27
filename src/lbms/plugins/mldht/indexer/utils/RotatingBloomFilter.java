package lbms.plugins.mldht.indexer.utils;

import java.nio.ByteBuffer;

public class RotatingBloomFilter {
	
	GenericBloomFilter current;
	GenericBloomFilter previous;
	int insertCount;
	int targetSize;
	
	public RotatingBloomFilter(int targetSize, int bitCount) {
		this.targetSize = targetSize;
		current = new GenericBloomFilter(bitCount, targetSize);
		previous = new GenericBloomFilter(bitCount, targetSize);
	}
	
	
	public void insert(ByteBuffer data)
	{
		current.insert(data);
		insertCount++;
		if(insertCount >= targetSize)
		{
			synchronized (this)
			{
				GenericBloomFilter toSwap = current;
				current = previous;
				current.clear();
				previous = toSwap;
				insertCount = 0;
			}
		}
	}
	
	public boolean contains(ByteBuffer data)
	{
		return current.probablyContains(data) || previous.probablyContains(data);
	}
	
	
	
	
	
}
