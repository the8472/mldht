package lbms.plugins.mldht.kad;

import java.io.Serializable;
import java.util.*;

/**
 * @author Damokles
 *
 */
public class Key implements Comparable<Key>, Serializable {
	
	public static final class DistanceOrder implements Comparator<Key> {
		
		final Key target;
		public DistanceOrder(Key target) {
			this.target = target;
		}
		
		
		public int compare(Key o1, Key o2) {
			return target.distance(o1).compareTo(target.distance(o2));
		}
	}

	private static final long	serialVersionUID	= -1180893806923345652L;
	public static final int		SHA1_HASH_LENGTH	= 20;
	protected byte[]			hash				= new byte[SHA1_HASH_LENGTH];

	/**
	 * A Key in the DHT.
	 *
	 * Key's in the distributed hash table are just SHA-1 hashes.
	 * Key provides all necesarry operators to be used as a value.
	 */
	protected Key () {
	}

	/**
	 * Clone constructor
	 *
	 * @param k Key to clone
	 */
	public Key (Key k) {
		System.arraycopy(k.hash, 0, hash, 0, SHA1_HASH_LENGTH);
	}

	/**
	 * Creates a Key with this hash
	 *
	 * @param hash the SHA1 hash, has to be 20 bytes
	 */
	public Key (byte[] hash) {
		if (hash.length != SHA1_HASH_LENGTH) {
			throw new IllegalArgumentException(
					"Invalid Hash must be 20bytes, was: " + hash.length);
		}
		System.arraycopy(hash, 0, this.hash, 0, SHA1_HASH_LENGTH);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo (Key o) {
		for (int i = 0; i < hash.length; i++) {
			//needs & 0xFF since bytes are signed in Java
			//so we must convert to int to compare it unsigned
			if ((hash[i] & 0xFF) < (o.hash[i] & 0xFF)) {
				return -1;
			} else if ((hash[i] & 0xFF) > (o.hash[i] & 0xFF)) {
				return 1;
			}
		}
		return 0;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals (Object obj) {
		if (obj instanceof Key)
			return Arrays.equals(hash, ((Key)obj).hash);
		return super.equals(obj);
	}

	/**
	 * @return the hash
	 */
	public byte[] getHash () {
		return hash.clone();
	}
	
	public Key getDerivedKey(int idx) {
		Key k = new Key(this);
		byte[] data = k.hash;
		for(int i=0;i<32;i++)
			if(((0x01 << i) & idx) != 0)
				data[i/8] ^= 0x80 >> (i % 8);
		
		return k;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode () {
		return (hash[0] ^ hash[1] ^ hash[2] ^ hash[3] ^ hash[4]) << 24
				| (hash[5] ^ hash[6] ^ hash[7] ^ hash[8] ^ hash[9]) << 16
				| (hash[10] ^ hash[11] ^ hash[12] ^ hash[13] ^ hash[14]) << 8
				| (hash[15] ^ hash[16] ^ hash[17] ^ hash[18] ^ hash[19]);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString () {
		return toString(true);
	}
	
	public String toString(boolean nicePrint)
	{
		StringBuilder b = new StringBuilder(nicePrint ? 44 : 40);
		for (int i = 0; i < hash.length; i++) {
			if (nicePrint && i % 4 == 0 && i > 0) {
				b.append(' ');
			}
			int nibble = (hash[i] & 0xF0) >> 4;
			b.append((char)(nibble < 0x0A ? '0'+nibble : 'A'+nibble-10 ));
			nibble = hash[i] & 0x0F;
			b.append((char)(nibble < 0x0A ? '0'+nibble : 'A'+nibble-10 ));
		}
		return b.toString();		
	}

	/**
	 * Returns the approximate distance of this key to the other key.
	 *
	 * Distance is simplified by returning the index of the first different Bit.
	 *
	 * @param id Key to compare to.
	 * @return integer marking the different bits of the keys
	 */
	public int findApproxKeyDistance (Key id) {

		// XOR our id and the sender's ID
		Key d = Key.distance(id, this);
		// now use the first on bit to determine which bucket it should go in

		int bit_on = 0xFF;
		byte[] data_hash = d.getHash();
		for (int i = 0; i < 20; i++) {
			// get the byte
			int b = data_hash[i] & 0xFF;
			// no bit on in this byte so continue
			if (b == 0) {
				continue;
			}

			for (int j = 0; j < 8; j++) {
				if ((b & (0x80 >> j)) != 0) {
					// we have found the bit
					bit_on = i * 8 + j;
					return 159 - bit_on;
				}
			}
		}
		return 0;
	}

	/**
	 * Calculates the distance between two Keys.
	 *
	 * The distance is basically a XOR of both key hashes.
	 *
	 * @param x
	 * @return new Key (this.hash ^ x.hash);
	 */
	public Key distance (Key x) {

		return distance(this, x);
	}


	/**
	 * Calculates the distance between two Keys.
	 *
	 * The distance is basically a XOR of both key hashes.
	 *
	 * @param a
	 * @param b
	 * @return new Key (a.hash ^ b.hash);
	 */
	public static Key distance (Key a, Key b) {
		Key x = new Key();
		for (int i = 0; i < a.hash.length; i++) {
			x.hash[i] = (byte) (a.hash[i] ^ b.hash[i]);
		}
		return x;
	}
	
	/**
	 * Creates a random Key
	 *
	 * @return newly generated random Key
	 */
	public static Key createRandomKey () {
		Key x = new Key();
		DHT.rand.nextBytes(x.hash);
		return x;
	}
}
