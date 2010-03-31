package lbms.plugins.mldht.kad;

import java.util.ArrayList;
import java.util.List;

import lbms.plugins.mldht.kad.messages.GetPeersResponse;

public class ScrapeResponseHandler {
	private List<GetPeersResponse>			scrapeResponses = new ArrayList<GetPeersResponse>(20);
	private int								scrapeSeeds;
	private int								scrapePeers;
	
	
	public void addGetPeersRespone(GetPeersResponse gpr)
	{
		scrapeResponses.add(gpr);
	}
	
	public int getScrapedPeers() {
		return scrapePeers;
	}
	
	public int getScrapedSeeds() {
		return scrapeSeeds;
	}
	
	public void process() {
		List<BloomFilter> seedFilters = new ArrayList<BloomFilter>();
		List<BloomFilter> peerFilters = new ArrayList<BloomFilter>();
		
		// process seeds first, we need them for some checks later (not yet implemented)
		for(GetPeersResponse response : scrapeResponses)
		{
			BloomFilter f = response.getScrapeSeeds();
			if(f != null)
				seedFilters.add(f);
		}
		
		scrapeSeeds = BloomFilter.unionSize(seedFilters);
		
		for(GetPeersResponse response : scrapeResponses)
		{
			BloomFilter f = response.getScrapePeers();
			if(f == null)
			{
				// TODO cross-check with seed filters
				f = new BloomFilter();
				for(DBItem item : response.getPeerItems())
				{
					if (item instanceof PeerAddressDBItem)
					{
						PeerAddressDBItem peer = (PeerAddressDBItem) item;
						f.insert(peer.getInetAddress());
					}
				}
			}
			
			peerFilters.add(f);
		}
		
		scrapePeers = BloomFilter.unionSize(peerFilters);
	}
	
	
}
