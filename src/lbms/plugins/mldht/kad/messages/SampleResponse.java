package lbms.plugins.mldht.kad.messages;

import lbms.plugins.mldht.kad.Key;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SampleResponse extends AbstractLookupResponse {
	
	ByteBuffer samples;

	public SampleResponse(byte[] mtid) {
		super(mtid, Method.SAMPLE_INFOHASHES, Type.RSP_MSG);
	}
	
	public void setSamples(ByteBuffer buf) {
		this.samples = buf;
	}
	
	public boolean remoteSupportsSampling() {
		return samples != null;
	}
	
	public Collection<Key> getSamples() {
		if(samples == null || samples.remaining() == 0) {
			return Collections.emptyList();
		}
		
		List<Key> keys = new ArrayList<>();
		ByteBuffer copy = samples.duplicate();
		
		while(copy.hasRemaining()) {
			keys.add(new Key(copy));
		}
		
		return keys;
	}
	
	@Override
	public Map<String, Object> getInnerMap() {
		Map<String, Object> inner = super.getInnerMap();
		
		inner.put("samples", samples);
		
		return inner;
		
	}

}
