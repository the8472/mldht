package lbms.plugins.mldht.indexer.utils;

import lbms.plugins.mldht.kad.utils.ThreadLocalUtils;

public class Blue {
	
	final float stepSize;
	float currentDroprate = 0.0f;
	
	public Blue(float stepSize) {
		this.stepSize = stepSize;
	}
	
	public void update(BlueState queueState) {
		if(queueState == BlueState.QUEUE_EMPTY && currentDroprate > 0.0f)
			currentDroprate -= stepSize;
		if(queueState == BlueState.QUEUE_FULL && currentDroprate < 1.0f)
			currentDroprate += stepSize;
	}
	
	public boolean doDrop() {
		return ThreadLocalUtils.getThreadLocalRandom().nextFloat() < currentDroprate;
	}
	
}
