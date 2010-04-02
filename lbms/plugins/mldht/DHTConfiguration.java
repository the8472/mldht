package lbms.plugins.mldht;

import java.io.File;

public interface DHTConfiguration {
	
	public boolean isPersistingID();

	public File getNodeCachePath();

	public int getListeningPort();
	
	public boolean noRouterBootstrap();
	
	public boolean allowMultiHoming();
	
	
}
