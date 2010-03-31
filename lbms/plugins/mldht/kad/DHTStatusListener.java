package lbms.plugins.mldht.kad;

/**
 * @author Damokles
 *
 */
public interface DHTStatusListener {
	public void statusChanged (DHTStatus newStatus, DHTStatus oldStatus);
}
