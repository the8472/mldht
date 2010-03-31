package lbms.plugins.mldht.kad;

/**
 * @author Damokles
 *
 */
public interface DHTLogger {
	public void log (String message);

	public void log (Exception e);
}
