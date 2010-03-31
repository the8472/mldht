/**
 *
 */
package lbms.plugins.mldht.azureus.gui;

import lbms.plugins.mldht.azureus.MlDHTPlugin;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ControlAdapter;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.gudy.azureus2.ui.swt.plugins.UISWTViewEvent;
import org.gudy.azureus2.ui.swt.plugins.UISWTViewEventListener;

/**
 * @author Leonard
 *
 */
public class DHTDebugView implements UISWTViewEventListener{
	public static final String	VIEWID			= "mldht_DebugView";

	private MlDHTPlugin			plugin;
	private boolean				isCreated		= false;
	private boolean				isActivated		= false;
	private boolean				isRunning		= false;

	public DHTDebugView (MlDHTPlugin _plugin) {
		this.plugin = _plugin;
		//isRunning = plugin.getDHTs().isRunning();
	}

	/* (non-Javadoc)
	 * @see org.gudy.azureus2.ui.swt.plugins.UISWTViewEventListener#eventOccurred(org.gudy.azureus2.ui.swt.plugins.UISWTViewEvent)
	 */
	public boolean eventOccurred (UISWTViewEvent event) {
		switch (event.getType()) {

		case UISWTViewEvent.TYPE_CREATE:
			if (isCreated) {
				return false;
			}
			isCreated = true;
			break;

		case UISWTViewEvent.TYPE_FOCUSGAINED:
			activate();
			break;

		case UISWTViewEvent.TYPE_FOCUSLOST:
			deactivate();
			break;

		case UISWTViewEvent.TYPE_INITIALIZE:
			initialize((Composite) event.getData());
			break;

		case UISWTViewEvent.TYPE_CLOSE:
		case UISWTViewEvent.TYPE_DESTROY:
			delete();
			break;
		}
		return true;
	}

	/**
     *
     */
    private void delete () {
	    // TODO Auto-generated method stub

    }

	/**
     * @param data
     */
    private void initialize (Composite comp) {
    	GridData gridData = new GridData(GridData.FILL_BOTH);
		comp.setLayoutData(gridData);


		final ScrolledComposite scrollComposite = new ScrolledComposite(comp, SWT.V_SCROLL | SWT.H_SCROLL);


	    final Composite comp_on_sc = new Composite(scrollComposite,SWT.None);

		GridLayout gl = new GridLayout(2, false);
		comp_on_sc.setLayout(gl);

		gridData = new GridData(GridData.FILL_BOTH);
		comp_on_sc.setLayoutData(gridData);

		//-------------------------------------


		//-------------------------------------

		scrollComposite.setContent(comp_on_sc);
	    scrollComposite.setExpandVertical(true);
	    scrollComposite.setExpandHorizontal(true);
		scrollComposite.addControlListener(new ControlAdapter() {
			@Override
			public void controlResized(ControlEvent e) {
				scrollComposite.setMinSize(comp_on_sc.computeSize(SWT.DEFAULT, SWT.DEFAULT));
			}
		});

    }

	/**
     *
     */
    private void deactivate () {
	    // TODO Auto-generated method stub

    }

	/**
     *
     */
    private void activate () {
	    // TODO Auto-generated method stub

    }
}
