/**
 * 
 */
package no.hvl.dat110.gui;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JPopupMenu;

/**
 * @author tdoy
 *
 */
public class PopupListener extends MouseAdapter {
	
	private JPopupMenu popup;
	
	public PopupListener(JPopupMenu popup) {
		this.popup = popup;
	}
	
	@Override
	public void mousePressed(MouseEvent e) {
		
		showPopup(e);
		
	}
	
	@Override
	public void mouseReleased(MouseEvent e) {
		
		showPopup(e);
		
	}

	
	private void showPopup(MouseEvent e) {
        if (e.isPopupTrigger()) {
            popup.show(e.getComponent(),
                       e.getX(), e.getY());
        }
    }

}
