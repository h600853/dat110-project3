package no.hvl.dat110.gui;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.rmi.RemoteException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Set;

import javax.swing.DefaultListModel;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.util.FileManager;

/**
 * 
 * @author tdoy
 *
 */
public class FilesListing extends JFrame implements PropertyChangeListener {

	private static final long serialVersionUID = 1L;
	
	private JPanel contentPane;
	private JList<String> list;
	private DefaultListModel<String> dlmodel;
	
	private FileManager filemanager;
	 
	private JTable table;

	private int counter = 0;  
	
	/**
	 * Create the frame.
	 */
	public FilesListing(FileManager fm, JTable table) {
		
		this.filemanager = fm;
		this.table = table;

		setBounds(100, 100, 300, 150);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		// add list components
		dlmodel = new DefaultListModel<>();
		list = new JList<>(dlmodel);
		JScrollPane sp = new JScrollPane(list);
        
        // add listener to list that can bring up popup menus.
        JPopupMenu popup = createPopupMenu();
        MouseListener popupListener = new PopupListener(popup);
        list.addMouseListener(popupListener);
        
        contentPane.add(sp);
        setLocationRelativeTo(null);    		// center on screen
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setResizable(false);					// disable resizing of form
	}
	
	private JPopupMenu createPopupMenu() {
		
		JPopupMenu popup = new JPopupMenu();
		JMenuItem jmSearch = new JMenuItem("Search");
		jmSearch.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				jmSearchActionPerformed();
			}
			
		});
		
		popup.add(jmSearch);
		
		return popup;
		
	}
	
	public void addFileNameToList(String filename) {
		dlmodel.addElement(filename);
	}
	
	private void findFile(String filename) throws RemoteException {
 
		Set<Message> activepeers = filemanager.requestActiveNodesForFile(filename);
		
		counter = activepeers.size();		
		
		// clear all rows
		DefaultTableModel tmodel = (DefaultTableModel) table.getModel();
		tmodel.setRowCount(0);
		
		JOptionPane.showMessageDialog(null,
                "Search completed with "+counter+" results. See results in the table", "Message",
                JOptionPane.INFORMATION_MESSAGE);
		
		for(Message msg : activepeers) {
			try {
				double size = (double) msg.getBytesOfFile().length/1000;
				NumberFormat nf = new DecimalFormat();
				nf.setMaximumFractionDigits(3);
				Object[] rdata = {msg.getNameOfFile(), msg.getHashOfFile(), nf.format(size), msg.getNodeName(), msg.getPort()};
				updateTableRows(rdata);
			} catch(Exception e) {
				//
			}
		}
		
		this.dispose();

	}
    
	private void jmSearchActionPerformed() {
		
		try {
			String selectedfile = list.getSelectedValue();
			findFile(selectedfile);

		} catch(Exception e) {
			JOptionPane.showMessageDialog(this,
                    "Error! Please select a row and try again: " + e.getMessage(), "Error",
                    JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();
		}
		
	}
	
	private void updateTableRows(Object[] rdata) {
		
		DefaultTableModel tmodel = (DefaultTableModel) table.getModel();
		tmodel.addRow(rdata);
		
	}
	
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		//		 
	} 
	

}
