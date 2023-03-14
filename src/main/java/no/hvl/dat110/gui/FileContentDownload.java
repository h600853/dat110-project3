package no.hvl.dat110.gui;


import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;


public class FileContentDownload extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private JTextArea txtArea;
	private JScrollPane sp;
	
	private JButton btnClose = new JButton("Close");
	
	/**
	 * Create the frame.
	 */
	public FileContentDownload() {

		setBounds(100, 100, 400, 300);
		setLayout(new GridBagLayout());
		
		// add list components
		txtArea = new JTextArea();
		sp = new JScrollPane(txtArea); 
		txtArea.setEditable(false); 			// using this only for read-only
		txtArea.setLineWrap(true);
		txtArea.setWrapStyleWord(true);
		
		sp.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		sp.setPreferredSize(new Dimension(250, 250));
		
		// add action listeners
		btnClose.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				btnCloseActionPerformed();
			}
			
		});
		
        // define layouts
 		GridBagConstraints constraints = new GridBagConstraints();
 		constraints.anchor = GridBagConstraints.WEST;
 		constraints.insets = new Insets(5, 5, 5, 5);
 		
        // add components to frame and position them properly
		addComponentsToFrame(constraints);
        
		pack();
        setLocationRelativeTo(null);    		// center on screen
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setResizable(false);					// disable resizing of form
	}
	
	public void addContentToList(String txt) {
		
		txtArea.append(txt);
	}
	
	private void addComponentsToFrame(GridBagConstraints constraints) {
			
		constraints.gridx = 0;
        constraints.gridy = 0;
        constraints.gridwidth = 4;
        constraints.weightx = 1.0;
        constraints.weighty = 0.5;
        constraints.fill = GridBagConstraints.HORIZONTAL;
        add(sp, constraints);
        
		constraints.gridx = 0;
        constraints.gridy = 1;
        constraints.gridwidth = 0;
        constraints.weightx = 0.0;
        constraints.weighty = 0.5;
        constraints.fill = GridBagConstraints.NONE;
        add(btnClose, constraints);
	        
	}
	
	private void btnCloseActionPerformed() {
		this.dispose();
	}
	
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				try {
					FileContentDownload frame = new FileContentDownload();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	

}
