package no.hvl.dat110.gui;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileNameExtensionFilter;

public class FileSelector extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField txt;

	/**
	 * Create the frame. 
	 */
	public FileSelector(JTextField txt) {
		this.txt = txt;
		setBounds(100, 100, 450, 300);
		
		// define layouts
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		// set up components
		JFileChooser jfc = new JFileChooser();
		jfc.setDialogTitle("Select a txt file");
		jfc.setAcceptAllFileFilterUsed(false);
		FileNameExtensionFilter extFilter = new FileNameExtensionFilter("txt","txt");
		jfc.addChoosableFileFilter(extFilter);
		
		
		jfc.addActionListener(new ActionListener() { 

			@Override
			public void actionPerformed(ActionEvent e) {
				
				jfileChooserActionPerformed(e, jfc);
				
			}
			
		});
		
		contentPane.add(jfc);
		setLocationRelativeTo(null);    // center on screen
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
	}
	
	private void jfileChooserActionPerformed(ActionEvent e, JFileChooser jfc) {
		
		if(e.getActionCommand() == JFileChooser.APPROVE_SELECTION) {
			
			File selectedfile = jfc.getSelectedFile();
			txt.setText(selectedfile.getAbsolutePath());
			this.dispose();
			
		} else {
			
			this.dispose();
		}
	}

}
