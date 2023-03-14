package no.hvl.dat110.gui;


import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.NodeServer;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.FileManager;
import no.hvl.dat110.util.Util;
import no.hvl.dat110.chordoperations.ChordProtocols;

/**
 * 
 * @author tdoy
 *
 */
public class MainWindow extends JFrame implements PropertyChangeListener {

	private static final long serialVersionUID = 1L;
	
	private String ipaddress = "process15";				// static ip (name in this case)
	private int port = 9015;							// static port
	private NodeServer chordpeer = null;
	private ChordProtocols peerprotocol = null;
	private FileManager filemanager;
	private FilesListing flistframe;
	
	private JLabel lbl = new JLabel("Choose a file:");
	private JTextField txt = new JTextField(30);
	private JButton btnBrowse = new JButton("Browse");
	private JButton btnDistribute = new JButton("Distribute");
    
    private JLabel lblTxtArea = new JLabel("File and active peers");
    private JTable table;
    private JPopupMenu popup;
    private JScrollPane sp;
    
    private NodeInterface selectedpeer = null;

    private ExecutorService backgroundExec = Executors.newCachedThreadPool();
	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				try {
					MainWindow window = new MainWindow();
					window.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public MainWindow() {
		
		try {
			startProcesses();		// start the 5 processes - strong coupling here
		} catch (InterruptedException e) {
			//
		}
		initialize();
	}
	
	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {

		setTitle("ChordDHTPeer - Distributed/Decentralized P2P File Servers ("+ipaddress+"|"+port+")");
		setBounds(130, 130, 550, 650);
		setLayout(new GridBagLayout());
		
		// define menubar, menus
		JMenuBar jmb = new JMenuBar();
		JMenu menuFile = new JMenu("File");
		JMenu menuRing = new JMenu("Ring");
		JMenu menuConfig = new JMenu("Configure");
		JMenu menuDownload = new JMenu("Search");
		
		// menu items
		JMenuItem jmopen = new JMenuItem("Open");
		jmopen.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				btnBrowseActionPerformed();
				
			}
			
		});
		menuFile.add(jmopen);
		
		JMenuItem jmexit = new JMenuItem("Exit");
		jmexit.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				jmexitActionPerformed(e);
			}
			
		});
		menuFile.add(jmexit);
		
		JMenuItem jmjoin = new JMenuItem("Create/Join Ring");
		
		jmjoin.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {				
				
				boolean cond = false;
				try {
					String succ = chordpeer.getNode().getSuccessor().getNodeName();
					String pred = chordpeer.getNode().getPredecessor().getNodeName();
					cond = chordpeer.getNode().getNodeName().equals(succ) && chordpeer.getNode().getNodeName().equals(pred);
				} catch(RemoteException e1) {
					JOptionPane.showMessageDialog(null,"Error joining ring", "Message",JOptionPane.ERROR_MESSAGE);
				}

				if(cond) {
					peerprotocol.joinRing();
					peerprotocol.stabilizationProtocols();
				} else {
					JOptionPane.showMessageDialog(null,"Node already joined the ring", "Message",JOptionPane.INFORMATION_MESSAGE);
				}

			
			}			
		});
		menuRing.add(jmjoin);
		
		JMenuItem jmleave = new JMenuItem("Leave Ring");
		jmleave.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				try {
					peerprotocol.leaveRing();
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}				
			}
			
		});
		menuRing.add(jmleave);
		
		JMenuItem jmconfigip = new JMenuItem("IP/Port");
		jmconfigip.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				// call the ip/port form
				
			}
			
		});
		
		menuConfig.add(jmconfigip);
		
		JMenuItem jmconfig = new JMenuItem("Tracker");
		jmconfig.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				// call the config form
				
			}
			
		});
		menuConfig.add(jmconfig);
		
		// menuitems for menuDownload
		JMenuItem jmFind = new JMenuItem("Find");
		jmFind.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				// call the find form containing all files that have been distributed. Search any file
				jmFindActionPerformed();
	
			}
			
		});
		menuDownload.add(jmFind);
		
		// add menus to the menubar
		jmb.add(menuFile);
		jmb.add(menuRing);
		jmb.add(menuConfig);
		jmb.add(menuDownload);
		
		setJMenuBar(jmb);
		
		// set up other components		
		btnBrowse.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				btnBrowseActionPerformed();
			}
			
		});
		
		btnDistribute.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				btnDistributeActionPerformed();
			}
			
		});
               
        // table

        DefaultTableModel dfm = new DefaultTableModel();
        dfm.addColumn("Filename");
        dfm.addColumn("Hash");
        dfm.addColumn("Size (kb)");
        dfm.addColumn("Active peer");
        dfm.addColumn("Port");
        table = new JTable(dfm);
        
        sp = new JScrollPane(table);
        sp.setPreferredSize(new Dimension(420, 100));
        table.setFillsViewportHeight(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        
        // popupmenu to be used in the table
        popup = new JPopupMenu();
        JMenuItem jmtdownload = new JMenuItem("Download");
        jmtdownload.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				downloadFile();
			}
        	
        });
        popup.add(jmtdownload);
        
        JMenuItem jmtupdate = new JMenuItem("Update");
        jmtupdate.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				
				updateFile();
			}
        	
        });
        popup.add(jmtupdate);
        
        //Add listener to table that can bring up popup menus.
        MouseListener popupListener = new PopupListener(popup);
        table.addMouseListener(popupListener);
        
		// define layouts
		GridBagConstraints constraints = new GridBagConstraints();
		constraints.anchor = GridBagConstraints.WEST;
		constraints.insets = new Insets(5, 5, 5, 5);
		
        // add components to frame and position them properly
		addComponentsToFrame(constraints);
        
        pack();
        setLocationRelativeTo(null);    // center
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setResizable(false);			// disable resizing of form
		
		backgroundExec.execute(new Runnable() {

			@Override
			public void run() {
				chordpeer = new NodeServer(ipaddress, port, true); // start
				peerprotocol = new ChordProtocols(chordpeer.getNode());
				initializeFileManagerAndListFrame();
			}
			 
		});
		
	}
	
	private void addComponentsToFrame(GridBagConstraints constraints) {
		
		constraints.gridx = 0;
        constraints.gridy = 0;
        constraints.weightx = 1.0;
        constraints.weighty = 0.5;
        add(lbl, constraints);
 
        constraints.gridx = 1;
        constraints.weightx = 1.0;
        constraints.weighty = 0.5;
        constraints.fill = GridBagConstraints.HORIZONTAL;
        add(txt, constraints);
 
        constraints.gridx = 2;
        constraints.gridy = 0;
        constraints.weightx = 1.0 ;
        constraints.weighty = 0.5;
        constraints.gridwidth = 2;
        constraints.fill = GridBagConstraints.NONE;
        add(btnBrowse, constraints);
 
        constraints.gridx = 1;
        constraints.gridy = 1;
        constraints.weightx = 1.0;
        constraints.weighty = 0.5;
        constraints.anchor = GridBagConstraints.CENTER;
        add(btnDistribute, constraints);
        
        constraints.gridx = 0;
        constraints.gridy = 3;
        constraints.gridwidth = 2;
        constraints.weightx = 1.0;
        constraints.weighty = 0.5;
        constraints.anchor = GridBagConstraints.WEST;
        add(lblTxtArea, constraints);
 
        constraints.gridx = 0;
        constraints.gridy = 4;
        constraints.gridwidth = 4;
        constraints.weightx = 1.0;
        constraints.weighty = 0.5;
        constraints.fill = GridBagConstraints.HORIZONTAL;
        add(sp, constraints);
	}
	
	private void btnBrowseActionPerformed() {
		
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				try {
					FileSelector frame = new FileSelector(txt);
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}
	
	private void jmFindActionPerformed() {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				flistframe.setVisible(true);
			}
		});
	}
	
	private void initializeFileManagerAndListFrame() {
		try {
			if(filemanager == null) {
				filemanager = new FileManager(chordpeer.getNode(),"", Util.numReplicas);
			}
			flistframe = new FilesListing(filemanager, table);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void btnDistributeActionPerformed() {
		
		try {
			
			filemanager = new FileManager(chordpeer.getNode(), txt.getText(), Util.numReplicas);
			FileReplicator frtask = new FileReplicator(filemanager, flistframe);			
			frtask.addPropertyChangeListener(this);
			frtask.execute();
			
		} catch(Exception ex) {
			ex.printStackTrace();
			JOptionPane.showMessageDialog(this,
                    "Error executing file distribution task: " + ex.getMessage(), "Error",
                    JOptionPane.ERROR_MESSAGE);
		}
		
	}

	private void jmexitActionPerformed(ActionEvent e) {
		try {
			peerprotocol.leaveRing();
		} catch (RemoteException e2) {
			e2.printStackTrace();
		}	// leave gracefully - update successors/predecessors and transfer keys accordingly
		try {
			Thread.sleep(3000); 	// wait a little for transfer to complete before shutting down
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		this.dispose(); 		// close the form
		System.exit(0);			// kill everything
	}
	
	private Message getContent() throws RemoteException {
		
		int selectedrow = table.getSelectedRow();
		TableModel tmodel = table.getModel();

		Object fileId = tmodel.getValueAt(selectedrow, 1);
		String peerAddress = tmodel.getValueAt(selectedrow, 3).toString();
		String port = tmodel.getValueAt(selectedrow, 4).toString();
		
		// contact the peer using the listed address and port
		selectedpeer = Util.getProcessStub(peerAddress, Integer.valueOf(port));
		
		Message peerdata = selectedpeer.getFilesMetadata((BigInteger) fileId);		
		
		return peerdata;
	}
	
	private void downloadFile() {
		
		try {
			Message peerdata = getContent();
			String filecontent = new String(peerdata.getBytesOfFile());
			
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					try {
						FileContentDownload fcframe = new FileContentDownload();
						fcframe.addContentToList(filecontent);
						fcframe.setVisible(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			
		} catch(Exception ex) {
			JOptionPane.showMessageDialog(this,
                    "Error! Please select a row and try again: " + ex.getMessage(), "Error",
                    JOptionPane.ERROR_MESSAGE);
			ex.printStackTrace();
		}
	}
	
	private void updateFile() {
		
		try {
			
			Message peerdata = getContent();
			String filecontent = new String(peerdata.getBytesOfFile());
			
			SwingUtilities.invokeLater(new Runnable() {
				public void run() {
					try {
						FileContentUpdate fcframe = new FileContentUpdate(filemanager, selectedpeer, peerdata);
						fcframe.addContentToList(filecontent);
						fcframe.setVisible(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			
		} catch(Exception ex) {
			JOptionPane.showMessageDialog(this,
                    "Error! Please select a row and try again: " + ex.getMessage(), "Error",
                    JOptionPane.ERROR_MESSAGE);
			ex.printStackTrace();
		}
		
	}
	
	@Override
	public void propertyChange(PropertyChangeEvent evt) {
		//		
	}
	
	private void startProcesses() throws InterruptedException {
		Thread.sleep(1000);
		new NodeServer("process1", 9091);
		Thread.sleep(2000);
		new NodeServer("process2", 9092);
		Thread.sleep(2000);
		new NodeServer("process3", 9093);
		Thread.sleep(2000);
		new NodeServer("process4", 9094);
		Thread.sleep(2000);
		new NodeServer("process5", 9095);
	}

}
