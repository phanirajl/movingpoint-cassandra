import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.PrintStream;

public class TestGUI {
    private JFrame mainFrame;
    private static String serverAccessed;
    private static String keyspaceAccessed;
    private static int indexPoint;
    private static int indexPoints;
    private static int indexLine;
    private static MPQueryResult result;
    private static boolean isMOQuery;
    JTextArea keyspace = new JTextArea();
    JTextArea server = new JTextArea();
    JTextArea query = new JTextArea();
    JTextArea tableQuery = new JTextArea();
    JTextArea console = new JTextArea(15, 78);
    JTextArea tabel = new JTextArea();
    JTextArea filepath = new JTextArea();

    PrintStream out = new PrintStream( new TextAreaOutputStream( console ) );

    public TestGUI(){
        prepareGUI();
    }
    public static void main(String[] args){
        TestGUI swingControlDemo = new TestGUI();
        swingControlDemo.initialize();
    }

    private void prepareGUI(){

        mainFrame = new JFrame("Moving Object");
        mainFrame.setSize(1000,700);

        mainFrame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent){
                System.exit(0);
            }
        });

        mainFrame.setVisible(true);
    }

    private void initialize(){
        JTabbedPane jtp = new JTabbedPane();
        mainFrame.add(jtp);

        JPanel jp1 = new JPanel();
        JPanel jp2 = new JPanel();
        JPanel jp3 = new JPanel();
        JPanel jp4 = new JPanel();

        JLabel label1 = new JLabel();
        label1.setText("Config");
        jtp.addTab("Config", jp1);

        jtp.addTab("Create", jp2);

        JLabel label3 = new JLabel();
        label3.setText("Insert");
        jp3.add(label3);
        jtp.addTab("Insert", jp3);

        jtp.addTab("Query", jp4);


        //CONFIG
        jp1.setLayout(new GridLayout(3,3));
        JPanel p1 = new JPanel();
        JPanel p2 = new JPanel();
        JPanel p3 = new JPanel();
        JPanel p4 = new JPanel();
        JPanel p5 = new JPanel();
        JPanel p6 = new JPanel();
        JPanel p7 = new JPanel();
        JPanel p8 = new JPanel();
        JPanel p9 = new JPanel();
        p2.add(label1);

//        Border borderline = BorderFactory.createLineBorder(Color.black);

        JLabel inputKeyspace = new JLabel("Input Keyspace : ", JLabel.CENTER);
        keyspace = new JTextArea(1, 15);
        JLabel inputServer = new JLabel("Input Server      : ", JLabel.CENTER);
        server = new JTextArea(1, 15);

        JButton setButton = new JButton("Set");
        setButton.setActionCommand("Set");
        setButton.addActionListener(new ButtonClickListener());

        p5.add(inputKeyspace);
        p5.add(keyspace);
        p5.add(inputServer);
        p5.add(server);
        p5.add(setButton);

//        p5.setBorder(borderline);

        jp1.add(p1);
        jp1.add(p2);
        jp1.add(p3);
        jp1.add(p4);
        jp1.add(p5);
        jp1.add(p6);
        jp1.add(p7);
        jp1.add(p8);
        jp1.add(p9);

        //QUERY
        jp4.setLayout(new GridLayout(2,1));
        JPanel p10 = new JPanel();
        JPanel p11 = new JPanel();

        JLabel inputQuery = new JLabel("Query :        ");
        query = new JTextArea(10, 75);
        query.setLineWrap(true);

        JRadioButton MOQuery = new JRadioButton("Query MO");
        JRadioButton NMOQuery = new JRadioButton("Query Non MO");

        MOQuery.setActionCommand("qmo");
        NMOQuery.setActionCommand("qnmo");

        ButtonGroup bg = new ButtonGroup();
        bg.add(MOQuery);
        bg.add(NMOQuery);

        MOQuery.addActionListener(new ButtonClickListener());
        NMOQuery.addActionListener(new ButtonClickListener());

        JButton submitButton = new JButton("Submit");
        submitButton.setActionCommand("Submit");
        submitButton.addActionListener(new ButtonClickListener());

        JButton clearButton = new JButton("Clear");
        clearButton.setActionCommand("Clear");
        clearButton.addActionListener(new ButtonClickListener());

        p10.add(inputQuery);
        p10.add(query);
        p10.add(MOQuery);
        p10.add(NMOQuery);
        p10.add(submitButton);
        p10.add(clearButton);

        JLabel showResult = new JLabel("Result : ");

        JButton visualizeButton = new JButton("Visualize");
        visualizeButton.setActionCommand("Visualize");
        visualizeButton.addActionListener(new ButtonClickListener());

        console.setLineWrap(true);
        JScrollPane sp = new JScrollPane(console);

        p11.add(showResult);
        p11.add(sp);
        p11.add(visualizeButton);

        jp4.add(p10);
        jp4.add(p11);

        //INSERT
        jp3.setLayout(new GridLayout(3,3));
        JPanel p12 = new JPanel();
        JPanel p13 = new JPanel();
        JPanel p14 = new JPanel();
        JPanel p15 = new JPanel();
        JPanel p16 = new JPanel();
        JPanel p17 = new JPanel();
        JPanel p18 = new JPanel();
        JPanel p19 = new JPanel();
        JPanel p20 = new JPanel();
        p13.add(label3);

        JLabel inputTabel = new JLabel("Table Name    : ", JLabel.CENTER);
        tabel = new JTextArea(1, 15);
        JLabel inputFilepath = new JLabel("Input Filepath : ", JLabel.CENTER);
        filepath = new JTextArea(1, 15);

        JButton insertButton = new JButton("Insert");
        insertButton.setActionCommand("Insert");
        insertButton.addActionListener(new ButtonClickListener());

        p16.add(inputTabel);
        p16.add(tabel);
        p16.add(inputFilepath);
        p16.add(filepath);
        p16.add(insertButton);

        jp3.add(p12);
        jp3.add(p13);
        jp3.add(p14);
        jp3.add(p15);
        jp3.add(p16);
        jp3.add(p17);
        jp3.add(p18);
        jp3.add(p19);
        jp3.add(p20);

        //CREATE
        jp2.setLayout(new GridLayout(3,1));
        JPanel p21 = new JPanel();
        JPanel p22 = new JPanel();
        JPanel p23 = new JPanel();

        JLabel createTable = new JLabel("Create Table : ");
        tableQuery = new JTextArea(8, 75);

        JButton createButton = new JButton("Create");
        createButton.setActionCommand("Create");
        createButton.addActionListener(new ButtonClickListener());

        p22.add(createTable);
        p22.add(tableQuery);
        p22.add(createButton);

        jp2.add(p21);
        jp2.add(p22);
        jp2.add(p23);

    }

    private class ButtonClickListener implements ActionListener {
        public void actionPerformed(ActionEvent e) {
            String command = e.getActionCommand();
            if (command.equals("Set")) {
                serverAccessed = server.getText();
                keyspaceAccessed = keyspace.getText();
            }
            else if (command.equals("Submit")) {
                result = null;
                indexPoint = -1;
                indexPoints = -1;
                indexLine = -1;
                console.setText("");
                System.setOut(out);
                System.setErr(out);
                String queryText = query.getText();
                QueryEngine qe = new QueryEngine(serverAccessed, keyspaceAccessed);
                if (isMOQuery == true) {
                    result = qe.mpQuery(queryText);
                }
                else {
                    qe.originalQuery(queryText);
                }
            }
            else if (command.equals("Clear")) {
                query.setText("");
            }
            else if (command.equals("Visualize")) {
                ViewerEngine ve = new ViewerEngine();
                ve.showMap(result);
            }
            else if (command.equals("Insert")) {
                try {
                    Preprocess pre = new Preprocess();
                    pre.preprocessData(filepath.getText());
                    AddCSVToDB acdb = new AddCSVToDB(keyspace.getText(), server.getText(), tabel.getText(), "new.csv");
//                    AddCSVToDB acdb = new AddCSVToDB(keyspace.getText(), server.getText(), tabel.getText(), filepath.getText());
                    acdb.addToDB();
                }
                catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
            else if (command.equals("Create")) {
                QueryEngine qe = new QueryEngine(serverAccessed, keyspaceAccessed);
                qe.originalQuery(tableQuery.getText());
            }
            else if (command.equals("qmo")) {
                isMOQuery = true;
            }
            else if (command.equals("qnmo")) {
                isMOQuery = false;
            }
        }
    }

}