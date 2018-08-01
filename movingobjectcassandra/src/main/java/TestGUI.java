import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.*;
import javax.swing.border.Border;
import java.awt.*;
import java.awt.event.*;
import java.io.PrintStream;
import java.io.IOException;
import java.nio.file.Files;
import java.io.File;

public class TestGUI {
    private JFrame mainFrame;
    private static String serverAccessed;
    private static String keyspaceAccessed;
    private static int indexPoint;
    private static int indexPoints;
    private static int indexLine;
    private static Object[][] result;
    JTextArea keyspace = new JTextArea();
    JTextArea server = new JTextArea();
    JTextArea query = new JTextArea();
    JTextArea tableQuery = new JTextArea();
    JTextArea console = new JTextArea(18, 78);
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
        mainFrame.setSize(1000,800);

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

        JButton submitButton = new JButton("Submit");
        submitButton.setActionCommand("Submit");
        submitButton.addActionListener(new ButtonClickListener());

        JButton clearButton = new JButton("Clear");
        clearButton.setActionCommand("Clear");
        clearButton.addActionListener(new ButtonClickListener());

        p10.add(inputQuery);
        p10.add(query);
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
        tableQuery = new JTextArea(10, 78);

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
                result = qe.getResult(queryText);
                if (result!=null) {
                    for (int j=0; j<result[0].length; j++) {
                        if (result[0][j] != null) {
                            if (result[0][j].getClass() == Point.class) {
                                indexPoint = j;
                            }
                            else if (result[0][j].getClass() == Points.class) {
                                indexPoints = j;
                            }
                            else if (result[0][j].getClass() == Line.class) {
                                indexLine = j;
                            }
                        }

                    }
                }
            }
            else if (command.equals("Clear")) {
                query.setText("");
            }
            else if (command.equals("Visualize")) {
                showMap();
            }
            else if (command.equals("Insert")) {
                try {
                    AddCSVToDB acdb = new AddCSVToDB(keyspace.getText(), server.getText(), tabel.getText(), filepath.getText());
                    acdb.addToDB();
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
            else if (command.equals("Create")) {
                QueryEngine qe = new QueryEngine(serverAccessed, keyspaceAccessed);
                qe.originalQuery(tableQuery.getText());
            }
        }
    }

    private void showMap() {
        String content = "";
        String precontent = "<html>\n" +
                "\t<head>\n" +
                "\t\t<link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.3.1/dist/leaflet.css\" integrity=\"sha512-Rksm5RenBEKSKFjgI3a41vrjkw4EVPlJ3+OiI65vTjIdo9brlAacEuKOiQ5OFh7cOI1bkDwLqdLw3Zg0cRJAAQ==\" crossorigin=\"\"/>\n" +
                "   \t\t<script src=\"https://unpkg.com/leaflet@1.3.1/dist/leaflet.js\" integrity=\"sha512-/Nsx9X4HebavoBvEBuyp3I7od5tA0UzAxs+j83KgC8PU0kgB4XiK4Lfe4y4cgBtaRJQEIFCW+oC506aPT2L1zw==\" crossorigin=\"\"></script>\n" +
                "   \t\t<style type=\"text/css\">\n" +
                "   \t\t\t#mapid {\n" +
                "\t\t\t    height: 400px;\n" +
                "\t\t\t    width: 600px; }\n" +
                "   \t\t</style>\n" +
                "\n" +
                "\t</head>\n" +
                "\t<body>\n" +
                "\n" +
                "\t\t<div id=\"mapid\"></div>\n" +
                "\t\t<script>\n" +
                "   \t\t\tvar mymap = L.map('mapid').setView([-2.980039043, 104.7500297], 1);\n" +
                "   \t\t\tL.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}', {\n" +
                "    \t\t\tattribution: 'Map data &copy; <a href=\"https://www.openstreetmap.org/\">OpenStreetMap</a> contributors, <a href=\"https://creativecommons.org/licenses/by-sa/2.0/\">CC-BY-SA</a>, Imagery © <a href=\"https://www.mapbox.com/\">Mapbox</a>',\n" +
                "    \t\t\tmaxZoom: 18,\n" +
                "    \t\t\tid: 'mapbox.streets',\n" +
                "    \t\t\taccessToken: 'pk.eyJ1Ijoic2NhcmxldHRhanVsaWEiLCJhIjoiY2ppenM3dDd5MGFjMzNrcGN4YzdwdG0xNyJ9.-c5Wgxw2uqnAVrdHFSG9Jw'\n" +
                "\t\t\t}).addTo(mymap);\n";
        String postContent = "   \t\t</script>\n"+ "\t</body>\n" + "</html>";
        if (indexPoint!=-1) {
            for (int i=0; i<result.length; i++) {
                content = content + "\t\t\tvar marker = L.marker([" +  ((Point)result[i][indexPoint]).ordinat + "," +  ((Point)result[i][indexPoint]).absis + "]).addTo(mymap);\n";
            }
            content = precontent + content + postContent;
        }
        else if (indexLine!=-1) {
            content = content + "\t\t\tvar latlngs = [";
            for (int i=0; i<result.length; i++) {
                for (int j=0; j<((Line)result[i][indexLine]).no_points; j++) {
                    content = content + "[";
                    content = content + ((Line)result[i][indexLine]).point_set.get(j).ordinat + "," + ((Line)result[i][indexLine]).point_set.get(j).absis;
                    content = content + "]";
                    if (j!=((Line)result[i][indexLine]).no_points-1) {
                        content = content + ",";
                    }
                }
                content = content + "];\n";
                content = content + "\t\t\tvar polyline = L.polyline(latlngs, {color: 'red'}).addTo(mymap);";
            }
            String view = "   \t\t\tmymap.setView([" + ((Line)result[0][indexLine]).point_set.get(0).ordinat +","+ ((Line)result[0][indexLine]).point_set.get(0).absis + "], 18);\n";
            view = view + "   \t\t\tvar marker = L.marker([" +  ((Line)result[0][indexLine]).point_set.get(0).ordinat + "," +  ((Line)result[0][indexLine]).point_set.get(0).absis + "]).addTo(mymap);\n";
            content = precontent + content + view + postContent;
        }
        else if (indexPoints!=1) {
            for (int i=0; i<result.length; i++) {
                for (Point p : ((Points)result[i][indexPoints]).point_set) {
                    content = content + "\t\t\tvar marker = L.marker([" +  p.ordinat + "," +  p.absis + "]).addTo(mymap);\n";
                }
            }
            content = precontent + content + postContent;
        }
        else {
            content = precontent + postContent;
        }
        File file = new File("test.html");
        try {
            Files.write(file.toPath(), content.getBytes());
            Desktop.getDesktop().browse(file.toURI());
        } catch (IOException exp) {
            // TODO Auto-generated catch block
        }
    }
}