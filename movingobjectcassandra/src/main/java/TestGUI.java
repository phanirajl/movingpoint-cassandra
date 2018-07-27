import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;

public class TestGUI {
    private JFrame mainFrame;
    //   private JLabel headerLabel;
    private JPanel controlPanel;
    JTextArea keyspace = new JTextArea(1, 15);
    JTextArea server = new JTextArea(1, 15);
    JTextArea table = new JTextArea(1, 15);
    JTextArea file = new JTextArea(1, 15);
    private JTextArea queryText;
    private JTextArea console = new JTextArea(6,25);
    private static String serverAccessed;
    private static String keyspaceAccessed;
    private static int indexPoint;
    private static int indexPoints;
    private static int indexLine;
    private static Object[][] result;

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

//      headerLabel = new JLabel("",JLabel.CENTER );

        mainFrame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent){
                System.exit(0);
            }
        });

        mainFrame.setVisible(true);
    }
    private void initialize(){
        mainFrame.getContentPane().removeAll();
        mainFrame.revalidate();
        mainFrame.setLayout(new GridLayout(3,3));
        JButton insertButton = new JButton("Insert Data");
        JButton queryButton = new JButton("Query Data");

        insertButton.setPreferredSize(new Dimension(60, 40));
        queryButton.setPreferredSize(new Dimension(60, 40));

        insertButton.setActionCommand("Insert");
        queryButton.setActionCommand("Query");

        insertButton.addActionListener(new ButtonClickListener());
        queryButton.addActionListener(new ButtonClickListener());

        JPanel p1 = new JPanel();
        JPanel p2 = new JPanel();
        JPanel p3 = new JPanel();
        JPanel p4 = new JPanel();
        JPanel p5 = new JPanel();
        JPanel p6 = new JPanel();
        JPanel p7 = new JPanel();
        JPanel p8 = new JPanel();
        controlPanel = new JPanel();
        controlPanel.setLayout(new GridLayout(2,1));

//      mainFrame.add(headerLabel);
        mainFrame.add(p1);
        mainFrame.add(p2);
        mainFrame.add(p3);
        mainFrame.add(p4);
        mainFrame.add(controlPanel);
        mainFrame.add(p5);
        mainFrame.add(p6);
        mainFrame.add(p7);
        mainFrame.add(p8);

        controlPanel.add(insertButton);
        controlPanel.add(queryButton);

        mainFrame.setVisible(true);
    }

    private void showQueryPanel() {
        mainFrame.getContentPane().removeAll();
        mainFrame.setLayout(new GridLayout(2,3));
        JLabel inputKeyspace = new JLabel("Input Keyspace: ", JLabel.CENTER);
        keyspace = new JTextArea(1, 15);
        JLabel inputServer = new JLabel("Input Server: ", JLabel.CENTER);
        server = new JTextArea(1, 15);
        JLabel queryLabel = new JLabel("Input Query: ", JLabel.CENTER);
        queryText = new JTextArea(5,20);
        JScrollPane sp1 = new JScrollPane(queryText);
        queryText.setLineWrap(true);
        queryText.setWrapStyleWord(true);

        JButton submitButton = new JButton("Submit");
        JButton cancelButton = new JButton("Cancel");

        submitButton.setActionCommand("Submit");
        cancelButton.setActionCommand("Cancel");

        submitButton.addActionListener(new ButtonClickListener());
        cancelButton.addActionListener(new ButtonClickListener());

        JButton enterButton = new JButton("Enter");
        enterButton.setActionCommand("Enter");
        enterButton.addActionListener(new ButtonClickListener());

        console.setLineWrap(true);
        console.setWrapStyleWord(true);
        JScrollPane sp2 = new JScrollPane(console);

        JButton homeButton = new JButton("Home");
        homeButton.setActionCommand("Home");
        homeButton.addActionListener(new ButtonClickListener());

        JButton visualizeButton = new JButton("Visualize");
        visualizeButton.setActionCommand("Visualize");
        visualizeButton.addActionListener(new ButtonClickListener());

        JPanel subP1 = new JPanel();
        JPanel subP2 = new JPanel();
        subP2.add(inputServer);
        subP2.add(server);
        subP2.add(inputKeyspace);
        subP2.add(keyspace);
        JPanel subP3 = new JPanel();
        subP3.add(enterButton);
        JPanel subP4 = new JPanel();
        JPanel subP5 = new JPanel();
        JPanel subP6 = new JPanel();
        JPanel subP7 = new JPanel();
        JPanel subP8 = new JPanel();
        JPanel subP9 = new JPanel();
        JPanel subP10 = new JPanel();

        JPanel p1 = new JPanel();
        p1.setLayout(new GridLayout(6,1));
        p1.add(subP1);
        p1.add(subP2);
        p1.add(subP3);
        p1.add(subP4);
        p1.add(subP5);
        p1.add(subP6);

        JPanel p2 = new JPanel();
        p2.add(queryLabel);
        p2.add(sp1);
        p2.add(submitButton);
        p2.add(cancelButton);
        JPanel p3 = new JPanel();
        JPanel p4 = new JPanel();
        JPanel p5 = new JPanel();
        p5.add(sp2);
        p5.add(visualizeButton);
        JPanel p6 = new JPanel();
        p6.setLayout(new GridLayout(8,1));
        p6.add(subP1);
        p6.add(subP4);
        p6.add(subP5);
        p6.add(subP6);
        p6.add(subP7);
        p6.add(subP8);
        p6.add(subP9);
        p6.add(subP10);
        subP10.add(homeButton);

//      mainFrame.add(headerLabel);
        mainFrame.add(p1);
        mainFrame.add(p2);
        mainFrame.add(p3);
        mainFrame.add(p4);
        mainFrame.add(p5);
        mainFrame.add(p6);
        mainFrame.revalidate();
    }

    private class ButtonClickListener  implements ActionListener {
        public void actionPerformed(ActionEvent e) {
            String command = e.getActionCommand();
            if ( command.equals( "Submit" ) ) {
                result = null;
                indexPoint = -1;
                indexPoints = -1;
                indexLine = -1;
                console.setText("");
                System.setOut(out);
                System.setErr(out);
                String query = queryText.getText();
                QueryEngine qe = new QueryEngine(serverAccessed, keyspaceAccessed);
                result = qe.getResult(query);
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
            else if( command.equals( "Cancel" ) )  {
                queryText.setText("");
            }
            else if ( command.equals("Insert")) {
                showInsertPanel();
            }
            else if (command.equals("Query")) {
                showQueryPanel();
            }
            else if (command.equals("Enter")) {
                serverAccessed = server.getText();
                keyspaceAccessed = keyspace.getText();
            }
            else if (command.equals("Home")) {
                initialize();
            }
            else if (command.equals("Visualize")) {
                showMap();

            }
            else if (command.equals("Create")) {
                QueryEngine qe = new QueryEngine(serverAccessed, keyspaceAccessed);
                qe.originalQuery(queryText.getText());
            }
            else if (command.equals("InsertData")) {
                try {
                    AddCSVToDB acdb = new AddCSVToDB(keyspace.getText(), server.getText(), table.getText(), file.getText());
                    acdb.addToDB();
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
            else if (command.equals("point")) {
                ExecCommand ec = new ExecCommand();
                String output = ec.executeCommand("python point_preprocess.py " + file.getText());
                System.out.println(output);
                try {
                    AddCSVToDB acdb = new AddCSVToDB(keyspace.getText(), server.getText(), table.getText(), "new.csv");
                    acdb.addToDB();
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
            else if (command.equals("points")) {

            }
            else if (command.equals("line")) {

            }
            else if (command.equals("mpoint")) {

            }
        }
    }

    private void showInsertPanel() {
        mainFrame.getContentPane().removeAll();
        mainFrame.setLayout(new GridLayout(2,3));
        JLabel inputKeyspace = new JLabel("Input Keyspace: ", JLabel.CENTER);
        keyspace = new JTextArea(1, 15);
        JLabel inputServer = new JLabel("Input Server: ", JLabel.CENTER);
        server = new JTextArea(1, 15);
        JLabel queryLabel = new JLabel("Create Table: ", JLabel.CENTER);
        queryText = new JTextArea(5,20);
        JScrollPane sp1 = new JScrollPane(queryText);
        queryText.setLineWrap(true);
        queryText.setWrapStyleWord(true);

        JButton createButton = new JButton("Create");
        JButton cancelButton = new JButton("Cancel");

        createButton.setActionCommand("Create");
        cancelButton.setActionCommand("Cancel");

        createButton.addActionListener(new ButtonClickListener());
        cancelButton.addActionListener(new ButtonClickListener());

        JButton homeButton = new JButton("Home");
        homeButton.setActionCommand("Home");
        homeButton.addActionListener(new ButtonClickListener());

        JButton enterButton = new JButton("Enter");
        enterButton.setActionCommand("Enter");
        enterButton.addActionListener(new ButtonClickListener());

        JLabel inputTable = new JLabel("Input Table: ", JLabel.CENTER);
        table = new JTextArea(1, 15);
        JLabel inputFile = new JLabel("Input File: ", JLabel.CENTER);
        file = new JTextArea(1, 15);

//        JButton preprocessPoint = new JButton("Preprocess Point and Add");
//        JButton preprocessPoints = new JButton("Preprocess Points and Add");
//        JButton preprocessLine = new JButton("Preprocess Line and Add");
//        JButton preprocessMPoint = new JButton("Preprocess MPoint and Add");
//
//        preprocessPoint.setActionCommand("point");
//        preprocessPoints.setActionCommand("points");
//        preprocessLine.setActionCommand("line");
//        preprocessMPoint.setActionCommand("mpoint");
//
//        preprocessPoint.addActionListener(new ButtonClickListener());
//        preprocessPoints.addActionListener(new ButtonClickListener());
//        preprocessLine.addActionListener(new ButtonClickListener());
//        preprocessMPoint.addActionListener(new ButtonClickListener());

        JButton insertData = new JButton("Insert");
        insertData.setActionCommand("InsertData");
        insertData.addActionListener(new ButtonClickListener());

        JPanel subP1 = new JPanel();
        JPanel subP2 = new JPanel();
        subP2.add(inputServer);
        subP2.add(server);
        subP2.add(inputKeyspace);
        subP2.add(keyspace);
        JPanel subP3 = new JPanel();
        subP3.add(enterButton);
        JPanel subP4 = new JPanel();
        JPanel subP5 = new JPanel();
        JPanel subP6 = new JPanel();
        JPanel subP7 = new JPanel();
        JPanel subP8 = new JPanel();
        JPanel subP9 = new JPanel();
        JPanel subP10 = new JPanel();
        JPanel subP11 = new JPanel();
        JPanel subP12 = new JPanel();
        JPanel subP13 = new JPanel();

        subP11.add(inputTable);
        subP11.add(table);
        subP11.add(inputFile);
        subP11.add(file);
        subP11.add(insertData);
//        subP13.add(preprocessPoint);
//        subP13.add(preprocessPoints);
//        subP13.add(preprocessLine);
//        subP13.add(preprocessMPoint);

        JPanel p1 = new JPanel();
        p1.setLayout(new GridLayout(6,1));
        p1.add(subP1);
        p1.add(subP2);
        p1.add(subP3);
        p1.add(subP4);
        p1.add(subP5);
        p1.add(subP6);

        JPanel p2 = new JPanel();
        p2.add(queryLabel);
        p2.add(sp1);
        p2.add(createButton);
        p2.add(cancelButton);
        JPanel p3 = new JPanel();
        p3.setLayout(new GridLayout(3,1));
        p3.add(subP11);
        p3.add(subP12);
        p3.add(subP13);
        JPanel p4 = new JPanel();
        JPanel p5 = new JPanel();
        JPanel p6 = new JPanel();
        p6.setLayout(new GridLayout(8,1));
        p6.add(subP1);
        p6.add(subP4);
        p6.add(subP5);
        p6.add(subP6);
        p6.add(subP7);
        p6.add(subP8);
        p6.add(subP9);
        p6.add(subP10);
        subP10.add(homeButton);

//      mainFrame.add(headerLabel);
        mainFrame.add(p1);
        mainFrame.add(p2);
        mainFrame.add(p3);
        mainFrame.add(p4);
        mainFrame.add(p5);
        mainFrame.add(p6);
        mainFrame.revalidate();

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
            content = precontent + content + postContent;
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
