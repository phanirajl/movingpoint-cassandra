import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import java.util.UUID;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.io.PrintStream;

public class TestGUI {
    private JFrame mainFrame;
    //   private JLabel headerLabel;
    private JPanel controlPanel;
    private JTextArea queryText;
    private JTextArea console = new JTextArea(8,30);
    PrintStream out = new PrintStream( new TextAreaOutputStream( console ) );
    SpringLayout layout = new SpringLayout();

    public TestGUI(){
        prepareGUI();
    }
    public static void main(String[] args){
        TestGUI swingControlDemo = new TestGUI();
        swingControlDemo.showEventDemo();
    }
    private void prepareGUI(){
        mainFrame = new JFrame("Moving Object");
        mainFrame.setSize(1000,800);
        mainFrame.setLayout(new GridLayout(0,2));

//      headerLabel = new JLabel("",JLabel.CENTER );

        mainFrame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowEvent){
                System.exit(0);
            }
        });
        controlPanel = new JPanel();
        controlPanel.setLayout(new GridLayout(4,0));

//      mainFrame.add(headerLabel);
        mainFrame.add(controlPanel);
        mainFrame.setVisible(true);
    }
    private void showEventDemo(){
//      headerLabel.setText("Control in action: Button");

        JLabel queryLabel = new JLabel("Input Query: ", JLabel.CENTER);
        queryText = new JTextArea(8,30);
        JScrollPane sp1 = new JScrollPane(queryText);
        queryText.setLineWrap(true);
        queryText.setWrapStyleWord(true);

        JButton submitButton = new JButton("Submit");
        JButton cancelButton = new JButton("Cancel");

        submitButton.setActionCommand("Submit");
        cancelButton.setActionCommand("Cancel");

        submitButton.addActionListener(new ButtonClickListener());
        cancelButton.addActionListener(new ButtonClickListener());

        JPanel panel1 = new JPanel();
        panel1.add(queryLabel);
        JPanel panel2 = new JPanel();
        panel2.add(sp1);
        JPanel panel3 = new JPanel();
        panel3.add(submitButton);
        panel3.add(cancelButton);
        console.setLineWrap(true);
        console.setWrapStyleWord(true);
        JScrollPane sp2 = new JScrollPane(console);
        JPanel panel4 = new JPanel();
        panel4.add(sp2);

        controlPanel.add(panel1);
        controlPanel.add(panel2);
        controlPanel.add(panel3);
        controlPanel.add(panel4);

        mainFrame.setVisible(true);
    }
    private class ButtonClickListener implements ActionListener{
        public void actionPerformed(ActionEvent e) {
            String command = e.getActionCommand();
            if ( command.equals( "Submit" ) ) {
                System.setOut(out);
                System.setErr(out);
                String query = queryText.getText();
                QueryEngine qe = new QueryEngine();
                Object[][] result = qe.getResult(query);
            }
            else if( command.equals( "Cancel" ) )  {
                queryText.setText("");
            }
        }
    }
}
