public class Preprocess {
    public void preprocessData(String filepath) {

        if (filepath.contains("_point.csv")) {
            ExecCommand ec = new ExecCommand();
            String s = ec.executeCommand("python point_preprocess.py " + filepath);
            System.out.println(s);
        }
        else if (filepath.contains("_points.csv")) {
            ExecCommand ec = new ExecCommand();
            String s = ec.executeCommand("python points_preprocess.py " + filepath);
            System.out.println(s);
        }
        else if (filepath.contains("_line.csv")) {
            ExecCommand ec = new ExecCommand();
            String s = ec.executeCommand("python line_preprocess.py " + filepath);
            System.out.println(s);
        }
        else if (filepath.contains("_mpoint.csv")) {
            ExecCommand ec = new ExecCommand();
            String s = ec.executeCommand("python mpoint_preprocess.py " + filepath);
            System.out.println(s);
        }
//                    AddCSVToDB acdb = new AddCSVToDB(keyspace.getText(), server.getText(), tabel.getText(), "new.csv");


    }
}
