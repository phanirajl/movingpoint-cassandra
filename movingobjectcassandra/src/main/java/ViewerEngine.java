import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ViewerEngine {
    public void showMap(MPQueryResult res) {
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
        if (res.indexPoint!=-1) {
            for (int i=0; i<res.result.length; i++) {
                content = content + "\t\t\tvar marker = L.marker([" +  ((Point)res.result[i][res.indexPoint]).ordinat + "," +  ((Point)res.result[i][res.indexPoint]).absis + "]).addTo(mymap);\n";
            }
            content = precontent + content + postContent;
        }
        else if (res.indexLine!=-1) {
            for (int i=0; i<res.result.length; i++) {
                content = content + "\t\t\tvar latlngs = [";
                for (int j=0; j<((Line)res.result[i][res.indexLine]).no_points; j++) {
                    content = content + "[";
                    content = content + ((Line)res.result[i][res.indexLine]).point_set.get(j).ordinat + "," + ((Line)res.result[i][res.indexLine]).point_set.get(j).absis;
                    content = content + "]";
                    if (j!=((Line)res.result[i][res.indexLine]).no_points-1) {
                        content = content + ",";
                    }
                }
                content = content + "];\n";
                content = content + "\t\t\tvar polyline = L.polyline(latlngs, {color: 'red'}).addTo(mymap);";
                content = content + "   \t\t\tvar marker = L.marker([" +  ((Line)res.result[i][res.indexLine]).point_set.get(0).ordinat + "," +  ((Line)res.result[i][res.indexLine]).point_set.get(0).absis + "]).addTo(mymap);\n";
            }
            String view = "   \t\t\tmymap.setView([" + ((Line)res.result[0][res.indexLine]).point_set.get(0).ordinat +","+ ((Line)res.result[0][res.indexLine]).point_set.get(0).absis + "], 18);\n";
            content = precontent + content + view + postContent;
        }
        else if (res.indexPoints!=1) {
            for (int i=0; i<res.result.length; i++) {
                for (Point p : ((Points)res.result[i][res.indexPoints]).point_set) {
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
