import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Scanner;

public class DbAccess {

    private static String user = "sa";
    private static String password = "";
    private static String url = "jdbc:hsqldb:file:/D:/Munka/mavenjdbc/hsqltest.db";

    public static void main(String[] args) {


        try (Connection c = DriverManager.getConnection(url, user, password)) {
            createTables(c);
            makeInsertsOnPeople(2000000,100);
            makeInsertsOnJobs(100);
            //printPersons(c);
            String targetPeople = getPerson(c, "test15000");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTables(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DROP TABLE IF EXISTS people;");
            st.executeUpdate("CREATE TABLE people (peopleId INTEGER IDENTITY PRIMARY KEY, peName VARCHAR(80), birthyear INT, jobId INT);");

            st.executeUpdate("DROP TABLE IF EXISTS jobs");
            st.executeUpdate("CREATE TABLE jobs (jobId INTEGER IDENTITY PRIMARY KEY , job VARCHAR(80));");

            System.out.println("Tables created");
        }
    }

    private static void addPerson(Connection conn, String name, int birthyear, int jobId) throws SQLException {
        try (PreparedStatement pst = conn.prepareStatement("INSERT INTO people (peName, birthyear, jobId) VALUES (?, ?, ?);")) {
            pst.setString(1, name);
            pst.setInt(2, birthyear);
            pst.setInt(3,jobId);
            pst.addBatch();
            pst.executeBatch();
        }
    }

    private static void addJobs(Connection connection, String jobName) throws SQLException {
        try (PreparedStatement pst = connection.prepareStatement("INSERT INTO jobs (job) VALUES (?);")) {
            pst.setString(1, jobName);
            pst.addBatch();
            pst.executeBatch();
        }
    }


    private static void printPersons(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * from people p, jobs j WHERE p.jobId = j.jobId;")) {
            System.out.println("select done");
            while (rs.next()) {
                final String name = rs.getString("peName");
                final int byear = rs.getInt("birthyear");
                final int nameId = rs.getInt("peopleId");
                final String job = rs.getString("job");
                System.out.println("[ Id: " + nameId + ", name: " + name + ", birthyear: " + byear + ", job: "+ job + " ]");

            }
        }
    }

    private static String getPerson(Connection conn, String name) throws SQLException {
        String data = "";
        try (PreparedStatement pst = conn.prepareStatement("SELECT * from people p, jobs j WHERE p.jobId = j.jobId and p.peName = ?;")) {
             //PreparedStatement pst = conn.prepareStatement("SELECT * FROM people;")){
            pst.setString(1, name);
            ResultSet rs = pst.executeQuery();
            while(rs.next()){               //ResultSet is positioned before first row
                final Integer dataID = rs.getInt("peopleId");
                final String dataPeName = rs.getString("peName");
                final int dataBirthyear = rs.getInt("birthyear");
                final String dataJob = rs.getString("job");
                System.out.println(data += "[ " + dataID + " " + dataPeName + " " + dataBirthyear + " " + dataJob +  " ]");
            }
            rs.close();
        }
        return data;
    }

    private static void makeInsertsOnPeople(int size, int jobNumber) throws SQLException {
        try (Connection c = DriverManager.getConnection(url, user, password)) {
            int makeJobs = 1;
            for (int i = 0; i <= size; i++) {
                if(makeJobs <= jobNumber){
                    addPerson(c, "test" + i, 1000 + i,makeJobs);
                    makeJobs++;
                }else{
                    makeJobs = 1;
                    addPerson(c, "test" + i, 1000 + i,makeJobs);
                    makeJobs++;
                }
                System.out.println(i);
            }
        }
        System.out.println("inserted to 'people' " + size + " data elements");
    }

    private static void makeInsertsOnJobs(int size) throws SQLException {
        try (Connection c = DriverManager.getConnection(url, user, password)) {
            for (int i = 0; i <= size; i++) {
                addJobs(c, "job" + i);
                //System.out.println(i);
            }
        }
        System.out.println("inserted to 'jobs' " + size + " data elements");
    }


}