/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movieratings_hbaseload;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author kumaran_jay
 */
public class HbaseLoad {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        
        HbaseLoad.insertRatings();
        HbaseLoad.insertMovieName();
    }

    public static Configuration conf = null;

    public static void createTable(String tableName, String family) throws IOException {
        conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        if (admin.tableExists(tableName)) {
            System.out.println("Table " + tableName + " already exist!");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor famColumnDescriptor = new HColumnDescriptor(
                    family.getBytes());
            tableDescriptor.addFamily(famColumnDescriptor);
            admin.createTable(tableDescriptor);
            System.out.println("Table " + tableName + " created!");
        }

    }
    
    public static void insertRatings() throws IOException {
        conf = HBaseConfiguration.create();
        createTable("ratingsTable", "ratings");
        HTable ratingsTable = new HTable(conf, "ratingsTable");
        Scanner input = new Scanner(new File("/Users/kumaran_jay/Documents/"
                + "BigData_Final_Kumaran/ml-latest/ratings.csv"));
        int rowk = 1;

        while (input.hasNextLine()) {
            String eachLine = input.nextLine();
            String[] eachRow = eachLine.split(",");
            //userId,movieId,rating,timestamp
            String userId = eachRow[0];
            String movieId = eachRow[1];
            String rating = eachRow[2];
            String timestamp = eachRow[3];

            
            Put p = new Put(Bytes.toBytes(movieId));
            p.add(Bytes.toBytes("ratings"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
            p.add(Bytes.toBytes("ratings"), Bytes.toBytes("movieId"), Bytes.toBytes(movieId));
            p.add(Bytes.toBytes("ratings"), Bytes.toBytes("rating"), Bytes.toBytes(rating));
            p.add(Bytes.toBytes("ratings"), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
            ratingsTable.put(p);
            rowk++;
        }
        System.out.println("Ratings data table inserted" + rowk);

        input.close();
        ratingsTable.close();
    }

    public static void insertMovieName() throws IOException {
        conf = HBaseConfiguration.create();
        createTable("movieNameTable", "movieName");
        HTable movieNameTable = new HTable(conf, "movieNameTable");
        Scanner input = new Scanner(new File("/Users/kumaran_jay/Documents/"
                + "BigData_Final_Kumaran/ml-latest/movies2.csv"));
        int rowk = 1;

        while (input.hasNext()) {
            String eachLine = input.nextLine();
            String[] eachRow = eachLine.split(",");
            //movieId,title,genres
            String movieId = eachRow[0];
            String title = eachRow[1];
            String year = eachRow[2];
            String genres = eachRow[3];


            Put p = new Put(Bytes.toBytes(movieId));
            p.add(Bytes.toBytes("movieName"), Bytes.toBytes("movieId"), Bytes.toBytes(movieId));
            p.add(Bytes.toBytes("movieName"), Bytes.toBytes("title"), Bytes.toBytes(title));
            p.add(Bytes.toBytes("movieName"), Bytes.toBytes("year"), Bytes.toBytes(year));
            p.add(Bytes.toBytes("movieName"), Bytes.toBytes("genres"), Bytes.toBytes(genres));
            movieNameTable.put(p);
            rowk++;
        }
        System.out.println("Movie table inserted" + rowk);
        input.close();
        movieNameTable.close();
    }

}
