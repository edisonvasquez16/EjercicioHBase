package example;

import example.actions.InsertData;
import example.interactions.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

    protected static String NAMESPACE = "HBaseBD";
    protected static TableName TABLE = TableName.valueOf("TablaEnvios");
    protected static byte[] COLUMN_QUALIFIER;
    protected static byte[] ROW_ID;
    protected static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    public static void main(final String[] args) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection();
             Admin admin = connection.getAdmin()) {
            admin.getClusterMetrics();
            System.out.println("*** CONNECTION SUCCESSFULLY ***\n");
            int option = 0;
            while (option != 5) {
                System.out.println("SELECT THE OPTION TO EXECUTE: \n" +
                        "1. INSERT DATA IN TABLE \n" +
                        "2. GET DATA FOR ROW_KEY \n" +
                        "3. DELETE ROW \n" +
                        "4. DELETE TABLE DEFAULT \n" +
                        "5. GET INFO LAST MONTH \n");
                option = Integer.parseInt(br.readLine());
                switch (option) {
                    case 1:
                        InsertData.inTable(admin, connection);
                        break;
                    case 2:
                        System.out.println("Row key for register get: ");
                        ROW_ID = captureString();
                        try (Table table = connection.getTable(TABLE)) {
                            GetInfo.allColumns(table, ROW_ID);
                        }
                        break;
                    case 3:
                        try (Table table = connection.getTable(TABLE)) {
                            System.out.println("Row key for delete data: ");
                            DeleteRow.withInfo(table, captureString());
                        }
                        break;
                    case 4:
                        //DeleteFrom.namespaceAndTable(admin, TABLE);
                        break;
                    case 5:
                        try (Table table = connection.getTable(TABLE)) {
                            GetInfo.toLastMonth(table);
                        }
                        break;
                    case 10:
                        System.out.println("EXIT SUCCESSFULLY");
                        break;
                    default:
                        System.out.println("NOT VALID OPTION");
                        break;
                }
            }
        }
    }

    protected static byte[] captureString() throws IOException {
        return Bytes.toBytes(br.readLine());
    }
}