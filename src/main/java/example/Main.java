package example;

import example.actions.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Objects;

public class Main {

    static String NAMESPACE = "HBaseBD";
    static TableName TABLE = TableName.valueOf("TableEnvios");
    static byte[] COLUMN_FAMILY;
    static byte[] COLUMN_QUALIFIER;
    static byte[] ROW_ID;
    static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

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
                        "5. EXIT \n");
                option = Integer.parseInt(br.readLine());
                switch (option) {
                    case 1:
                        System.out.println("Row key for register: ");
                        ROW_ID = captureString();

                        String addFamily = "";
                        ArrayList<byte[]> columnsFamily = new ArrayList<>();
                        ArrayList<ArrayList<byte[]>> map = new ArrayList<>();
                        while (!Objects.equals(addFamily, "EXIT")) {
                            String addColumn = "";
                            System.out.println("Column Family Name: (For terminate write EXIT)");
                            byte[] familyColumnValue = captureString();
                            if (!Bytes.toString(familyColumnValue).equals("EXIT")) {
                                columnsFamily.add(familyColumnValue);
                                while (!Objects.equals(addColumn, "EXIT")) {
                                    ArrayList<byte[]> fields = new ArrayList<>();
                                    System.out.println("Column Qualifier Name: (For terminate write EXIT)");
                                    System.out.println("Qualifier: ");
                                    COLUMN_QUALIFIER = captureString();
                                    if (!Bytes.toString(COLUMN_QUALIFIER).equals("EXIT")) {
                                        fields.add(COLUMN_QUALIFIER);
                                        System.out.println("Value: ");
                                        String value = br.readLine();
                                        fields.add(Bytes.toBytes(value));
                                        fields.add(familyColumnValue);
                                        map.add(fields);
                                    } else {
                                        addColumn = "EXIT";
                                    }
                                }
                            } else {
                                addFamily = Bytes.toString(familyColumnValue);
                            }
                        }

                        Create.namespaceAndTable(admin, NAMESPACE, TABLE, columnsFamily);
                        try (Table table = connection.getTable(TABLE)) {
                            PutRow.rowToTable(table, ROW_ID, map);
                        }
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
                        DeleteFrom.namespaceAndTable(admin, TABLE);
                        break;
                    case 5:
                        System.out.println("EXIT SUCCESSFULLY");
                        break;
                    default:
                        System.out.println("NOT VALID OPTION");
                        break;
                }
            }
        }

    }

    private static byte[] captureString() throws IOException {
        return Bytes.toBytes(br.readLine());
    }
}