package example.actions;

import example.Main;
import example.interactions.Create;
import example.interactions.PutRow;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class InsertData extends Main {

    public static void inTable(Admin admin, Connection connection) throws IOException {
        System.out.println("Row key for register: ");
        ROW_ID = captureString();
        String addFamily = "";
        ArrayList<byte[]> columnsFamily = new ArrayList<>();
        ArrayList<ArrayList<byte[]>> map = new ArrayList<>();
        while (!Objects.equals(addFamily, "EXIT")) {
            addFamily = captureFamilyColumns(columnsFamily, map);
        }
        Create.namespaceAndTable(admin, NAMESPACE, TABLE, columnsFamily);
        try (Table table = connection.getTable(TABLE)) {
            PutRow.rowToTable(table, ROW_ID, map);
        }
    }

    private static String captureFamilyColumns(ArrayList<byte[]> columnsFamily, ArrayList<ArrayList<byte[]>> map) throws IOException {
        String addColumn = "";
        System.out.println("Column Family Name: (For terminate write EXIT)");
        byte[] familyColumnValue = captureString();
        if (!Bytes.toString(familyColumnValue).equals("EXIT")) {
            columnsFamily.add(familyColumnValue);
            while (!Objects.equals(addColumn, "EXIT")) {
                addColumn = captureColumnsData(familyColumnValue, map);
            }
            return "";
        } else {
            return Bytes.toString(familyColumnValue);
        }
    }

    private static String captureColumnsData(byte[] familyColumnValue, ArrayList<ArrayList<byte[]>> map) throws IOException {
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
            return "";
        } else {
            return "EXIT";
        }
    }

}
