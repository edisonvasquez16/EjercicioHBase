package example.actions;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;

public class PutRow {

    public static void rowToTable(Table table, byte[] rowId, ArrayList<ArrayList<byte[]>> map) {
        map.forEach(
                fields -> {
                    try {
                        table.put(new Put(rowId)
                                .addColumn(fields.get(2), fields.get(0), fields.get(1)));
                        System.out.println("Row [" + Bytes.toString(rowId) + "] \n" +
                                " Table: [" + table.getName().getNameAsString() + "] \n"
                                + "Family: [" + Bytes.toString(fields.get(2)) + " \n"
                                + "Data: [" + Bytes.toString(fields.get(0)) + " : "
                                + Bytes.toString(fields.get(1)));

                    } catch (Exception e) {
                        System.out.println(e);
                    }
                }
        );
    }
}
