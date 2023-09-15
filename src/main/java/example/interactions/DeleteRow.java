package example.actions;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteRow {

    public static void withInfo(final Table table, byte[] rowKey) throws IOException {
        System.out.println("Deleting row [" + Bytes.toString(rowKey) + "] from Table ["
                + table.getName().getNameAsString() + "].");
        table.delete(new Delete(rowKey));
    }

}
