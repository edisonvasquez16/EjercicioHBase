package example.actions;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class GetInfo {

    public static void allColumns(final Table table, byte[] rowKey) throws IOException {

        Result row = table.get(new Get(rowKey));

        System.out.println("Row [" + Bytes.toString(row.getRow()) + "] was in Table ["
                + table.getName().getNameAsString() + "], contains the following information:");

        for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry : row.getNoVersionMap()
                .entrySet()) {

            String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

            System.out.println("  Family [" + columnFamilyName + "]:");

            for (Map.Entry<byte[], byte[]> columnNameAndValueMap : colFamilyEntry.getValue().entrySet()) {

                System.out.println("    Column [" + Bytes.toString(columnNameAndValueMap.getKey()) + "], Value: "
                        + Bytes.toString(columnNameAndValueMap.getValue()));
            }
        }
    }

}
