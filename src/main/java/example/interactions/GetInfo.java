package example.interactions;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;
import java.util.NavigableMap;

public class GetInfo {

    static Scan scan = new Scan();

    public static void allColumns(final Table table, byte[] rowKey) throws IOException {

        Result row = table.get(new Get(rowKey));
        System.out.println("User [" + Bytes.toString(rowKey) + "], contains the following information:");
        printResult(row);

    }

    public static void toLastMonth(final Table table) throws IOException {

        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        c1.add(Calendar.MONTH, -1);
        long minus = c1.getTimeInMillis();
        scan.setTimeRange(minus, c2.getTimeInMillis());
        scan.addColumn(Bytes.toBytes("Usuarios"), Bytes.toBytes("Email"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next())
            System.out.println("Found row : " + result);

    }


    private static void printResult(Result row) {

        try {
            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry : row.getNoVersionMap()
                    .entrySet()) {

                String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

                System.out.println("  Family [" + columnFamilyName + "]:");

                for (Map.Entry<byte[], byte[]> columnNameAndValueMap : colFamilyEntry.getValue().entrySet()) {

                    System.out.println("    Column [" + Bytes.toString(columnNameAndValueMap.getKey()) + "], Value: "
                            + Bytes.toString(columnNameAndValueMap.getValue()));
                }
            }
        } catch (Exception e) {
            System.out.println("USER NOT FOUND");
        }
    }

}
