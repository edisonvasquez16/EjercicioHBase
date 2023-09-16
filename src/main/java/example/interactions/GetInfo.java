package example.interactions;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Map;
import java.util.NavigableMap;

public class GetInfo {

    static Scan scan = new Scan();
    static  Calendar calendar = Calendar.getInstance();

    static LocalDate actualDate = LocalDate.now();

    public static void allInfo(final Table table, byte[] rowKey) throws IOException {

        Result row = table.get(new Get(rowKey));
        System.out.println("User [" + Bytes.toString(rowKey) + "], contains the following information:");
        printResult(row);
        //shipmentsInProcess(table, Bytes.toString(rowKey));

    }

    public static void shipmentsInProcess(final Table table, String string) throws IOException {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("Estado"),  // Column family
                Bytes.toBytes("Estado"),  // Column qualifier
                CompareFilter.CompareOp.NOT_EQUAL,  // Operador de comparación
                new SubstringComparator("Entregado a destinatario")  // Valor a comparar

        );
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                Bytes.toBytes("Estado"),  // Column family
                Bytes.toBytes("Estado"),  // Column qualifier
                CompareFilter.CompareOp.NOT_EQUAL,  // Operador de comparación
                new SubstringComparator("Devuelto")  // Valor a comparar

        );

        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(
                Bytes.toBytes("Usuarios"),  // Column family
                Bytes.toBytes("Email"),  // Column qualifier
                CompareFilter.CompareOp.EQUAL,  // Operador de comparación
                new SubstringComparator(string)  // Valor a comparar

        );

        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        fl.addFilter(filter);
        fl.addFilter(filter2);
        fl.addFilter(filter3);

        scan.setFilter(fl);
        scan.setReversed(true);

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {

                byte[] rowKeyBytes = result.getRow();
                byte[] fechaRegistroBytes = result.getValue(Bytes.toBytes("Envios"), Bytes.toBytes("Fecha"));
                String rowKey = Bytes.toString(rowKeyBytes);
                String fechaRegistro = Bytes.toString(fechaRegistroBytes);
                System.out.println("RowKey: " + rowKey);
                System.out.println("Fecha de Registro: " + fechaRegistro);

            }
        }

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

    public static void listShipmentsWithPackagesStatusToBeProgrammed(final Table table) throws  IOException {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("Estado"),  // Column family
                Bytes.toBytes("Estado"),  // Column qualifier
                CompareFilter.CompareOp.EQUAL,  // Operador de comparación
                new SubstringComparator("Recepción de paquete por programar")  // Valor a comparar
        );
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);

        scan.setFilter(filter);

        // Realizar el escaneo y procesar los resultados
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));

                    printData(rowKey, columnFamily, columnQualifier, columnValue);
                }
            }
        }
    }


    public static void listShipmentsWithPackagesWithDelicateDescription(final Table table) throws  IOException {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("Envios"),  // Column family
                Bytes.toBytes("Descripcion"),  // Column qualifier
                CompareFilter.CompareOp.EQUAL,  // Operador de comparación
                new SubstringComparator("delicado")  // Valor a comparar
        );
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);

        scan.setFilter(filter);
        int actualYear = calendar.get(Calendar.YEAR);

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    String  year = rowKey.split("-")[1].trim();
                    if (year.equals(String.valueOf(actualYear)) & columnValue.contains("delicado")) {
                        printData(rowKey, columnFamily, columnQualifier, columnValue);
                    }
                }
            }
        }
    }

    public static void listShipmentsLastMonth(final Table table) throws IOException {
        int quantity=0;
        int entregados = 0;
        int devueltos = 0;
        String yearMonth = actualDate.format(DateTimeFormatter.ofPattern("yyyy-MM"));
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(yearMonth));
        scan.setFilter(rowFilter);
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                quantity++;
                byte[] cellValue = result.getValue(Bytes.toBytes("Estado"), Bytes.toBytes("Estado"));
                String value = Bytes.toString(cellValue);
                if (value != null && value.contains("Entregado"))
                    entregados++;
                if (value != null && value.contains("Devuelto"))
                    devueltos++;
            }
            int porcentajeEntregados = entregados * 100 / quantity;
            int porcentajeDevueltos = devueltos * 100 / quantity;
            System.out.println("Total de envios: " + quantity);
            System.out.println("Porcentaje de entregados: " + porcentajeEntregados + "%");
            System.out.println("Porcentaje de devueltos: " + porcentajeDevueltos + "%");
        }
    }



    private static void printData(String rowKey, String columnFamily, String columnQualifier, String columnValue) {
        System.out.println("RowKey: " + rowKey +
                ", Column Family: " + columnFamily +
                ", Column Qualifier: " + columnQualifier +
                ", Column Value: " + columnValue);
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
