package example.actions;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class Create {

    public static void namespaceAndTable(final Admin admin, String namespace, TableName tableName, ArrayList<byte[]> columnFamily) throws IOException {

        if (!Validate.namespaceExists(admin, namespace)) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }

        Collection<ColumnFamilyDescriptor> columns = new ArrayList<>();
        columnFamily.forEach(
                column -> columns.add(ColumnFamilyDescriptorBuilder.of(column))
        );

        try {
            if (!admin.tableExists(tableName)) {
                TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamilies(columns).build();
                admin.createTable(desc);
            }
        } catch (Exception e){
            System.out.println(e);
        }
    }

}
