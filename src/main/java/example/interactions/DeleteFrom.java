package example.actions;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

public class DeleteFrom {

    public static void namespaceAndTable(final Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println("Disabling/deleting Table [" + tableName + "].");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        /*if (Validate.namespaceExists(admin, nameSpace)) {
            System.out.println("Deleting Namespace [" + nameSpace + "].");
            admin.deleteNamespace(nameSpace);
        }*/
    }

}
