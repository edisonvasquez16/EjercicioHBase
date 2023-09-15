package example.interactions;

import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

public class Validate {

    public static boolean namespaceExists(final Admin admin, final String namespaceName) throws IOException {
        try {
            admin.getNamespaceDescriptor(namespaceName);
        } catch (NamespaceNotFoundException e) {
            return false;
        }
        return true;
    }

}
