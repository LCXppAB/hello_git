package neu;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDAO {
    public static boolean namespaceExists(String namespace, HBaseAdmin admin){
        boolean exists = false;
        try {
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
                if (namespaceDescriptor.getName().equals(namespace)) {
                    exists = true;
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }
    public static void createNamespace(String namespace,HBaseAdmin admin) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespaceDescriptor);
    }
    public static void createTable(String tablename,HBaseAdmin admin,String... cf) throws IOException {
        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tablename));
        for(String str:cf){
            //构建列族对象
            ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
            tableDescriptor.setColumnFamily(family);

        }
        TableDescriptor tableDescriptor1=tableDescriptor.build();
        admin.createTable(tableDescriptor1);

    }
    public static Result getOneRowData(HTable hTable, Get get) throws IOException {
        Result result = hTable.get(get);
        return result;
    }
}
