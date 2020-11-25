package neu;

import com.sun.org.apache.xerces.internal.xs.StringList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.naming.Name;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Service {
    //获取hbase配置
    private static Configuration configuration =null;
    private static Connection connection=null;


    static {
        System.out.println("123");
        configuration=HBaseConfiguration.create();
        //configuration.set("zookeeper.znode.parent","/hbase-unsecure"); //与 hbase-site-xml里面的配置信息 zookeeper.znode.parent 一致
        configuration.set("hbase.zookeeper.quorum","mnode1:2181");  //hbase 服务地址
        configuration.set("hbase.master","mnode1:60000"); //端口号
        System.out.println(Constant.NAME_SPACE);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void createNamespace(String namespace) {
        //获取管理员对象
        System.out.println("123");
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) connection.getAdmin();
            //判断命名空间是否为已创建
            boolean exists = HBaseDAO.namespaceExists(namespace, admin);
            if (exists) {
                System.out.println(namespace + "已存在");
            } else {
                HBaseDAO.createNamespace(namespace, admin);
                System.out.println(namespace + "创建成功");
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createTable(String tableName, String... cf){
        //获取管理员对象
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            if (exists) {
                System.out.println(tableName + "已存在");
            } else {
                HBaseDAO.createTable(tableName, admin, cf);
                System.out.println(tableName + "创建成功");
            }
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void uploadWeibo(String userid, String content) {
        HTable contentTable = null;
        HTable relationTable = null;
        HTable eboxTable = null;
        try {
            contentTable = (HTable) connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
        } catch (IOException e) {
            e.printStackTrace();
        }
        long timestamp = System.currentTimeMillis();
        String rowkey = userid;
        //Put put = new Put(Bytes.toBytes(rowKey));
        // put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), timestamp, Bytes.toBytes(content));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(Constant.CF1), Bytes.toBytes(Constant.USERID), Bytes.toBytes(userid));
        put.addColumn(Bytes.toBytes(Constant.CF1), Bytes.toBytes(Constant.CONTENT), Bytes.toBytes(content));
        try {
            contentTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("微博发布成功");
        //查询粉丝用户
        try {
            relationTable = (HTable) connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
            Get get = new Get(Bytes.toBytes(userid));
            get.addFamily(Bytes.toBytes(Constant.CF2));
            Result result = HBaseDAO.getOneRowData(relationTable, get);
            List<byte[]> fans = new ArrayList<byte[]>();
            for (Cell cell : result.rawCells()) {
                fans.add(CellUtil.cloneQualifier(cell));
            }
            //在邮件箱表中遍历加入fans
            List<Put> puts = new ArrayList<Put>();
            if (!fans.isEmpty()) {
                eboxTable = (HTable) connection.getTable(TableName.valueOf(Constant.EBOX_TABLE));
                for (byte[] fanid : fans) {
                    Put put_ebox = new Put(fanid);
                    //加入时间戳
                    put_ebox.addColumn(Bytes.toBytes(Constant.CF1), Bytes.toBytes(userid), timestamp, Bytes.toBytes(rowkey));
                    puts.add(put_ebox);
                    eboxTable.put(puts);
                }
                System.out.println("已向所有粉丝发送邮件");
            } else {
                System.out.println("用户没有粉丝");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addFocus(String userid, String focusUserId) {
        HTable contentTable = null;
        HTable relationTable = null;
        HTable eboxTable = null;
        try {
            relationTable = (HTable) connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
            Put putFocus = new Put(Bytes.toBytes(userid));
            putFocus.addColumn(Bytes.toBytes(Constant.CF1), Bytes.toBytes(focusUserId), Bytes.toBytes(focusUserId));
            Put putFans = new Put(Bytes.toBytes(focusUserId));
            putFans.addColumn(Bytes.toBytes(Constant.CF2), Bytes.toBytes(userid), Bytes.toBytes(userid));
            List<Put> puts = new ArrayList<Put>();
            puts.add(putFocus);
            puts.add(putFans);
            relationTable.put(puts);
            System.out.println(userid+"成功关注"+focusUserId);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void deleteFocus(String userid, String focusUserId) {
        HTable contentTable = null;
        HTable relationTable = null;
        HTable eboxTable = null;
        try {
            //修改关系表
            relationTable = (HTable) connection.getTable(TableName.valueOf(Constant.RELATION_TABLE));
            Delete deleteFocus = new Delete(Bytes.toBytes(userid));
            deleteFocus.addColumn(Bytes.toBytes(Constant.CF1), Bytes.toBytes(focusUserId));
            Delete deleteFans = new Delete(Bytes.toBytes(focusUserId));
            deleteFans.addColumn(Bytes.toBytes(Constant.CF2), Bytes.toBytes(userid));
            List<Delete> deletes = new ArrayList<Delete>();
            deletes.add(deleteFocus);
            deletes.add(deleteFans);
            relationTable.delete(deletes);
            System.out.println("用户" + userid + "的关注用户" + focusUserId + "已删除");
            System.out.println("用户" + focusUserId + "的粉丝用户" + userid + "已删除");
            //删除收件箱中数据
            eboxTable = (HTable) connection.getTable(TableName.valueOf(Constant.EBOX_TABLE));
            List<Delete> deleteList = new ArrayList<Delete>();
            Delete deleteMails = new Delete(Bytes.toBytes(userid));
            deleteMails.addColumns(Bytes.toBytes(Constant.CF1), Bytes.toBytes(focusUserId));
            deleteList.add(deleteMails);
            eboxTable.delete(deleteList);
            System.out.println("用户" + userid + "关注的用户" + focusUserId + "所有微博已删除成功");


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static  List<Weibo> getUserWeiboList(String userid) {
        List<Weibo> weibo_list = new ArrayList<Weibo>();
        HTable contentTable = null;
        HTable eboxTable = null;
        try {
            //contentTable = (HTable) connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
            eboxTable = (HTable) connection.getTable(TableName.valueOf(Constant.EBOX_TABLE));
            Get get=new Get(Bytes.toBytes(userid));
            Result result=HBaseDAO.getOneRowData(eboxTable,get);
            List<byte[]> rowkeys = new ArrayList<byte[]>();
            for (Cell cell:result.rawCells()){
                rowkeys.add(CellUtil.cloneValue(cell));
            }
            List<Get> gets = new ArrayList<Get>();
            if(!rowkeys.isEmpty()){
                contentTable = (HTable) connection.getTable(TableName.valueOf(Constant.CONTENT_TABLE));
                for(byte[] rowkey:rowkeys){
                    System.out.println(Bytes.toString(rowkey));
                    Get getWeibo=new Get(rowkey);
                    gets.add(getWeibo);

                }
                Weibo weibo = new Weibo();
                Result[] results=contentTable.get(gets);
                for(Result r:results) {
                        weibo.setUserid(Bytes.toString(r.getValue(Bytes.toBytes(Constant.CF1),Bytes.toBytes(Constant.USERID))));
                        weibo.setContent(Bytes.toString(r.getValue(Bytes.toBytes(Constant.CF1),Bytes.toBytes(Constant.CONTENT))));
                        weibo.setTime(result.rawCells()[0].getTimestamp());
                        weibo_list.add(weibo);
                        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes(Constant.CF1), Bytes.toBytes(Constant.USERID))));
                    }
                }

                for (Weibo weibo1 : weibo_list) {
                    System.out.println("用户名" + weibo1.getUserid() + "" + " time: " + weibo1.getTime() + " content : " + weibo1.getContent());

                }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return weibo_list;
    }
}









