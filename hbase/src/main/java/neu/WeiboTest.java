package neu;

import java.io.IOException;

public class WeiboTest {
    public static void main(String[] args){
        Service.createNamespace(Constant.NAME_SPACE);
        Service.createTable(Constant.EBOX_TABLE,Constant.CF1);
        Service.createTable(Constant.CONTENT_TABLE,Constant.CF1);
        Service.createTable(Constant.RELATION_TABLE,Constant.CF1,Constant.CF2);
     Service.addFocus("111","222");
        Service.deleteFocus("111","222");
       Service.uploadWeibo("222","东北大学");
      Service.getUserWeiboList("111");
    }

}
