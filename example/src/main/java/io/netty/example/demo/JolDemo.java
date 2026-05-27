package io.netty.example.demo;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.vm.VM;

import java.util.HashMap;
import java.util.Map;

/**
 * 打印对象布局信息 类的静态布局  数组对象布局
 */
public class JolDemo {

    static class ParentClass{
        protected int prentInt;
        protected String prentString;
        protected long prentLong;

    }
    static class ChildClass extends ParentClass{
        private int childInt;
        private String childString;
        private long childLong;
    }


    public static void main(String[] args) {
        // 打印当前JVM信息(版本、位数、内存模型等)
        System.out.println(VM.current().details());
        System.out.println("================================================");
        //基础类型包装类的内存布局
        Integer intObj = 123;
        String integerLayOut = ClassLayout.parseInstance(intObj).toPrintable();
        System.out.println("基础类型包装类的内存布局"+integerLayOut);
        System.out.println("================================================");
        // 自定义类(含继承)的内存布局
        ChildClass childObj = new ChildClass();
        childObj.prentInt = 123;
        childObj.prentString = "hello";
        childObj.prentLong = 123;
        childObj.childInt = 123123;
        childObj.childString = "helloChild";
        childObj.childLong = 123123123;
        String childLayOut = ClassLayout.parseInstance(childObj).toPrintable();
        System.out.println("自定义类(含继承)的内存布局"+childLayOut);
        System.out.println("================================================");
        // 计算对象的浅大小(自身占用内存)
        long shallowSize = ClassLayout.parseInstance(childObj).instanceSize();
        System.out.println("对象的浅大小"+shallowSize);
        System.out.println("================================================");
        // 计算对象的深大小
        long deepSize = GraphLayout.parseInstance(childLayOut).totalSize();
        System.out.println("对象的深大小"+deepSize);
        System.out.println("================================================");
        // 类的静态布局
        System.out.println("类的静态布局"+ClassLayout.parseClass(ChildClass.class).toPrintable());
        System.out.println("================================================");
        // 赋值对象的深大小与布局
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("key1", 123);
        map.put("key2", "hello");
        map.put("key3", new ChildClass());
        long mapDeepSize = GraphLayout.parseInstance(map).totalSize();
        System.out.println("复杂对象的深大小"+mapDeepSize);
        System.out.println("复杂对象布局"+ClassLayout.parseInstance(map).toPrintable());

        /**
         * 自定义类(含继承)的内存布局io.netty.example.echo.JolDemo$ChildClass object internals:
         * OFF  SZ               TYPE DESCRIPTION               VALUE
         *   0   8                    (object header: mark)     0x0000000000000005 (biasable; age: 0)
         *   8   4                    (object header: class)    0xf801695a
         *  12   4                int ParentClass.prentInt      123
         *  16   8               long ParentClass.prentLong     123
         *  24   4   java.lang.String ParentClass.prentString   (object)
         *  28   4                int ChildClass.childInt       123123
         *  32   8               long ChildClass.childLong      123123123
         *  40   4   java.lang.String ChildClass.childString    (object)
         *  44   4                    (object alignment gap)    这里是因为对象大小必须是8字节的倍数，这里对齐填充
         * Instance size: 48 bytes
         * Space losses: 0 bytes internal + 4 bytes external = 4 bytes total
         */

        // OFF 字段在对象内存中的偏移量(字节)，从0开始计数
        // SZ  该字段占用的字节数
        // TYPE  字段的类型
        // DESCRIPTION 字段所属类+字段名
        // value 字段当前值
    }
}
