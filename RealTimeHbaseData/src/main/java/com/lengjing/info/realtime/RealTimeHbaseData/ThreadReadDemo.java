package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ThreadReadDemo {
	
	/**Java多线程读大文件 
     * @param args 
     */  
    public static void main(String[] args) {  
        Thread t1=new Thread(new MultiThread(),"A");  
        Thread t2=new Thread(new MultiThread(),"B");  
        Thread t3=new Thread(new MultiThread(),"C");  
        Thread t4=new Thread(new MultiThread(),"D");  
        Thread t5=new Thread(new MultiThread(),"E");  
        Thread t6=new Thread(new MultiThread(),"F");  
        Thread t7=new Thread(new MultiThread(),"G");  
        Thread t8=new Thread(new MultiThread(),"H");  
        Thread t9=new Thread(new MultiThread(),"I");  
        Thread t10=new Thread(new MultiThread(),"J");  
        /*Thread t11=new Thread(new MultiThread(),"K");  
        Thread t12=new Thread(new MultiThread(),"L");  
        Thread t13=new Thread(new MultiThread(),"M");  
        Thread t14=new Thread(new MultiThread(),"N");  
        Thread t15=new Thread(new MultiThread(),"O"); */
        t1.start();  
        t2.start(); 
        t3.start();
        t4.start();
        t5.start();
        t6.start();  
        t7.start(); 
        t8.start();
        t9.start();
        t10.start();
       /* t11.start();  
        t12.start(); 
        t13.start();
        t14.start();
        t15.start();*/
    }  
  
}  
  
  
 class MultiThread implements Runnable{   
    private static BufferedReader br = null;  
    private List<String> list;  
      
    static{ 
    		try {  
                br = new BufferedReader(new FileReader("d:\\yubing.lei\\Desktop\\hivetest\\9"),1000);  
            } catch (FileNotFoundException e) {  
                // TODO Auto-generated catch block  
                e.printStackTrace();  
            } 
    }  
      
    public void run() {  
        String line = null;  
        int count = 0;  
        while(true) {  
            this.list = new ArrayList<String>();  
            synchronized(br) {  
                try {  
                    while((line = br.readLine()) != null) {  
                        if(count<100) {  
                            list.add(line);  
                            count++;  
                        }else {  
                            list.add(line);  
                            count = 0;  
                            break;  
                        }  
                    }  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
            try {  
                Thread.sleep(1);  
                display(this.list);  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
            if(line == null)  
                break;  
        }  
          
          
    }  
      
    public void display(List<String> list) {  
        for(String str:list) {  
        	try {
				LoadHbaseData.loadData(str.replaceAll("\r", "").replaceAll("\n", ""));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
        }  
        //System.out.println(list.size());  
    }  

}
