package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import org.apache.commons.io.IOUtils;

public class HttpConnectHelper {
	
	
	public void feedback(String path) throws IOException{
		 URL url = new URL(path);
		 HttpURLConnection conn = null;
		 conn = (HttpURLConnection) url.openConnection();
		 conn.setRequestProperty("Content-Type", "text/xml;charset=utf-8");
		 conn.setRequestProperty("Cache-Control", "no-cache");
		 conn.setRequestProperty("Connection", "close");
		 conn.setConnectTimeout(60000);
		 conn.getInputStream();
	}
	
	public String getInfo(String method, String requestUrl, Map<String, String> headers,String sign){
		HttpURLConnection conn = null;  
        InputStream in = null;  
        String result = null;  
        try {  
            URL url = new URL(requestUrl);  
            conn = (HttpURLConnection) url.openConnection();  
//            conn.setRequestProperty("sign", sign);
            
            conn.setRequestMethod(method);  
            conn.setDoInput(true);  
            conn.setDoOutput(true);  
            if (headers != null) {  
                for (Map.Entry<String, String> entry : headers.entrySet()) {  
                    conn.addRequestProperty(entry.getKey(), URLEncoder.encode(entry.getValue(), "UTF-8"));  
                }  
            }  
            int status = conn.getResponseCode();  
            System.out.println("Response status: " + status);  
            in = conn.getErrorStream();  
            if (in != null) {  
                result = IOUtils.toString(in);  
            }  
            in = conn.getInputStream();  
            if (in != null) {  
                result = IOUtils.toString(in);  
            }  
        } catch (Exception e) {  
            System.err.println(e.getMessage());  
        } finally {  
            IOUtils.closeQuietly(in);  
        }  
        return result;  
	}

}
