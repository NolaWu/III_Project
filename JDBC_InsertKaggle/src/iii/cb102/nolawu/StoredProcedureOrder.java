package iii.cb102.nolawu;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.*;
public class StoredProcedureOrder {
	public static void main(String[] args) throws IOException {
		Connection conn = null;
		OutputStream f = new FileOutputStream("res/log.txt"); //log
		OutputStreamWriter writer = new OutputStreamWriter(f);
		try {     
			String connUrl = "jdbc:mysql://localhost:3306/kaggle?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
			conn = DriverManager.getConnection(connUrl, "root", "pwd");
			conn.setAutoCommit(false); //open auto-commit 
			final int batchSize = 10000;
			int count = 0;	
			File file=new File("res/orders.csv");
			CallableStatement cstmt = conn.prepareCall("{call insert_order(?,?,?,?,?,?,?)}");
			BufferedReader in=new BufferedReader(new FileReader(file));
			StringBuffer sb=new StringBuffer(256);
			String str=null;
			int step=0;
			try {
				while((str=in.readLine())!=null) {				
					sb.append(str).append("\n");			
					if(step!=0) {
						String[] split_line = str.split(",");
						for(int i=0;i<split_line.length;i++) { 
							cstmt.setString(i, split_line[i]);								
						}
						if(split_line.length<7) {
							cstmt.setString(7, null);
						}
						cstmt.addBatch();
						if(count%batchSize==0) {
							cstmt.executeBatch();
							conn.commit(); 
							System.out.println(count+" Success");
						}
					}
					step++;
					count++;					
					
				}
				cstmt.executeBatch();
				System.out.println(count+" Success");
				System.out.println(" ---------------------- ");
				in.close();
				conn.commit(); //close auto-commit
			} catch (IOException e) {
				String errmessage=e.getMessage();
				writer.append(errmessage);
				writer.append("\r\n");
				System.err.println(count);
		
				}							
		} catch (SQLException | FileNotFoundException e) {
			String errmessage=e.getMessage();
			writer.append(errmessage);
			writer.append("\r\n");
			writer.close();
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch(SQLException e) { 
					e.printStackTrace();
				}
		}
		System.out.println("Success");
	}
}
