package iii.cb102.nolawu;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Insertproducts {

	public static void main(String[] args) throws IOException {
		final int batchSize = 100;
		int count = 0;
		File file=new File("res/products.csv");
		Connection conn = null;
		OutputStream f = new FileOutputStream("res/log.txt");//log
		OutputStreamWriter writer = new OutputStreamWriter(f);
		try {     
			String connUrl = "jdbc:mysql://localhost:3306/kaggle?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
			conn = DriverManager.getConnection(connUrl, "root", "pwd");
			conn.setAutoCommit(false); // auto-commit 
			try {
				BufferedReader in=new BufferedReader(new FileReader(file));
				StringBuffer sb=new StringBuffer(256);
				String str=null;
				int step=0;
				String insStmt = "INSERT INTO products VALUES (?, ?, ?, ?)";
				PreparedStatement pstmt = conn.prepareStatement(insStmt);
				while((str=in.readLine())!=null) {
					sb.append(str).append("\n");			
					if(step!=0) {
						String[] split_line = str.split(","); 
						for(int i=0;i<split_line.length;i++) { 
							pstmt.setString(i+1, split_line[i]);								
						}
						pstmt.addBatch(); 
						if(count%batchSize==0) {
							pstmt.executeBatch();
						}
					}
					step++;
					count++;					
					System.out.println(count+" Success");
					System.out.println(" ---------------------- ");
				}		
				pstmt.executeBatch();
				in.close();
				conn.commit(); 
			}catch(IOException e) {
				String errmessage=e.getMessage();
				writer.append(errmessage);
				writer.append("\r\n");

				System.err.println("Error:  "+count);}
		} catch (SQLException e) {
			String errmessage=e.getMessage();
			writer.append(errmessage);
			writer.append("\r\n");
			System.err.println("Error:  "+count);
			writer.close();
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch(SQLException e) { 
					e.printStackTrace();
				}		
		}
		System.out.print("~~~Success~~~");
	}

}