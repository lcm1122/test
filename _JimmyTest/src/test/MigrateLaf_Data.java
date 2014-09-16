package test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MigrateLaf_Data {
	
	String dbUrl_From="jdbc:oracle:thin:@192.168.169.204:1521:LAFORA";
	String dbUid_From="LAFDBUSER";
	String dbPwd_From="Laf1688";
	
	String dbUrl_To="jdbc:oracle:thin:@JimmyLiu-NB:1521:OCTON";
	String dbUid_To="LAFDBUSER";
	String dbPwd_To="laf1688";
	
	String[] tablesAry={	
		
	  "CONFIRM_FILE",
	  "DEPT_INFO",
	  "LAWYER_INFO",
	  "LAWYER_INFO_SYNCSQLSERVER",
	  "LAWYER_LOGIN_INFO",
	  "MENU_PROG",
	  "PAYMENTTYPE_INFO",
	  "PLACE_INFO",
	  "PUBLISHMESSAGE",
	  "STATEMENT",
	  "STATEMENT_DETAIL",
	  "STATEMENT_DETAIL_YEAR",
	  "STATEMENT_YEAR",
	  
	  //"USER_EXEC_SQL_LOG",
	  //"USER_FUNC_ACCESS_LOG",
	  "USER_INFO"
	};
	
	Connection connFrom=null;
	Connection connTo=null;
   
	public Connection getConnFrom() throws Exception{
    	Class.forName("oracle.jdbc.driver.OracleDriver");
    	//Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
    	//System.out.println("222 jdbc type 4 direct connect to DB");
    	return  DriverManager.getConnection(dbUrl_From, dbUid_From, dbPwd_From);  		
	}
	
	public Connection getConnTo() throws Exception{
    	Class.forName("oracle.jdbc.driver.OracleDriver");
    	//Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
    	//System.out.println("222 jdbc type 4 direct connect to DB");
    	return  DriverManager.getConnection(dbUrl_To, dbUid_To, dbPwd_To);  		
	}	
	
	public void initConn() throws Exception{
		connFrom=this.getConnFrom();
		connTo=this.getConnTo();
	}
	
	public void closeConn(){
		try{
			if(connFrom!=null)  connFrom.close();
		}catch(Exception e){}
		try{
			if(connTo!=null)  connTo.close();
		}catch(Exception e){}		
	}
	
	public void copyDataByTable(String tableName){
		
		System.out.println("Table Name : "+tableName);
		
		PreparedStatement psmtFrom=null;
		ResultSet rsFrom=null;
		
		PreparedStatement psmtTo=null;
		
		String selectSql="select * from "+tableName;
		String insertSql=null;
		StringBuilder insertSqlSB=new StringBuilder();
		insertSqlSB.append("insert into ").append(tableName);
		insertSqlSB.append(" values(");
		
		List<String> columnTypeNamesList=new ArrayList<String>();
		
		try{
			connTo.setAutoCommit(false);
			
			String deleteSql="delete from "+tableName;
			psmtTo=connTo.prepareStatement(deleteSql);
			//psmtTo.executeUpdate();
			
			psmtFrom=connFrom.prepareStatement(selectSql);
			rsFrom=psmtFrom.executeQuery();
			int columnCount=0;
			int count=0;
			int insertCount=0;
			while(rsFrom.next()){
				count++;
				if(count==1){
				  ResultSetMetaData rsmd=rsFrom.getMetaData();
				  columnCount=rsmd.getColumnCount();				  
				  
				  for(int i=1;i<=columnCount;i++){
				    String columnTypeName=rsmd.getColumnTypeName(i);
				    int columnType=rsmd.getColumnType(i);
				    String columnName=rsmd.getColumnName(i);
				    int precision=rsmd.getPrecision(i);
				    int scale=rsmd.getScale(i);
				    //if("NUMBER".equalsIgnoreCase(columnTypeName)){
				    //  System.out.println("columnName="+columnName+", columnTypeName="+columnTypeName+", columnType="+columnType+", precision="+precision+", scale="+scale);
				    //}
				    //TIMESTAMP ,VARCHAR2, NUMBER, DATE
				    columnTypeNamesList.add(columnTypeName);
				    insertSqlSB.append("?");
				    if(i!=columnCount) insertSqlSB.append(", ");
				  }
				  insertSqlSB.append(" )");
				  insertSql=insertSqlSB.toString();
				  System.out.println("insertSql="+insertSql);
				  psmtTo=connTo.prepareStatement(insertSql);
				}
				
				for(int i=1;i<=columnCount;i++){
					String columnTypeName=columnTypeNamesList.get(i-1);
					
					
					if("TIMESTAMP".equalsIgnoreCase(columnTypeName)){						
						psmtTo.setTimestamp(i, rsFrom.getTimestamp(i));
						
					}
					if("DATE".equalsIgnoreCase(columnTypeName)){
						//rsFrom.getDate(i);
						psmtTo.setDate(i, rsFrom.getDate(i));
					}	
					if("NUMBER".equalsIgnoreCase(columnTypeName)){
						//rsFrom.getInt(i);
						psmtTo.setInt(i, rsFrom.getInt(i));
					}	
					if("VARCHAR2".equalsIgnoreCase(columnTypeName)){
						rsFrom.getString(i);
						psmtTo.setString(i, rsFrom.getString(i));
					}						
				} //for
				//insertCount+=psmtTo.executeUpdate();
			} //while
			connTo.commit();
			System.out.println("insertCount="+insertCount);
		}
		catch(Exception e){
			
			e.printStackTrace();
			if(e instanceof SQLException){
				try {
					connTo.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		
		
	}
	
	public void copyDatas(){
		for(int i=0;i<tablesAry.length;i++){
			copyDataByTable(tablesAry[i]);
		}
	}
	
	public static void main(String[] args) throws Exception {
		MigrateLaf_Data test=new MigrateLaf_Data();
		test.initConn();
		test.copyDatas();
		test.closeConn();
	}

}
