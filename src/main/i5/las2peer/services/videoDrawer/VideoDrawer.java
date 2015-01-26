package i5.las2peer.services.videoDrawer;

import i5.las2peer.api.Service;
import i5.las2peer.restMapper.HttpResponse;
import i5.las2peer.restMapper.RESTMapper;
import i5.las2peer.restMapper.annotations.ContentParam;
import i5.las2peer.restMapper.annotations.DELETE;
import i5.las2peer.restMapper.annotations.GET;
import i5.las2peer.restMapper.annotations.HeaderParam;
import i5.las2peer.restMapper.annotations.HttpHeaders;
import i5.las2peer.restMapper.annotations.POST;
import i5.las2peer.restMapper.annotations.Path;
import i5.las2peer.restMapper.annotations.PathParam;
import i5.las2peer.restMapper.annotations.QueryParam;
import i5.las2peer.restMapper.annotations.Version;
import i5.las2peer.restMapper.tools.ValidationResult;
import i5.las2peer.restMapper.tools.XMLCheck;
import i5.las2peer.security.Context;
import i5.las2peer.security.UserAgent;
import i5.las2peer.services.videoDrawer.database.DatabaseManager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.ParseException;










import com.mysql.jdbc.ResultSetMetaData;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 * LAS2peer Service
 * 
 * This is a template for a very basic LAS2peer service
 * that uses the LAS2peer Web-Connector for RESTful access to it.
 * 
 */
@Path("video-drawing")
@Version("0.1")
public class VideoDrawer extends Service {

	private String jdbcDriverClassName;
	private String jdbcLogin;
	private String jdbcPass;
	private String jdbcUrl;
	private String jdbcSchema;
	private DatabaseManager dbm;

	public VideoDrawer() {
		// read and set properties values
		// IF THE SERVICE CLASS NAME IS CHANGED, THE PROPERTIES FILE NAME NEED TO BE CHANGED TOO!
		setFieldValues();
		// instantiate a database manager to handle database connection pooling and credentials
		dbm = new DatabaseManager(jdbcDriverClassName, jdbcLogin, jdbcPass, jdbcUrl, jdbcSchema);
	}

	/**
	 * Simple function to validate a user login.
	 * Basically it only serves as a "calling point" and does not really validate a user
	 * (since this is done previously by LAS2peer itself, the user does not reach this method
	 * if he or she is not authenticated).
	 * 
	 */
	@GET
	@Path("validation")
	public HttpResponse validateLogin() 
				{
		String returnString = "";
		returnString += "You are " + ((UserAgent) getActiveAgent()).getLoginName() + " and your login is valid!";
		HttpResponse res = new HttpResponse(returnString);
		res.setStatus(200);
		return res;
	}
	
	
	/*
	 * posts a video into database
	 */
	@POST
	@Path("drawings")
	public HttpResponse postVideo(@ContentParam String data)
	{

		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		try {
			
			JSONObject o;
			try{	
				o = (JSONObject) JSONValue.parseWithException(data);
			} catch (ParseException e1) {
				throw new IllegalArgumentException("data is not valid JSON!");
			}
			for (Object key: o.keySet()){
				result+= key + " " + o.get(key);
			}
			
			// get connection from connection pool
			conn = dbm.getConnection();
			
			// prepare statement
			stmnt = conn.prepareStatement("INSERT INTO drawings( drawing, time, duration, videoUrl) VALUES (?,?,?,?);");
			stmnt.setString( 1, (String) o.get("drawing") );
			stmnt.setDouble( 2, (double) o.get("time"));
		    stmnt.setInt( 3, (int) o.get("duration"));
		    stmnt.setString( 4, (String) o.get("videoUrl") );
			
		    int rows = stmnt.executeUpdate(); // same works for insert
			result = "Database insert. " + rows + " rows affected";
			
			// return 
			HttpResponse r = new HttpResponse(result);
			r.setStatus(200);
			return r;
			
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
		
	}
	
	
	
	@POST
	@Path("drawings/{drawingId}")
	public HttpResponse updateVideo(@PathParam("drawingId") int drawingIdPost,@ContentParam String data) {
		
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		String updateStr="";
		try {
			JSONObject o;
			try{	
				o = (JSONObject) JSONValue.parseWithException(data);
			} catch (ParseException e1) {
				throw new IllegalArgumentException("data is not valid JSON!");
			}
			conn = dbm.getConnection();
									
			for (Object key: o.keySet()){
				
				if(updateStr.equals("")){
					updateStr+= key + "  =  '" + o.get(key) +  "' ";
				}else{
					updateStr+= ", " + key + "  =  '" + o.get(key)  +  "' ";
				}
			}
			conn = dbm.getConnection();
			stmnt = conn.prepareStatement("UPDATE drawings SET " + updateStr + "  WHERE drawingId = " + drawingIdPost + ";");
			int rows = stmnt.executeUpdate(); // same works for insert
			result = "Database updated. " + rows + " rows affected";
			
			// return 
			HttpResponse r = new HttpResponse(result);
			r.setStatus(200);
			return r;
			
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
		
	}
	
	@GET
	@Path("drawings/{videoUrl}")
	public HttpResponse getVideo(@PathParam("videoUrl") String videoUrlJson) {
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		int columnCount;
		ResultSetMetaData rsmd = null;
		String columnName;
		try {
			JSONObject o;
			try{	
				o = (JSONObject) JSONValue.parseWithException(videoUrlJson);
			} catch (ParseException e1) {
				throw new IllegalArgumentException("data is not valid JSON!");
			}
			// get connection from connection pool
			conn = dbm.getConnection();
			
			// prepare statement
			stmnt = conn.prepareStatement("SELECT  drawingId, time, duration, videoUrl, drawing FROM drawings WHERE videoUrl = ?;");
			stmnt.setString( 1, (String) o.get("videoUrl") );
			
			// retrieve result set
			rs = stmnt.executeQuery();
			rsmd = (ResultSetMetaData) rs.getMetaData(); 
			columnCount = rsmd.getColumnCount();
			
			// process result set
			JSONArray rset = new JSONArray();
			if (rs.next()) {
				JSONObject row = new JSONObject();
				for(int i=1;i<=columnCount;i++) { 
					result = rs.getString(i); 
					columnName = rsmd.getColumnName(i);
					// setup resulting JSON Object 
					row.put(columnName, result); 
					}
				rset.add(row);
				
				while (rs.next()) {
					
					JSONObject ro = new JSONObject();
					for(int i=1;i<=columnCount;i++){ 
						result = rs.getString(i); 
						columnName = rsmd.getColumnName(i);
						// setup resulting JSON Object 
						ro.put(columnName, result); 
						}
					rset.add(ro);
				}
				
				
				// return HTTP Response on success
				HttpResponse r = new HttpResponse(rset.toJSONString());
				r.setStatus(200);
				return r;
			}
			 else {
				result = "No result for the query " + videoUrlJson;
				
				// return HTTP Response on error
				HttpResponse er = new HttpResponse(result);
				er.setStatus(404);
				return er;
			}
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
	
	}

	@GET
	@Path("drawings")
	public HttpResponse getAllVideo() {
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		int columnCount;
		ResultSetMetaData rsmd = null;
		String columnName;
		try {
			// get connection from connection pool
			conn = dbm.getConnection();
			
			// prepare statement
			stmnt = conn.prepareStatement("SELECT drawingId, videoUrl, drawing, time, duration FROM drawings;");

			// retrieve result set
			rs = stmnt.executeQuery();
			rsmd = (ResultSetMetaData) rs.getMetaData(); 
			columnCount = rsmd.getColumnCount();
			
			// process result set
			JSONArray rset = new JSONArray();
			if (rs.next()) {
				JSONObject row = new JSONObject();
				for(int i=1;i<=columnCount;i++) { 
					result = rs.getString(i); 
					columnName = rsmd.getColumnName(i);
					// setup resulting JSON Object 
					row.put(columnName, result); 
					}
				rset.add(row);
				
				while (rs.next()) {
					
					JSONObject ro = new JSONObject();
					for(int i=1;i<=columnCount;i++){ 
						result = rs.getString(i); 
						columnName = rsmd.getColumnName(i);
						// setup resulting JSON Object 
						ro.put(columnName, result); 
						}
					rset.add(ro);
				}
				
				// return HTTP Response on success
				HttpResponse r = new HttpResponse(rset.toJSONString());
				r.setStatus(200);
				return r;
			}
			 else {
				result = "No result for the query " ;
				
				// return HTTP Response on error
				HttpResponse er = new HttpResponse(result);
				er.setStatus(404);
				return er;
			}
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
	
	}
	@DELETE
	@Path("drawings/{drawingId}")
	public HttpResponse deleteVideo(@PathParam("drawingId") int drawingIdPost) {
		
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		try {
			conn = dbm.getConnection();
			stmnt = conn.prepareStatement("DELETE FROM drawings WHERE drawingId = ?;");
			stmnt.setInt(1, drawingIdPost);
			int rows = stmnt.executeUpdate(); // same works for insert
			result = "Database delete. " + rows + " rows affected";
			
			// return 
			HttpResponse r = new HttpResponse(result);
			r.setStatus(200);
			return r;
			
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
		
	}
	

	/**
	 * Another example method.
	 * 
	 * @param myInput
	 * 
	 */
	@POST
	@Path("myResourcePath/{input}")
	public HttpResponse exampleMethod(@PathParam("input") String myInput) {
		String returnString = "";
		returnString += "You have entered " + myInput + "!";
		HttpResponse res = new HttpResponse(returnString);
		res.setStatus(200);
		return res;
		
	}

	/**
	 * Example method that shows how to retrieve a user email address from a database 
	 * and return an HTTP response including a JSON object.
	 * 
	 * WARNING: THIS METHOD IS ONLY FOR DEMONSTRATIONAL PURPOSES!!! 
	 * IT WILL REQUIRE RESPECTIVE DATABASE TABLES IN THE BACKEND, WHICH DON'T EXIST IN THE TEMPLATE.
	 * 
	 */
	@GET
	@Path("userEmail/{username}")
	public HttpResponse getUserEmail(@PathParam("username") String username) {
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		try {
			// get connection from connection pool
			conn = dbm.getConnection();
			
			// prepare statement
			stmnt = conn.prepareStatement("SELECT email FROM users WHERE username = ?;");
			stmnt.setString(1, username);
			
			// retrieve result set
			rs = stmnt.executeQuery();
			
			// process result set
			if (rs.next()) {
				result = rs.getString(1);
				
				// setup resulting JSON Object
				JSONObject ro = new JSONObject();
				ro.put("email", result);
				
				// return HTTP Response on success
				HttpResponse r = new HttpResponse(ro.toJSONString());
				r.setStatus(200);
				return r;
				
			} else {
				result = "No result for username " + username;
				
				// return HTTP Response on error
				HttpResponse er = new HttpResponse(result);
				er.setStatus(404);
				return er;
			}
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
	}

	/**
	 * Example method that shows how to change a user email address in a database.
	 * 
	 * WARNING: THIS METHOD IS ONLY FOR DEMONSTRATIONAL PURPOSES!!! 
	 * IT WILL REQUIRE RESPECTIVE DATABASE TABLES IN THE BACKEND, WHICH DON'T EXIST IN THE TEMPLATE.
	 * 
	 */
	@POST
	@Path("userEmail/{username}/{email}")
	public HttpResponse setUserEmail(@PathParam("username") String username, @PathParam("email") String email) {
		
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		try {
			conn = dbm.getConnection();
			stmnt = conn.prepareStatement("UPDATE users SET email = ? WHERE username = ?;");
			stmnt.setString(1, email);
			stmnt.setString(2, username);
			int rows = stmnt.executeUpdate(); // same works for insert
			result = "Database updated. " + rows + " rows affected";
			
			// return 
			HttpResponse r = new HttpResponse(result);
			r.setStatus(200);
			return r;
			
		} catch (Exception e) {
			// return HTTP Response on error
			HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
			er.setStatus(500);
			return er;
		} finally {
			// free resources if exception or not
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (stmnt != null) {
				try {
					stmnt.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					Context.logError(this, e.getMessage());
					
					// return HTTP Response on error
					HttpResponse er = new HttpResponse("Internal error: " + e.getMessage());
					er.setStatus(500);
					return er;
				}
			}
		}
	}

	/**
	 * Method for debugging purposes.
	 * Here the concept of restMapping validation is shown.
	 * It is important to check, if all annotations are correct and consistent.
	 * Otherwise the service will not be accessible by the WebConnector.
	 * Best to do it in the unit tests.
	 * To avoid being overlooked/ignored the method is implemented here and not in the test section.
	 * @return  true, if mapping correct
	 */
	public boolean debugMapping() {
		String XML_LOCATION = "./restMapping.xml";
		String xml = getRESTMapping();

		try {
			RESTMapper.writeFile(XML_LOCATION, xml);
		} catch (IOException e) {
			e.printStackTrace();
		}

		XMLCheck validator = new XMLCheck();
		ValidationResult result = validator.validate(xml);

		if (result.isValid())
			return true;
		return false;
	}

	/**
	 * This method is needed for every RESTful application in LAS2peer. There is no need to change!
	 * 
	 * @return the mapping
	 */
	public String getRESTMapping() {
		String result = "";
		try {
			result = RESTMapper.getMethodsAsXML(this.getClass());
		} catch (Exception e) {

			e.printStackTrace();
		}
		return result;
	}

}
