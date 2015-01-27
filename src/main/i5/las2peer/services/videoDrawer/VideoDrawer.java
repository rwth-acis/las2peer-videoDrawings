package i5.las2peer.services.videoDrawer;

import i5.las2peer.api.Service;
import i5.las2peer.restMapper.HttpResponse;
import i5.las2peer.restMapper.MediaType;
import i5.las2peer.restMapper.RESTMapper;
import i5.las2peer.restMapper.annotations.ContentParam;
import i5.las2peer.restMapper.annotations.DELETE;
import i5.las2peer.restMapper.annotations.GET;
import i5.las2peer.restMapper.annotations.HeaderParam;
import i5.las2peer.restMapper.annotations.HttpHeaders;
import i5.las2peer.restMapper.annotations.POST;
import i5.las2peer.restMapper.annotations.Path;
import i5.las2peer.restMapper.annotations.PathParam;
import i5.las2peer.restMapper.annotations.Produces;
import i5.las2peer.restMapper.annotations.QueryParam;
import i5.las2peer.restMapper.annotations.Version;
import i5.las2peer.restMapper.annotations.swagger.ApiInfo;
import i5.las2peer.restMapper.annotations.swagger.ApiResponses;
import i5.las2peer.restMapper.annotations.swagger.ApiResponse;
import i5.las2peer.restMapper.annotations.swagger.Notes;
import i5.las2peer.restMapper.annotations.swagger.ResourceListApi;
import i5.las2peer.restMapper.annotations.swagger.Summary;
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
 * This is a video drawing service. This service will post/get/delete drawings from the user frontend to
 * the database. 
 * 
 */
@Path("video-drawing")
@Version("0.1")
@ApiInfo(
	title="Video drawing service",
	description="<p>A RESTful service for saving free drawing annotations of the videos.</p>",
	termsOfServiceUrl="",
	contact="bbakiu@dbis.rwth-aachen.de",
	license="",
	licenseUrl=""
)
public class VideoDrawer extends Service {

	private String jdbcDriverClassName;
	private String jdbcLogin;
	private String jdbcPass;
	private String jdbcUrl;
	private String jdbcSchema;
	private DatabaseManager dbm;
	
	private String epUrl;
	
	public VideoDrawer() {
		// read and set properties values
		// IF THE SERVICE CLASS NAME IS CHANGED, THE PROPERTIES FILE NAME NEED TO BE CHANGED TOO!
		setFieldValues();
		
		
		 if(!epUrl.endsWith("/")){
			 epUrl += "/";
			 }
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
	@Produces(MediaType.TEXT_HTML)
	@Path("validation")
	@ResourceListApi(description = "Check the user")
	@Summary("Return a greeting for the logged in user")
	@Notes("This is an example method")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "The user is logged in"),
	})
	public HttpResponse validateLogin() 
				{
		String returnString = "";
		returnString += "You are " + ((UserAgent) getActiveAgent()).getLoginName() + " and your login is valid!";
		HttpResponse res = new HttpResponse(returnString);
		res.setStatus(200);
		return res;
	}
	
	
	/*
	 * Posts a drawing in the database. The String data is a JSON string which contains
	 * the drawing JSON from FabricJS, the time of the video when this drawing is created, 
	 * the duration for this annotation and the url of the video.
	 */
	@POST
	@Path("drawings")
	@Summary("Insert new annotation")
	@Notes("Requires authentication.")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Video annotation saved successfully."),
	@ApiResponse(code = 401, message = "User is not authenticated."),
	@ApiResponse(code = 409, message = "Annotations already exists."),
	@ApiResponse(code = 500, message = "Internal error.")
	})
	public HttpResponse postDrawing(@ContentParam String data)
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
		if(getActiveAgent().getId() != getActiveNode().getAnonymous().getId()){
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
		} else {
			result = "User in not authenticated";
			// return
			HttpResponse r = new HttpResponse(result);
			r.setStatus(401);
			return r;
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
	 * Updates a drawing in the database. 
	 * @param drawingIdPost The Id of the drawing that will be updated.
	 * @param data The JSON with the updated data.
	 * @return a successful or failed update.
	 */
	
	@POST
	@Path("drawings/{drawingId}")
	@Summary("Update an annotation")
	@Notes("Requires authentication.")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Video annotation updated successfully."),
	@ApiResponse(code = 401, message = "User is not authenticated."),
	@ApiResponse(code = 404, message = "Annotation not found"),
	@ApiResponse(code = 500, message = "Internal error.")
	})
	public HttpResponse updateDrawing(@PathParam("drawingId") int drawingIdPost,@ContentParam String data) {
		
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
			if(getActiveAgent().getId() != getActiveNode().getAnonymous().getId()){
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
			} else {
				result = "User in not authenticated";
				// return
				HttpResponse r = new HttpResponse(result);
				r.setStatus(401);
				return r;
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
	 * Gets all the annotations (drawings) for a video.	
	 * @param videoUrlJson the URL of the video, but it is encoded in a JSON string. This is done to avoid requests that
	 * have as a extension something like /http://www.google.com/video?id=100
	 * @return
	 */
	@GET
	@Path("drawings/{videoUrl}")
	@ResourceListApi(description = "Return annotations for a selected video")
	@Summary("return a JSON with video annotations stored for the given VideoUrl")
	@Notes("Return all annotations in JSON with Id, time and duration")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Video annotations"),
	@ApiResponse(code = 404, message = "Video url does not exist"),
	@ApiResponse(code = 500, message = "Internal error"),
	})
	public HttpResponse getVideoDrawings(@PathParam("videoUrl") String videoUrlJson) {
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
	/**
	 * Gets all annotations in the database
	 * @return
	 */
	@GET
	@Path("drawings")
	@ResourceListApi(description = "Return annotations for all videos")
	@Summary("return a JSON with video annotations stored for all videos")
	@Notes("Notes")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Video annotation"),
	@ApiResponse(code = 404, message = "Nothing exists"),
	@ApiResponse(code = 500, message = "Internal error"),
	})
	public HttpResponse getAllDrawings() {
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
	
	/**
	 * Deletes one annotation from the database.
	 * @param drawingIdPost the id of the annotation to be deleted.
	 * @return
	 */
	@DELETE
	@Path("drawings/{drawingId}")
	@Summary("Delete video annotation")
	@Notes("Requires authentication.")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Video annotation deleted successfully."),
	@ApiResponse(code = 401, message = "User is not authenticated."),
	@ApiResponse(code = 404, message = "Video annotation not found."),
	@ApiResponse(code = 500, message = "Internal error.")
	})
	public HttpResponse deleteDrawing(@PathParam("drawingId") int drawingIdPost) {
		
		String result = "";
		Connection conn = null;
		PreparedStatement stmnt = null;
		ResultSet rs = null;
		try {
			if(getActiveAgent().getId() != getActiveNode().getAnonymous().getId()){
				conn = dbm.getConnection();
				stmnt = conn.prepareStatement("DELETE FROM drawings WHERE drawingId = ?;");
				stmnt.setInt(1, drawingIdPost);
				int rows = stmnt.executeUpdate(); // same works for insert
				result = "Database delete. " + rows + " rows affected";
				
				// return 
				HttpResponse r = new HttpResponse(result);
				r.setStatus(200);
				return r;
			} else {
				result = "User in not authenticated";
				// return
				HttpResponse r = new HttpResponse(result);
				r.setStatus(401);
				return r;
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
	


	// ================= Swagger Resource Listing & API Declarations =====================
	@GET
	@Path("api-docs")
	@Summary("retrieve Swagger 1.2 resource listing.")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Swagger 1.2 compliant resource listing"),
	@ApiResponse(code = 404, message = "Swagger resource listing not available due to missing annotations."),
	})
	@Produces(MediaType.APPLICATION_JSON)
	public HttpResponse getSwaggerResourceListing(){
	return RESTMapper.getSwaggerResourceListing(this.getClass());
	}
	@GET
	@Path("api-docs/{tlr}")
	@Produces(MediaType.APPLICATION_JSON)
	@Summary("retrieve Swagger 1.2 API declaration for given top-level resource.")
	@ApiResponses(value={
	@ApiResponse(code = 200, message = "Swagger 1.2 compliant API declaration"),
	@ApiResponse(code = 404, message = "Swagger API declaration not available due to missing annotations."),
	})
	public HttpResponse getSwaggerApiDeclaration(@PathParam("tlr") String tlr){
	return RESTMapper.getSwaggerApiDeclaration(this.getClass(),tlr, epUrl);
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
