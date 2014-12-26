/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Crawler;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Petr
 */
public class ESConnect {
	private Client client;
	private String indexName;
	private boolean isMapping;
		
	public ESConnect (String indexName){
			Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost",9300));
			this.client = client;
			this.indexName = indexName;
			if (!isIndex()){
				this.isMapping = false;
			}
	}

	public void postElasticSearch(JSONObject jsn) throws Exception{
		jsn.put("source", "Web");
		if (!isIndex()) throw new Exception("Pro tento index neexistuje mapping");
		client.prepareIndex(this.indexName, jsn.get("type").toString(), jsn.get("id").toString())
									.setSource(jsn).execute().actionGet();
	}
	
	public void postElasticSearch(List<JSONObject> jsonList) throws Exception{
		if (!isIndex()) throw new Exception("Pro tento index neexistuje mapping");
		for (JSONObject jsnObj : jsonList){
			jsnObj.put("source", "Web");
			client.prepareIndex(this.indexName, jsnObj.get("type").toString(), jsnObj.get("id").toString())
									.setSource(jsnObj).execute().actionGet();
		}
	}
	
	public boolean isIndex(){
		final IndicesExistsResponse res = client.admin().indices().prepareExists(this.indexName).execute().actionGet();
		return res.isExists();
	}
	
	public void createIndex(){
		if (!isIndex()) {
			client.admin().indices().prepareCreate(this.indexName).execute().actionGet();
		}	
	}
		
	public void createMapping(String documentType, JSONObject mapping){
		try {
			PutMappingRequest pmr = new PutMappingRequest(this.indexName);
			pmr.source(mapping);
			pmr.type(documentType);
			//pmr.ignoreConflicts(true);
			client.admin().indices().putMapping(pmr).actionGet();		
			this.isMapping = true;
		} catch (Exception e) {
			Logger.getLogger(ESConnect.class.getName()).log(Level.SEVERE, null, e);
		}
	}
	
	public void createMapping(String documentType){
		createMapping(documentType, prepareMapping(documentType));
	}
	
	public void setIndexSettings(){
		try {
			JSONObject analyzer = getAnalyzer(this.indexName);
			CloseIndexRequest cir = new CloseIndexRequest(this.indexName);
			client.admin().indices().close(cir).actionGet();
			
			UpdateSettingsRequest usrq = new UpdateSettingsRequest(this.indexName);
			usrq.settings(analyzer);
			client.admin().indices().updateSettings(usrq).actionGet();
			
			OpenIndexRequest oir = new OpenIndexRequest(this.indexName);
			client.admin().indices().open(oir);
		} catch (Exception e) {
			Logger.getLogger(ESConnect.class.getName()).log(Level.SEVERE, null, e);
		}
	}
	
					
	public void endSession(){
		client.close();
	}
	
	public JSONObject prepareJsonForIndex(String user, Date dateTime, String message, String id, String source, String url) {
		Format f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		JSONObject lineJS = new JSONObject();
		lineJS.put("message", message);
		lineJS.put("userId", user);
		lineJS.put("userName", user);
		lineJS.put("userName.raw", user);
		lineJS.put("created", f.format(dateTime));
		lineJS.put("level", 0);
		lineJS.put("url", url);
		lineJS.put("type", "discussion");
		lineJS.put("page", source);
		lineJS.put("page.raw", source);
		lineJS.put("id",  id);

		return lineJS;
	}
	
		/**
	 * Metoda vytvori mapovani pro analyzer k indexu a typu, ktery je specifikovan
	 * 
	 * @param index mapovani, ktere ma byt vytvoren
	 * @param typ k namapovani
	 */
	public JSONObject prepareMapping(String typ) {
		//String mappingBody = "{\"properties\": {\"czech\": {\"type\":\"string\",\"analyzer\": \"czech\"}}}";

		JSONObject types = new JSONObject();
			JSONObject message = new JSONObject();	
			message.put("type", "string");
			message.put("analyzer", "czech");
		types.put("message", message);

			JSONObject userId = new JSONObject();
			userId.put("type", "string");
		types.put("userId", userId);	

			JSONObject userName = new JSONObject();
			userName.put("type", "string");
		types.put("userName", userName);	

			JSONObject userNameRaw = new JSONObject();
			userNameRaw.put("type", "string");
			userNameRaw.put("index" , "not_analyzed");
		types.put("userName.raw", userNameRaw);			
		
			JSONObject created = new JSONObject();
			created.put("format", "yyyy-MM-dd HH:mm:ss");
			created.put("type", "date");
		types.put("created", created);	

			JSONObject postId = new JSONObject();
			postId.put("type", "string");
		types.put("postId", postId);	

			JSONObject likes = new JSONObject();
			likes.put("type", "string");
		types.put("likes", likes);	

			JSONObject page = new JSONObject();
			page.put("type", "string");
		types.put("page", page);	

			JSONObject pageRaw = new JSONObject();
			pageRaw.put("type", "string");
			pageRaw.put("index" , "not_analyzed");
		types.put("page.raw", pageRaw);	
		
			JSONObject id = new JSONObject();
			id.put("type", "string");
		types.put("id", id);	
		
			JSONObject url = new JSONObject();
			url.put("type", "string");
		types.put("url", url);	
		
			JSONObject urlRaw = new JSONObject();
			urlRaw.put("type", "string");
			urlRaw.put("index" , "not_analyzed");
		types.put("url.raw", urlRaw);	

			JSONObject level = new JSONObject();
			level.put("type", "long");
		types.put("level", level);	
		
		JSONObject mappingBody = new JSONObject();
		mappingBody.put("properties", types);
		
		return mappingBody;
	}
	
		/**
	 * Metoda vytvori index v ES
	 * 
	 * @param index nazev indexu, ktery ma byt vytvoren
	 */
	public JSONObject getAnalyzer(String index) {
		String analyzer = "{\"settings\": {\"analysis\": {\"filter\": {\"czech_stop\": {\"type\": \"stop\",\"stopwords\":  \"_czech_\"},\"czech_keywords\": {\"type\":       \"keyword_marker\",\"keywords\":   [\"x\"]}, \"czech_stemmer\": { \"type\":       \"stemmer\", \"language\":   \"czech\"}},\"analyzer\": {\"czech\": {\"tokenizer\":  \"standard\",\"filter\": [ \"lowercase\",\"czech_stop\", \"czech_keywords\", \"czech_stemmer\"]}}}}}";
		//String analyzer = "{\"analysis\": {\"filter\": {\"czech_stop\": {\"type\": \"stop\",\"stopwords\":  \"_czech_\"},\"czech_keywords\": {\"type\":       \"keyword_marker\",\"keywords\":   [\"x\"]}, \"czech_stemmer\": { \"type\":       \"stemmer\", \"language\":   \"czech\"}},\"analyzer\": {\"czech\": {\"tokenizer\":  \"standard\",\"filter\": [ \"lowercase\",\"czech_stop\", \"czech_keywords\", \"czech_stemmer\"]}}}}";
		JSONParser parser = new JSONParser();		
	  Object obj = null;
		try {
			obj = parser.parse(analyzer);
		} catch (ParseException ex) {
			Logger.getLogger(ESConnect.class.getName()).log(Level.SEVERE, null, ex);
		}
		JSONObject jsonObject = (JSONObject) obj;
		return jsonObject;
	}
}
