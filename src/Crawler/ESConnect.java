/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Crawler;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.elasticsearch.index.Index;
import org.json.simple.JSONObject;

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
		if (this.isMapping == false) throw new Exception("Pro tento index neexistuje mapping");
		client.prepareIndex(this.indexName, jsn.get("type").toString(), jsn.get("id").toString())
									.setSource(jsn).execute().actionGet();
	}
	
	public void postElasticSearch(List<JSONObject> jsonList) throws Exception{
		if (this.isMapping == false) throw new Exception("Pro tento index neexistuje mapping");
		for (JSONObject jsnObj : jsonList){
			client.prepareIndex(this.indexName, jsnObj.get("type").toString(), jsnObj.get("id").toString())
									.setSource(jsnObj).execute().actionGet();
		}
	}
	
	public boolean isIndex(){
		final IndicesExistsResponse res = client.admin().indices().prepareExists(this.indexName).execute().actionGet();
		return res.isExists();
	}
	
	public void createIndex(){
		client.admin().indices().prepareCreate(this.indexName).execute().actionGet();
	}
		
	public void createMapping(String documentType, JSONObject mapping){
		try {
			PutMappingRequest pmr = new PutMappingRequest(this.indexName);
			pmr.source(mapping);
			pmr.type(documentType);
			client.admin().indices().putMapping(pmr).actionGet();		
			this.isMapping = true;
		} catch (Exception e) {
			Logger.getLogger(ESConnect.class.getName()).log(Level.SEVERE, null, e);
		}
	}
	
	public void setIndexSettings(JSONObject analyzer){
		try {
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
	
	public JSONObject prepareJsonForIndex(String user, Date dateTime, String message, String id, String source) {
		Format f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		JSONObject lineJS = new JSONObject();
		lineJS.put("message", message);
		lineJS.put("userId", user);
		lineJS.put("userName", user);
		lineJS.put("created", f.format(dateTime));
		//lineJS.put("caption", post.getCaption());
		//lineJS.put("posttype", post.getType());
		lineJS.put("type", "discussion");
		lineJS.put("page", source);
		lineJS.put("id",  id);

		return lineJS;
	}
}

