/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Crawler;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @author Yasser Ganjisaffar [lastname at gmail dot com]
 */
public class BasicCrawlerPenize extends WebCrawler {
	private String source = "penize.cz";
	private ESConnect escon;
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" + "|png|tiff?|mid|mp2|mp3|mp4"
      + "|wav|avi|mov|mpeg|ram|m4v|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	public BasicCrawlerPenize() {
		super();
		this.escon = new ESConnect("banky","10.0.0.109","elasticsearch");
		//this.escon.createIndex();
		//this.escon.createMapping("discussion");
		//this.escon.setIndexSettings();
	}	
	
  /**
   * You should implement this function to specify whether the given url
   * should be crawled or not (based on your crawling logic).
   */
	@Override
  public boolean shouldVisit(WebURL url) {
    String href = url.getURL().toLowerCase();
    return !FILTERS.matcher(href).matches() && href.startsWith("http://www.penize.cz/");
  }

  /**
   * This function is called when a page is fetched and ready to be processed
   * by your program.
   */
  @Override
  public void visit(Page page) {
    int docid = page.getWebURL().getDocid();
    String url = page.getWebURL().getURL();
    String domain = page.getWebURL().getDomain();
    String path = page.getWebURL().getPath();
    String subDomain = page.getWebURL().getSubDomain();
    String parentUrl = page.getWebURL().getParentUrl();
    String anchor = page.getWebURL().getAnchor();

    //System.out.println("Docid: " + docid);
    System.out.println("URL: " + url);
//    System.out.println("Domain: '" + domain + "'");
//    System.out.println("Sub-domain: '" + subDomain + "'");
//    System.out.println("Path: '" + path + "'");
//    System.out.println("Parent page: " + parentUrl);
//    System.out.println("Anchor text: " + anchor);

    if (page.getParseData() instanceof HtmlParseData) {
      HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
      String text = htmlParseData.getText();
      String html = htmlParseData.getHtml();
      List<WebURL> links = htmlParseData.getOutgoingUrls();
			
  		Document doc = Jsoup.parse(html);
			Elements items = doc.getElementsByAttributeValueStarting("id","comment_");
			Elements itemsbw =  doc.getElementsByAttributeValueStarting("id","bw_comment_");
			items.addAll(itemsbw);
			
			MessageDigest messageDigest = null;
			try {
				messageDigest = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException ex) {
				Logger.getLogger(BasicCrawlerMesec.class.getName()).log(Level.SEVERE, null, ex);
			}			
			
			for (Element item : items ){
				String[] meta = item.getElementsByClass("info").get(0).text().replace("\\s+", " ").replace("\u00a0"," ").replace(",", "|").split("[|]");
				String dT = meta[0].trim().replace("\\s+", " ").replace("\u00a0"," ");
				String user = meta.length == 1 || meta[1].length() == 0?"noname":meta[1].trim();
				
				DateFormat formatter = new SimpleDateFormat("d.MM.yyyy HH:mm:ss");
				Date dateTime = null;
				try {
					dateTime = formatter.parse(dT + ":00");
				} catch (ParseException ex) {
					Logger.getLogger(BasicCrawlerMesec.class.getName()).log(Level.SEVERE, null, ex);
				}
								
				Elements messageEl = item.getElementsByAttributeValueContaining("id", "commenttext_");
				String message = messageEl.get(0).text();
				
				Elements ratingEl = item.getElementsByAttributeValueMatching("class", "rating comment clearfix");
				String rating = ratingEl.get(0).getElementsByAttributeValueStarting("class", "value").get(0).text();
				
			  String unique = dateTime+message+source;
				messageDigest.update(unique.getBytes(), 0 , unique.length());
				String encryptedString = new BigInteger(1, messageDigest.digest()).toString();
			
				try {
					escon.postElasticSearch(escon.prepareJsonForIndex(user, dateTime, message, encryptedString, source, url));
				} catch (Exception ex) {
					Logger.getLogger(BasicCrawlerPenize.class.getName()).log(Level.SEVERE, null, ex);
				}
				//url
				//domain
				//path
				//System.out.println("message " + dateTime + " délka zprávy" + message.length());
			}
//      System.out.println("Text length: " + text.length());
//      System.out.println("Html length: " + html.length());
//      System.out.println("Number of outgoing links: " + links.size());
    }
//    Header[] responseHeaders = page.getFetchResponseHeaders();
//    if (responseHeaders != null) {
//      System.out.println("Response headers:");
//      for (Header header : responseHeaders) {
//        System.out.println("\t" + header.getName() + ": " + header.getValue());
//      }
//    }

//    System.out.println("=============");
  }
}