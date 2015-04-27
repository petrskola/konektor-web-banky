/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Crawler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Petr
 */
public class CrawlerStarter {
	
	
	public static void main(String[] args){
		String tempStorage = "c:\\ES\\konektor-web-banky\\temp\\";
		int crawlerCount = 1;

		try {
			BasicCrawlController mesec = new BasicCrawlController(tempStorage, crawlerCount, new String[]{"http://www.mesec.cz"}, 1);
			BasicCrawlController penize = new BasicCrawlController(tempStorage, crawlerCount, new String[]{"http://www.penize.cz"}, 2);
			//BasicCrawlController bitCoin = new BasicCrawlController(tempStorage, crawlerCount, new String[]{"https://bitcointalk.org/"}, 1);
		} catch (Exception ex) {
			Logger.getLogger(CrawlerStarter.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
