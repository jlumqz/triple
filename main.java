
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import redis.clients.jedis.Jedis;

public class main {

	public static void main(String[] args) throws Exception {
	//	PrintStream myout = new PrintStream(new FileOutputStream(new File("D:/log.log")));   
	
//		System.setOut(myout);   
		String basedir="/home/meng/papershow/datasets/";
		
		Map<String, Integer> dataset_db_map = new HashMap<String, Integer>();
		dataset_db_map.put("ActivityEducationalInstitution_infobox_properties.nt",1);
		dataset_db_map.put("ArchitecturalStructure_infobox_properties.nt",2);
		dataset_db_map.put("Event_infobox_properties.nt",3);
		dataset_db_map.put("Event_NaturalPlace_WrittenWork_infobox_properties.nt",4);
		dataset_db_map.put("Infrastructure_infobox_properties.nt",5);
		dataset_db_map.put("RouteOfTransportation_infobox_properties.nt",6);
		dataset_db_map.put("Species_infobox_properties.nt",7);
		dataset_db_map.put("Tunnel_infobox_properties.nt",8);
		
		
		Iterator<Entry<String,Integer>> iterator = dataset_db_map.entrySet().iterator();
	
		Entry<String,Integer> entry;

		while (iterator.hasNext()) {
		    entry = iterator.next();
		    String filename = entry.getKey();
		    Integer dbno = entry.getValue();
		    System.out.println(filename);
		    System.out.println(dbno);
		    System.out.println("");
		    
		    FileInputStream is = new FileInputStream(basedir+filename);
			Jedis jedis = new Jedis("127.0.0.1");
			jedis.select(dbno.intValue());

			NxParser nxp = new NxParser(is);

			Node[] nxx;
			while (nxp.hasNext())
			{
				String key=null;
				String value=null;
			     nxx = nxp.next();
			     if(nxx[0].toString().startsWith("http://dbpedia.org/resource/"))
			     {
			    	 //System.out.print(nxx[0].toString().substring("http://dbpedia.org/resource/".length())+"|");
			    	 key=nxx[0].toString().substring("http://dbpedia.org/resource/".length())+"|";
			     }
			     else
			     {
			    	 System.err.println(nxx[0]+" NOT START WITH \"http://dbpedia.org/resource/\"");
			    	 System.exit(1);
			     }
			     
			     if(nxx[1].toString().startsWith("http://dbpedia.org/property/"))
			     {
			    	 //System.out.print(nxx[1].toString().substring("http://dbpedia.org/property/".length())+"\t");
			    	 key=key+nxx[1].toString().substring("http://dbpedia.org/property/".length());
			     }
			     else
			     {
			    	 System.err.println(nxx[1]+" NOT START WITH \"http://dbpedia.org/property/\"");
			    	 System.exit(1);
			     }
			     
			     
			     if(nxx[2].toString().startsWith("http://dbpedia.org/resource/"))
			     {
			    	 //System.out.println(nxx[2].toString().substring("http://dbpedia.org/resource/".length()));
			    	 value=nxx[2].toString().substring("http://dbpedia.org/resource/".length());
			     }
			     else
			     {
			    	// if(nxx[2].toString().startsWith("http://"))
			    	 if(false)
				     {
			    		 if(nxx[1].toString().equals("http://dbpedia.org/property/officialWebsite")
			    				 ||nxx[1].toString().equals("http://dbpedia.org/property/website")
			    				 ||nxx[1].toString().equals("http://dbpedia.org/property/geometry")
			    				 ||nxx[1].toString().equals("http://dbpedia.org/property/mpsub")
			    				 ||nxx[1].toString().equals("http://dbpedia.org/property/operator")
			    				 )
			    		 {
			    			 //System.out.println(nxx[2].toString());
			    			 value=nxx[2].toString();
			    		 }
			    		 else
			    		 {
			    			 System.err.println(nxx[0]);
			    			 System.err.println(nxx[1]);
				    		 System.err.println(nxx[2]+" NOT START WITH \"http://dbpedia.org/resource/\"   BUT START WITH \"http\"");
				    		 System.exit(1);
			    		 }
				     }
			    	 else
			    	 {
			    		 //System.out.println(nxx[2].toString());
			    		 value=nxx[2].toString();
			    	 }
			     }
			     jedis.set(key, value);
			     
			}

		}
		
		
		
		System.err.println("\n\nSuccess!");

	}

}
