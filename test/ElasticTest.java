import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.json.JSONException;
import org.json.JSONObject;

import services.EsService;

public class ElasticTest {
	private String cluster = "locketCast";
	private String nodes = "localhost:9300,localhost:9301,localhost:9302";
	private String type = "table";

	public ElasticTest() {
		EsService.init(cluster, nodes);
	}

	private XContentBuilder buildJsonMappings() {
		XContentBuilder builder = null;
		try {
			builder = XContentFactory.jsonBuilder().startObject()
					.startObject(type).startObject("properties");
			builder.startObject("user_id").field("type", "string")
					.field("store", "yes").field("index", "analyzed")
					.endObject();
			builder.startObject("data").field("type", "string")
					.field("store", "yes").field("index", "analyzed")
					.endObject();
			builder.endObject().endObject().endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder;
	}

	public void putIndex(Long id, String indexString) throws JSONException {
		Date createdAt = new Date();
		type = new SimpleDateFormat("yyyyMMdd").format(createdAt);

		// 1) put the log into elasticsearch
		IndexResponse response = EsService.addIndexing("updatelogs", type, id,
				EsService.getMap(indexString));
		System.out.println("id:" + response.getId());
	}

	public void putIndex2(Long id, String indexString) throws JSONException {
		Date createdAt = new Date();
		type = new SimpleDateFormat("yyyyMMdd").format(createdAt) + "_1";

		// 2) put the log into elasticsearch
		IndexResponse response = EsService.addIndexing("updatelogs2", type,
				buildJsonMappings(), id, EsService.getMap(indexString));
		System.out.println("id:" + response.getId());
	}

	public List<String> getTerm(String searchString) throws JSONException {
		JSONObject input = new JSONObject(searchString);
		SearchResponse sr = EsService.getTerm("updatelogs", "user_id",
				input.getString("user_id"));

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
			System.out.println(hit.getId() + " -> " + hit.getSourceAsString());
		}
		return lRslt;
	}

	public List<String> getQuery(String searchString) throws JSONException {
		SearchResponse sr = EsService.getQuery("updatelogs", searchString);

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
			System.out.println(hit.getId() + " -> " + hit.getSourceAsString());
		}
		return lRslt;
	}

	public static void main(String[] args) {
		ElasticTest es = new ElasticTest();

		try {
			System.out.println("===============");
			es.putIndex(
					Long.parseLong("1"),
					"{\"user_id\":\"doohee323\", \"action\":\"search\", \"data\":\"facebook Feed\"}");
			System.out.println("===============");
			es.putIndex(
					Long.parseLong("2"),
					"{\"user_id\":\"doohee323\", \"action\":\"search\", \"data\":\"facebook Feed\"}");

			System.out.println("===============");
			es.getTerm("{\"user_id\":\"doohee323\"}");
			System.out.println("===============");
			es.getQuery("user_id:doohee323");
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}