package controllers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.json.JSONException;
import org.json.JSONObject;

import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import services.EsService;

public class Esearch extends Controller {

	private static String cluster = "locketCast";
	private static String nodes = "localhost:9300,localhost:9301,localhost:9302";
	private static String index = "updatelogs";
	private static String type = "";

	private static XContentBuilder buildJsonMappings() {
		XContentBuilder builder = null;
		try {
			builder = XContentFactory.jsonBuilder().startObject()
					.startObject(type).startObject("properties");
			for (int i = 1; i < 5; i++) {
				builder.startObject("ATTR_" + i).field("type", "string")
						.field("store", "yes").field("index", "analyzed")
						.endObject();
			}
			builder.endObject().endObject().endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder;
	}

	public static Result putIndex(Long id, String indexString)
			throws JSONException {
		Date createdAt = new Date();
		type = new SimpleDateFormat("yyyyMMdd").format(createdAt);

		// EsService.init(cluster, nodes);
		// if (EsService.isIndexExist(index)) {
		// EsService.deleteIndex(index);
		// EsService.createIndex(index, type, buildJsonMappings());
		// } else {
		// EsService.createIndex(index, type, buildJsonMappings());
		// }

		// put the log into elasticsearch
		EsService.addIndexing("updatelogs", type, id,
				EsService.getMap(indexString));
		return ok(Json.toJson("OK"));
	}

	public static Result getTerm(String searchString) throws JSONException {
		JSONObject input = new JSONObject(searchString);
		SearchResponse sr = EsService.getTerm("updatelogs",
				"user_id", input.getString("user_id"));

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
		}

		return ok(Json.toJson(lRslt));
	}

	public static Result getQuery(String searchString) throws JSONException {
		SearchResponse sr = EsService.getQuery("updatelogs",
				searchString);

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
		}

		return ok(Json.toJson(lRslt));
	}
}
