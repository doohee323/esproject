package controllers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

	private static String type = "";

	private static XContentBuilder buildJsonMappings() {
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

	public static Result putIndex(Long id, String indexString)
			throws JSONException {
		Date createdAt = new Date();
		type = new SimpleDateFormat("yyyyMMdd").format(createdAt);

		// 1) put the log into elasticsearch
		EsService.addIndexing("updatelogs", type, id,
				EsService.getMap(indexString));

		return ok(Json.toJson("OK"));
	}

	public static Result putIndex2(Long id, String indexString)
			throws JSONException {
		Date createdAt = new Date();
		type = new SimpleDateFormat("yyyyMMdd").format(createdAt) + "_1";

		// 2) put the log into elasticsearch
		EsService.addIndexing("updatelogs2", type, buildJsonMappings(), id,
				EsService.getMap(indexString));

		return ok(Json.toJson("OK"));
	}

	public static Result getTerm(String searchString) throws JSONException {
		JSONObject input = new JSONObject(searchString);
		SearchResponse sr = EsService.getTerm("updatelogs", "user_id",
				input.getString("user_id"));

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
		}

		return ok(Json.toJson(lRslt));
	}

	public static Result getQuery(String searchString) throws JSONException {
		SearchResponse sr = EsService.getQuery("updatelogs", searchString);

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
		}

		return ok(Json.toJson(lRslt));
	}
}
