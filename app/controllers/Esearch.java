package controllers;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.json.JSONException;
import org.json.JSONObject;

import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import services.ElasticsearchService;

public class Esearch extends Controller {

	public static Result putIndex(Long id, String indexString) {
		Date createdAt = new Date();

		// put the log into elasticsearch
		ElasticsearchService.addIndexing("updatelogs", new SimpleDateFormat(
				"yyyyMMdd").format(createdAt), id, indexString);

		return ok(Json.toJson("OK"));
	}

	public static Result getTerm(String searchString) throws JSONException {
		JSONObject input = new JSONObject(searchString);
		SearchResponse sr = ElasticsearchService.getTerm("updatelogs", "user_id", input.getString("user_id"));

		List<String> lRslt = new ArrayList<String>();
		java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
		while (hit_it.hasNext()) {
			SearchHit hit = hit_it.next();
			lRslt.add(hit.getSourceAsString());
		}

		return ok(Json.toJson(lRslt));
	}
}
