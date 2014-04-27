package controllers;

import java.text.SimpleDateFormat;
import java.util.Date;

import play.mvc.Controller;
import play.mvc.Result;
import services.ElasticsearchService;
import views.html.index;

public class Esearch extends Controller {

    public static Result putIndex(Long id, String indexString) {
		Date createdAt = new Date();
    	
		// put the log into elasticsearch
		ElasticsearchService.addIndexing("updatelogs", new SimpleDateFormat("yyyyMMdd").format(createdAt), id, indexString);
    	
        return ok(index.render("Your new application is ready."));
    }

}
