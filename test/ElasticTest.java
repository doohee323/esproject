import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import services.ElasticsearchService;

public class ElasticTest {
	private String cluster = "locketCast";
	private String nodes = "localhost:9300,localhost:9301,localhost:9302";
	private String index = "updatelogs";
	private String type = "table";
	private Client client = null;

	private XContentBuilder buildJsonMappings() {
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

	private void createData() {
		System.out.println("Data creation");
		IndexResponse response = null;
		for (int i = 0; i < 10; i++) {
			Map<String, Object> json = new HashMap<String, Object>();
			json.put("ATTR_" + i, "new value" + i);
			response = this.client.prepareIndex(index, type).setSource(json)
					.setRefresh(true).setOperationThreaded(false).execute()
					.actionGet();
		}
		String _index = response.getIndex();
		String _type = response.getType();
		long _version = response.getVersion();
		System.out.println("Index : " + _index + "   Type : " + _type
				+ "   Version : " + _version);
		System.out.println("----------------------------------");
	}

	public ElasticTest() {
		// // local
		// this.node = nodeBuilder().local(true).node();
		// this.client = node.client();

		// cluster
		ElasticsearchService.init(cluster, nodes);
		this.client = ElasticsearchService.getClient();

		if (ElasticsearchService.isIndexExist(index)) {
			ElasticsearchService.deleteIndex(this.client, index);
			ElasticsearchService.createIndex(index, type, buildJsonMappings());
		} else {
			ElasticsearchService.createIndex(index, type, buildJsonMappings());
		}
	}

	public void simpleTest() {
		System.out.println("(create)----------------------------------------");
		createData();

		System.out
				.println("(retrieve)----------------------------------------");
		for (int i = 0; i < 10; i++) {
			SearchResponse sr = ElasticsearchService.queryString(index, "ATTR_"
					+ i + ":new value" + i);
			java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
			while (hit_it.hasNext()) {
				SearchHit hit = hit_it.next();
				System.out
						.println(hit.getId() + "->" + hit.getSourceAsString());
			}
		}
	}

	public void mappingTest() {
		System.out.println("(create)----------------------------------------");
		ElasticsearchService.getMappings(index, type);
		createData();

		System.out
				.println("(retrieve)----------------------------------------");
		for (int i = 0; i < 10; i++) {
			SearchResponse sr = ElasticsearchService.queryString(index, "ATTR_"
					+ i + ":new value" + i);
			java.util.Iterator<SearchHit> hit_it = sr.getHits().iterator();
			while (hit_it.hasNext()) {
				SearchHit hit = hit_it.next();
				System.out
						.println(hit.getId() + "->" + hit.getSourceAsString());
			}
		}
	}

	public static void main(String[] args) {
		ElasticTest es = new ElasticTest();

		System.out.println("[simple test]");
		es.simpleTest();
		System.out.println("[mapping test]");
		es.mappingTest();
	}
}