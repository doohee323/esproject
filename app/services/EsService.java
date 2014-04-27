package services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.Play;

public class EsService {

	private static final Logger log = LoggerFactory
			.getLogger(EsService.class);

	static Client client;
	static String clusterName;
	static String nodes;

	public static void init() {
		if (clusterName == null) {
			clusterName = Play.application().configuration()
					.getString("elasticsearch.clusterName");
		}
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", clusterName).build();
		if (nodes == null) {
			nodes = Play.application().configuration()
					.getString("elasticsearch.nodes");
		}
		client = buildClient(settings);
	}

	public static void init(String aCluster, String aNode) {
		clusterName = aCluster;
		nodes = aNode;
		init();
	}

	public static Client getClient() {
		return client;
	}

	/**
	 * adding index<br>
	 * 
	 * @param aIndex
	 *            index name.
	 * @param aType
	 *            type name.
	 * @param id
	 *            uid.
	 * @param content
	 *            index content.
	 */
	public static void addIndexing(String aIndex, String aType, Long id,
			Map<String, Object> map) {
		if (client == null) {
			init();
		}

		IndexResponse response = client
				.prepareIndex(aIndex, aType, String.valueOf(id)).setSource(map)
				.setRefresh(true).setOperationThreaded(false).execute()
				.actionGet();
		log.debug(response.getId());
	}

	/**
	 * getTerm<br>
	 * 
	 * @param aIndex
	 *            index name.
	 * @param aTerm
	 *            term name.
	 * @return SearchResponse
	 */
	public static SearchResponse getTerm(String aIndex, String aKey,
			String aValue) {
		if (client == null) {
			init();
		}
		SearchResponse response = client.prepareSearch(aIndex)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.termQuery(aKey, aValue)).setFrom(0)
				.setSize(60).setExplain(true).execute().actionGet();
		return response;
	}

	/**
	 * getQuery<br>
	 * 
	 * @param aIndex
	 *            index name.
	 * @param aQueryString
	 *            queryString.
	 * @return SearchResponse
	 */
	public static SearchResponse getQuery(String aIndex, String aQueryString) {
		if (client == null) {
			init();
		}
		SearchResponse response = client.prepareSearch(aIndex)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.queryString(aQueryString)).setFrom(0)
				.setSize(60).setExplain(true).execute().actionGet();
		return response;
	}

	/**
	 * get index<br>
	 * 
	 * @param aIndex
	 *            index name.
	 * @param aTerm
	 *            term name.
	 * @return SearchResponse
	 */
	public static SearchResponse getTerm(String aIndex, String aKey) {
		return getTerm(aIndex, aKey, aKey);
	}

	/**
	 * registering elasticsearch client TransportAddress<br>
	 * cluster.node.list<br>
	 * 
	 * @param settings
	 *            client settings Info.
	 */
	protected static Client buildClient(Settings settings) {
		TransportClient client = new TransportClient(settings);
		String[] nodeList = nodes.split(",");
		int nodeSize = nodeList.length;
		for (int i = 0; i < nodeSize; i++) {
			client.addTransportAddress(toAddress(nodeList[i]));
		}
		return client;
	}

	/**
	 * registering InetSocketTransportAddress <br>
	 * 
	 * @param address
	 *            node's ip:port Info.
	 * @return InetSocketTransportAddress
	 */
	private static InetSocketTransportAddress toAddress(String address) {
		if (address == null)
			return null;
		String[] splitted = address.split(":");
		int port = 9300;
		if (splitted.length > 1) {
			port = Integer.parseInt(splitted[1]);
		}
		return new InetSocketTransportAddress(splitted[0], port);
	}

	public static void getMappings(String index, String type) {
		ClusterState clusterState = client.admin().cluster().prepareState()
				.setIndices(index).execute().actionGet().getState();
		IndexMetaData inMetaData = clusterState.getMetaData().index(index);
		MappingMetaData metad = inMetaData.mapping(type);

		if (metad != null) {
			try {
				String structure = metad.getSourceAsMap().toString();
				System.out.println(structure);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static PutMappingResponse createIndex(String index, String type,
			XContentBuilder typemapping) {
		// put mapping before index creation
		// XContentBuilder typemapping = buildJsonMappings();
		// client.admin()
		// .indices()
		// .create(new CreateIndexRequest(index)
		// .mapping(type, typemapping)).actionGet();

		// put mapping after index creation
		client.admin().indices().create(new CreateIndexRequest(index))
				.actionGet();
		PutMappingResponse response = null;
		try {
			response = client.admin().indices().preparePutMapping(index)
					.setType(type).setSource(typemapping.string()).execute()
					.actionGet();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return response;
	}

	public static void deleteIndex(Client client, String index) {
		try {
			DeleteIndexResponse delete = client.admin().indices()
					.delete(new DeleteIndexRequest(index)).actionGet();
			if (!delete.isAcknowledged()) {
			} else {
			}
		} catch (Exception e) {
		}
	}

	public static void deleteIndex(String index) {
		try {
			DeleteIndexResponse delete = client.admin().indices()
					.delete(new DeleteIndexRequest(index)).actionGet();
			if (!delete.isAcknowledged()) {
			} else {
			}
		} catch (Exception e) {
		}
	}

	public static boolean isIndexExist(String index) {
		ActionFuture<IndicesExistsResponse> exists = client.admin().indices()
				.exists(new IndicesExistsRequest(index));
		IndicesExistsResponse actionGet = exists.actionGet();

		return actionGet.isExists();
	}

	public static Map<String, Object> getMap(String indexString) {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			JSONObject properties = new JSONObject(indexString);
			Iterator<String> iter = properties.keys();
			while (iter.hasNext()) {
				String key = String.valueOf(iter.next());
				String value = properties.getString(key);
				map.put(key, value);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return map;
	}

}
