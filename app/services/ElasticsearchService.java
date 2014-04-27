package services;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.Play;

public class ElasticsearchService {
	
	private static final Logger log = LoggerFactory.getLogger(ElasticsearchService.class);

	static Client client;
	
	public static void init(String aCluster) {
		Settings settings;
		settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", aCluster).build();
		client = buildClient(settings);
	}
	
	/**
	 * adding index<br>
	 * @param aIndex index name.
	 * @param aType type name.
	 * @param id uid.
	 * @param content index content.
	 * @throws Exception
	 */
	public static void addIndexing(String aIndex, String aType, Long id, String content) {
		if(client == null) {
			init(Play.application().configuration()
					.getString("elasticsearch.clusterName"));
		}
		IndexRequestBuilder requestBuilder = client.prepareIndex(aIndex, aType, String.valueOf(id));
		IndexResponse response = requestBuilder.setSource(content)
				.execute().actionGet();
		log.debug(response.getId());
	}

	/**
	 * registering elasticsearch client TransportAddress<br>
	 * cluster.node.list<br>
	 * @param settings
	 *            client settings Info.
	 * @throws Exception
	 */
	protected static Client buildClient(Settings settings) {
		TransportClient client = new TransportClient(settings);
		String nodes = Play.application().configuration()
				.getString("elasticsearch.nodes");
		String[] nodeList = nodes.split(",");
		int nodeSize = nodeList.length;
		for (int i = 0; i < nodeSize; i++) {
			client.addTransportAddress(toAddress(nodeList[i]));
		}
		return client;
	}

	/**
	 * registering InetSocketTransportAddress <br>
	 * @param address
	 *            node's ip:port Info.
	 * @return InetSocketTransportAddress
	 * @throws Exception
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
}