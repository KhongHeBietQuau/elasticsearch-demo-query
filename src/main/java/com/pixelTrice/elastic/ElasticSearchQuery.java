package com.pixelTrice.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.google.gson.Gson;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@Repository
public class ElasticSearchQuery {

    @Autowired
    private ElasticsearchClient elasticsearchClient;
//    @Autowired
//    private RestHighLevelClient restHighLevelClient;

    private final String indexName = "products";


    public String createOrUpdateDocument(Product product) throws IOException {

        IndexResponse response = elasticsearchClient.index(i -> i
                .index(indexName)
                .id(product.getId())
                .document(product)
        );
        if (response.result().name().equals("Created")) {
            return new StringBuilder("Document has been successfully created.").toString();
        } else if (response.result().name().equals("Updated")) {
            return new StringBuilder("Document has been successfully updated.").toString();
        }
        return new StringBuilder("Error while performing the operation.").toString();
    }

    public Product getDocumentById(String productId) throws IOException {
        Product product = null;
        GetResponse<Product> response = elasticsearchClient.get(g -> g
                        .index(indexName)
                        .id(productId),
                Product.class
        );

        if (response.found()) {
            product = response.source();
            System.out.println("Product name " + product.getName());
        } else {
            System.out.println("Product not found");
        }

        return product;
    }

    public String deleteDocumentById(String productId) throws IOException {

        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(productId));

        DeleteResponse deleteResponse = elasticsearchClient.delete(request);
        if (Objects.nonNull(deleteResponse.result()) && !deleteResponse.result().name().equals("NotFound")) {
            return new StringBuilder("Product with id " + deleteResponse.id() + " has been deleted.").toString();
        }
        System.out.println("Product not found");
        return new StringBuilder("Product with id " + deleteResponse.id() + " does not exist.").toString();

    }

    public List<Product> searchAllDocuments() throws IOException {

        SearchRequest searchRequest = SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse = elasticsearchClient.search(searchRequest, Product.class);
        List<Hit> hits = searchResponse.hits().hits();
        List<Product> products = new ArrayList<>();
        for (Hit object : hits) {

            System.out.print(((Product) object.source()));
            products.add((Product) object.source());

        }
        return products;
    }

    public List<JSONObject> searchAllDocumentMetricFile() throws IOException {

        SearchRequest searchRequest = SearchRequest.of(s ->

                s.index(".ds-metrics-system-process-2023.10.01-000001")
        );

        SearchResponse searchResponse = elasticsearchClient.search(searchRequest, Object.class);

        List<Hit> hits = searchResponse.hits().hits();
        List<JSONObject> metricFiles = new ArrayList<>();
        for (Hit object : hits) {


            String jsonInString = new Gson().toJson(object);
            JSONObject mJSONObject = new JSONObject(jsonInString);
            System.out.println(((Object) object.source()));
        }


        return metricFiles;
    }
//    public void searchELK()
//    {
//        org.elasticsearch.action.search.SearchRequest searchRequest = new org.elasticsearch.action.search.SearchRequest("cuong");
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("",""))
//                .filter(QueryBuilders.rangeQuery("timestamp").gte(10).lte(20)));
//        searchRequest.source(searchSourceBuilder);
//    }


}
