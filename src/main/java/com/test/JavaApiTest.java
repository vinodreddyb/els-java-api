package com.test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

public class JavaApiTest {
    private static final ElasticsearchClient client;

    static {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);
    }

    public static void main(String[] args) {


        Map<String, String> queryParams = new LinkedHashMap<>();
        queryParams.put("SG_STR_Preferred selection", "ATV212");
        queryParams.put("SG_STR_Protection level", "IP55");
        aggregate(queryParams);

    }

    private static void aggregate(Map<String, String> queryParams) {

        Map<String,Aggregation> aggregations = new HashMap<>();
        aggregations.put("aggs_all_filters",getAllAgg(queryParams));
        queryParams.forEach((name,val)-> {
            aggregations.put(name, getFilterAggregation(queryParams,name));
        });

        SearchRequest search = SearchRequest.of(r -> r
                .index("sc-sg-new")
                .query(q -> q
                        .term(t -> t
                                .field("SG_ID")
                                .value(v -> v.stringValue("61e92b4cb3cb6a671be535d0"))
                        ))
                .aggregations(aggregations));

        try {
            final SearchResponse<JsonNode> response = client.search(search, JsonNode.class);
            final List<Hit<JsonNode>> hits = response.hits().hits();

            queryParams.keySet().forEach(p-> {
                System.out.println("P---------------" + p);
                final Aggregate paggregate = response.aggregations().get(p);
                final List<StringTermsBucket> buckets = paggregate.filter().aggregations()
                        .get("facets").nested().aggregations()
                        .get("buckets").filter().aggregations()
                        .get("names").sterms().buckets().array();
                if(!buckets.isEmpty()) {
                    final Aggregate values = buckets.get(0).aggregations().get("values");
                    for (StringTermsBucket charValBuckets : values.sterms().buckets().array()) {
                        System.out.println(charValBuckets.key());
                    }
                }
                System.out.println("----------------------------------------");
            });


            final List<StringTermsBucket> allAggregationsBuckets = response.aggregations().get("aggs_all_filters")
                                              .filter().aggregations().get("facets")
                                              .nested().aggregations().get("names")
                                              .sterms().buckets().array();
            for (StringTermsBucket bucket : allAggregationsBuckets) {
                String characterName = bucket.key();
                if(queryParams.containsKey(characterName)) {
                    continue;
                }
                System.out.println(characterName);
                final Aggregate values1 = bucket.aggregations().get("values");
                for (StringTermsBucket charValBuckets : values1.sterms().buckets().array()) {
                    System.out.println(charValBuckets.key());
                }
                System.out.println("----------------");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static Aggregation getFilterAggregation(Map<String, String> queryParams, String param) {


        List<Query> filters = new ArrayList<>();
        queryParams.entrySet().stream()
                .filter(en-> !en.getKey().equals(param))
                .forEach(entry-> {
                    filters.add(Query.of(qnqbf ->
                            qnqbf.term(qnqbft -> qnqbft.field("criteria_facet.criteria_name").value(entry.getKey()))));
                    filters.add(Query.of(qnqbf ->
                            qnqbf.term(qnqbft -> qnqbft.field("criteria_facet.criteria_value").value(entry.getValue()))));
                });

        final Query facetFilter = Query.of(q -> q.nested(qn -> qn.path("criteria_facet")
                .query(qnq -> qnq.bool(qnqb -> qnqb.filter(filters)))));

        final TermsAggregation names = TermsAggregation.of(v -> v.field("criteria_facet.criteria_name"));
        final TermsAggregation values = TermsAggregation.of(v -> v.field("criteria_facet.criteria_value"));

        final Aggregation parameterSubAggregation = Aggregation.of(ag -> ag.nested(n -> n
                .path("criteria_facet"))
                .aggregations("buckets",
                        name ->
                                name.filter(fn->
                                        fn.term(fnt->
                                                fnt.field("criteria_facet.criteria_name").value(param)))
                                        .aggregations("names",fnta->
                                                fnta.terms(names)
                                                        .aggregations("values", fntav-> fntav.terms(values)))));


       return Aggregation.of(ag->
               ag.filter(facetFilter)
                       .aggregations("facets", parameterSubAggregation));
    }

    private static Aggregation getAllAgg(Map<String, String> queryParams) {
        final TermsAggregation names = TermsAggregation.of(v -> v.field("criteria_facet.criteria_name").size(50));
        final TermsAggregation values = TermsAggregation.of(v -> v.field("criteria_facet.criteria_value").size(50));

        final Aggregation allSubAggregation = Aggregation.of(ag -> ag.nested(n -> n
                .path("criteria_facet")).aggregations("names",
                name -> name.terms(names)
                        .aggregations("values", val -> val.terms(values))));

        List<Query> allAggFilters = new ArrayList<>();
        queryParams.forEach((name, val) -> {
            List<Query> facetInnerFilters = new ArrayList<>();
            facetInnerFilters.add(Query.of(qnqbf ->
                    qnqbf.term(qnqbft -> qnqbft.field("criteria_facet.criteria_name").value(name))));
            facetInnerFilters.add(Query.of(qnqbf ->
                    qnqbf.term(qnqbft -> qnqbft.field("criteria_facet.criteria_value").value(val))));

            allAggFilters.add(Query.of(q -> q.nested(qn -> qn.path("criteria_facet")
                    .query(qnq -> qnq.bool(qnqb -> qnqb.filter(facetInnerFilters))))));

        });

        return Aggregation.of(ag -> ag.filter(agf -> agf.bool(agfb -> agfb.filter(allAggFilters)))
                .aggregations("facets", allSubAggregation));
    }

    private static void search() {
        SearchResponse<Map> search = null;
        try {
            search = client.search(s -> s
                            .index("sc-sg-new")
                            .query(q -> q
                                    .term(t -> t
                                            .field("SG_ID")
                                            .value(v -> v.stringValue("61e92b4cb3cb6a671be535d0"))
                                    )),
                    Map.class);
            for (Hit<Map> hit : search.hits().hits()) {
                System.out.println(hit.source());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
