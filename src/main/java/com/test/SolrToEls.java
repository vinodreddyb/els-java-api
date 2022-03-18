package com.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SolrToEls {
    private final static String INDEX_DOC="{\"index\":{\"_index\":\"%s\",\"_id\":\"%s\"}}";
    private static String ELS_URL = "http://localhost:9200/";
    public static void main(String[] args) {
        final String indexName = "sc-sg-new";
        createIndex( indexName);
        String elsFilePath = "C:\\Vinod\\data\\solr-elastic\\solr-els.json";
        loadFromSolrGenerateElsIndexFiles(indexName,elsFilePath);
        uploadBulkFile(elsFilePath);

    }

    private static void loadFromSolrGenerateElsIndexFiles(String indexName, String outFilePath) {
        final JsonNode jsonNode = RestFulApiClient.sendGetRequest("http://10.194.158.132:8984/solr/SG_SQE_DATA/select?indent=on&q=SG_ID:61e92b4cb3cb6a671be535d0&wt=json&rows=1000", new TypeReference<JsonNode>() {
        });
        final JsonNode docs = jsonNode.get("response").get("docs");
        ObjectMapper objectMapper = new ObjectMapper();
        try(OutputStream os = new FileOutputStream(outFilePath)) {
            for (JsonNode doc : docs) {
                Map<String, Object> transferRecord = new HashMap<>();
                transferRecord.put("SG_ID", doc.get("SG_ID"));
                List<Map<String, Object>> facetRecords = new ArrayList<>();
                final Iterator<String> fiedNames = doc.fieldNames();
                while (fiedNames.hasNext()) {
                    final String fieldName = fiedNames.next();
                    if (fieldName.startsWith("SG_STR")) {
                        Map<String, Object> facetRecord = new HashMap<>();
                        facetRecord.put("criteria_name", fieldName);
                        facetRecord.put("criteria_value", doc.get(fieldName));
                        facetRecords.add(facetRecord);
                    } else if (fieldName.startsWith("RESULT")) {
                        transferRecord.put(fieldName, doc.get(fieldName));
                    }
                }
                transferRecord.put("criteria_facet", facetRecords);
                String finalDoc = objectMapper.writeValueAsString(transferRecord);
                String indexDoc = String.format(INDEX_DOC, indexName, doc.get("id").asText());
                os.write(indexDoc.getBytes(StandardCharsets.UTF_8));
                os.write('\n');
                os.write(finalDoc.getBytes(StandardCharsets.UTF_8));
                os.write('\n');
            }
        } catch (IOException e) {

        }
    }

    public static void uploadBulkFile(String path) {
        try {
            final byte[] data = Files.readAllBytes(Paths.get(path));
            RequestBody requestBody = RequestBody.create(data, MediaType.parse("application/json"));
            Request request = new Request.Builder()
                    .url(ELS_URL + "_bulk")
                    .post(requestBody)
                    .build();
            OkHttpClient okHttpClient = new OkHttpClient.Builder().build();
            final Response response = okHttpClient.newCall(request).execute();
            final String responseString = IOUtils.toString(response.body().byteStream(), StandardCharsets.UTF_8);
            System.out.println("Response " + responseString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createIndex(String indexName) {
        String url = ELS_URL + indexName;
        Map<String,String> headers= new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Content-type", "application/json");

        try( InputStream resourceAsStream = SolrToEls.class.getClassLoader().getResourceAsStream("sg-facet-mapping.json")) {

            final String s = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);
            OkHttpClient okHttpClient = new OkHttpClient.Builder().build();
            RequestBody requestBody = RequestBody.create(s, MediaType.parse("application/json"));
            Request request = new Request.Builder()
                    .url(url)
                    .headers(Headers.of(headers))
                    .put(requestBody)
                    .build();
            final Response response = okHttpClient.newCall(request).execute();
            final String responseString = IOUtils.toString(response.body().byteStream(), StandardCharsets.UTF_8);
            System.out.println("Response " + responseString);
        } catch (Exception e) {

            System.err.println(e);
        }

    }
}
