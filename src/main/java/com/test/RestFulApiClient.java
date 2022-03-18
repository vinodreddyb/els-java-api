package com.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import okhttp3.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;



public final class RestFulApiClient {
    private static   OkHttpClient okHttpClient;
    private static ObjectMapper objectMapper;

    static {
        okHttpClient = new OkHttpClient.Builder().build();
        objectMapper = new ObjectMapper();

        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.findAndRegisterModules();
    }

    public <T,R> R  sendPostRequest(String url, Map<String,String> headers, T body, TypeReference<R> responseType)  {

        System.out.println("Sending Post request to url " +  url);

        int statusCode = 0;
        try {
            String requestXml = objectMapper.writeValueAsString(body);
            System.out.println("Sending body {}" + requestXml);
            RequestBody requestBody = RequestBody.create(requestXml, MediaType.parse("application/json"));
            Request request = new Request.Builder()
                    .url(url)
                    .headers(Headers.of(headers))
                    .post(requestBody)
                    .build();

            return getResponse(request,responseType);
        } catch (IOException e) {
            System.out.println("Post error for url " + url + ": " + e );
            throw new RuntimeException("Post error for url " + url + ": " + e);
        }

    }

    public static  <R> R  sendGetRequest(String url,   TypeReference<R> responseType) {
        return sendGetRequest(url, new HashMap<>(), responseType);
    }
    public static  <R> R  sendGetRequest(String url, Map<String,String> headers,  TypeReference<R> responseType)  {

        int statusCode = 0;
        try {

            Request request = new Request.Builder()
                    .url(url)
                    .headers(Headers.of(headers))
                    .build();
            return getResponse(request, responseType);
        } catch (IOException e) {
            System.out.println("Get error for url " + url + ": " + e );
            throw new RuntimeException("Get error for url " + url + ": " + e);
        }
    }

    private static  <R> R getResponse(Request request,TypeReference<R> responseType) throws IOException {
        final Response response = okHttpClient.newCall(request).execute();
        int statusCode = response.code();
        if(response.isSuccessful()) {
            if(responseType == null) {
                return null;
            }
            try(InputStream inputStream = Objects.requireNonNull(response.body()).byteStream()) {
                return objectMapper.readValue(inputStream, responseType);
            }
        }  else if(statusCode == 500) {
            throw new RuntimeException("API calling error for url " + request.url().url()
                    + " " + response.message() + " " + statusCode);
        } else {
            return null;
        }

    }



}
