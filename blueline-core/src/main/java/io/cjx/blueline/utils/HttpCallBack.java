package io.cjx.blueline.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HttpCallBack implements FutureCallback<HttpResponse> {
    private String postBody = "";

    private static Logger logger = LoggerFactory.getLogger(HttpCallBack.class);

    public HttpCallBack(String postBody) {
        this.postBody = postBody;
    }
    @Override
    public void completed(HttpResponse response) {
        int status = response.getStatusLine().getStatusCode();
        String result = getHttpContent(response);
        if(status == 200)
        {
            logger.debug(String.format("[HttpCallBack] Data send to HttpCallBack successfully, and response is：" + result));
        }else{
            logger.error(String.format("[HttpCallBack] Data send to HttpCallBack failed, and response is：: %s,and source data is : %s",result,postBody));
        }

    }

    @Override
    public void failed(Exception ex) {
        logger.error(String.format("[HttpCallBack] Data send to HttpCallBack failed, and response is : %s,and source data is :%s",ex.getMessage(),postBody));
        ex.printStackTrace();
    }

    @Override
    public void cancelled() {
        logger.info("Send to HttpCallBack cancelled");

    }
    protected String getHttpContent(HttpResponse response) {

        HttpEntity entity = response.getEntity();
        String body = null;

        if (entity == null) {
            return null;
        }
        try {
            body = EntityUtils.toString(entity, "utf-8");

        } catch (ParseException e) {

            logger.warn("the response's content inputstream is corrupt", e);
        } catch (IOException e) {

            logger.warn("the response's content inputstream is corrupt", e);
        }
        return body;
    }
}
