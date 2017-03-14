package com.bazaarvoice.emopoller.emo;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

class EmoUtils {
    private EmoUtils() {}

    interface OnErr {
        void onErr(Response response, String entity);
    }

    static Response responseOrThrow(final Response response, final OnErr onErr) {
        if (response.getStatus() >= 200 && response.getStatus() < 300) {
            return response;
        } else {
            final Response.ResponseBuilder clone = Response.fromResponse(response);
            final String entity = response.readEntity(String.class);
            onErr.onErr(response, entity);
            throw new WebApplicationException(clone.entity(entity).build());
        }
    }

    static void consume(InputStream inputStream) {
        if (inputStream != null) {
            try (InputStream is = inputStream) {
                //noinspection StatementWithEmptyBody
                while (is.read() != -1) {}
                is.close();
            } catch (IOException e) {
                // shhhhh
            }
        }
    }


}
