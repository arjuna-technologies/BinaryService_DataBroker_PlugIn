/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.binaryservice.endpoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.jboss.resteasy.util.GenericType;
import com.arjuna.dbplugins.binaryservice.BinaryAcceptorDispatcher;

@Path("/endpoints")
@Stateless
public class BinaryAcceptorService
{
    private static final Logger logger = Logger.getLogger(BinaryAcceptorService.class.getName());

    @PUT
    @POST
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public String acceptBinary(@PathParam("id") String id, Map<String, Object> fields)
    {
        logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: [" + id + "]");

        try
        {
            logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: on \"" + id + "\" (field number = " + fields.size() + ")");

            _binaryAcceptorDispatcher.dispatch(id, fields);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorService.acceptBinary: Unable to process binary", throwable);

            throw new WebApplicationException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return "OK";
    }

    @PUT
    @POST
    @Path("/{id}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public String acceptBinary(@PathParam("id") String id, MultipartFormDataInput multipartFormDataInput)
    {
        try
        {
            logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: on \"" + id + "\"");

            Map<String, Object> fields   = new HashMap<String, Object>();
            byte[]              data     = multipartFormDataInput.getFormDataPart("file", new GenericType<byte[]>() {});
            String              filename = findFilename(multipartFormDataInput.getFormDataMap().get("file"));
            if (data != null)
                fields.put("data", data);
            if (filename != null)
                fields.put("filename", filename);

            if (fields.size() != 0)
            {
                logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: on \"" + id + "\" (field number = " + fields.size() + ")");

                _binaryAcceptorDispatcher.dispatch(id, fields);
            }
            else
                logger.log(Level.WARNING, "BinaryAcceptorService.acceptBinary: on \"" + id + "\" (field number is 0)");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorService.acceptBinary: Unable to process binary", throwable);

            throw new WebApplicationException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return "OK";
    }

    private String findFilename(List<InputPart> inputParts)
    {
        String filename = null;

        for (InputPart inputPart: inputParts)
            if (filename == null)
            {
                MultivaluedMap<String, String> multivaluedMap     = inputPart.getHeaders();
                String[]                       contentDisposition = multivaluedMap.getFirst("Content-Disposition").split(";");

                for (String possibleFilename : contentDisposition)
                    if (possibleFilename.trim().startsWith("filename"))
                        filename = possibleFilename.split("=")[1].trim().replaceAll("\"", "");
            }

        return filename;
    }

    @EJB
    private BinaryAcceptorDispatcher _binaryAcceptorDispatcher;
}
