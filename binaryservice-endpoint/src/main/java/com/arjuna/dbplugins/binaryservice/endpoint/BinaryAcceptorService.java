/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.binaryservice.endpoint;

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
import javax.ws.rs.core.Response;
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
    public String acceptBinary(@PathParam("id") String id, byte[] data)
    {
        logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: [" + id + "]");

        try
        {
            logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: on \"" + id + "\" (length = " + data.length + ")");

            _binaryAcceptorDispatcher.dispatch(id, data);
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

            byte[] data = multipartFormDataInput.getFormDataPart("file", new GenericType<byte[]>() {});

            if (data != null)
            {
                logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: on \"" + id + "\" (data length = " + data.length + ")");

                _binaryAcceptorDispatcher.dispatch(id, data);
            }
            else
                logger.log(Level.WARNING, "BinaryAcceptorService.acceptBinary: on \"" + id + "\" (data is null)");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorService.acceptBinary: Unable to process binary", throwable);

            throw new WebApplicationException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return "OK";
    }

    @EJB
    private BinaryAcceptorDispatcher _binaryAcceptorDispatcher;
}
