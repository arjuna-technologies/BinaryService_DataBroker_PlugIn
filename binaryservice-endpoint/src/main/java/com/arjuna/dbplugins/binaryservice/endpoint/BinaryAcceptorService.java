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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import com.arjuna.dbplugins.binaryservice.BinaryAcceptorDispatcher;

@Path("/endpoints")
@Stateless
public class BinaryAcceptorService
{
    private static final Logger logger = Logger.getLogger(BinaryAcceptorService.class.getName());

    @POST
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public void acceptBinary(@PathParam("id") String id, String putProposeAgreementRequest)
    {
        logger.log(Level.FINE, "BinaryAcceptorService.acceptBinary: [" + id + "]");

        try
        {
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorService.acceptBinary: Unable to process binary", throwable);

            throw new WebApplicationException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @EJB
    private BinaryAcceptorDispatcher _binaryAcceptorDispatcher;
}
