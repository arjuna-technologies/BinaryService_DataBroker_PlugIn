/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.binaryservice.endpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import com.arjuna.dbplugins.binaryservice.BinaryAcceptorDispatcher;

@WebServlet("/servlet/endpoints/*")
public class BinaryAcceptorServlet extends HttpServlet
{
    private static final long serialVersionUID = -1634923399669222259L;

    private static final Logger logger = Logger.getLogger(BinaryAcceptorServlet.class.getName());

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException
    {
        logger.log(Level.FINE, "BinaryAcceptorServlet.doPost");

        try
        {
            String id = request.getPathInfo().substring(1);
            byte[] data = getBytes(request.getInputStream());

            logger.log(Level.FINE, "BinaryAcceptorServlet.doPost: on \"" + id + "\" (length = " + data.length + ")");

            _binaryAcceptorDispatcher.dispatch(id, data);

            PrintWriter writer = response.getWriter();
            writer.println("OK");
            writer.flush();

            response.setStatus(HttpServletResponse.SC_OK);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorServlet.doPost: Unable to process binary", throwable);

            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
    
    private static byte[] getBytes(InputStream inputStream)
        throws IOException
    {
        int    bufferSize = 1024;
        byte[] buffer     = new byte[bufferSize];

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        int readLength = inputStream.read(buffer, 0, bufferSize);
        while (readLength != -1)
        {
            byteArrayOutputStream.write(buffer, 0, readLength);
            readLength = inputStream.read(buffer, 0, bufferSize);
        }

        return byteArrayOutputStream.toByteArray();
    }

    @EJB
    private BinaryAcceptorDispatcher _binaryAcceptorDispatcher;
}
