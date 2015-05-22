/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.binaryservice;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.Singleton;

@Singleton
public class BinaryAcceptorDispatcher
{
    private static final Logger logger = Logger.getLogger(BinaryAcceptorDispatcher.class.getName());

    public BinaryAcceptorDispatcher()
    {
        logger.log(Level.FINE, "BinaryAcceptorDispatcher");

        _syncObject                  = new Object();
        _binaryAcceptorDataSourceMap = new HashMap<String, BinaryAcceptorDataSource>();
    }

    public void dispatch(String id, Map<String, Object> map)
    {
        synchronized (_syncObject)
        {
            logger.log(Level.FINE, "BinaryAcceptorDispatcher.dispatch: " + id);

            BinaryAcceptorDataSource binaryAcceptorDataSource = _binaryAcceptorDataSourceMap.get(id);

            if (binaryAcceptorDataSource != null)
            {
                DispatchWorker dispatchWorker = new DispatchWorker(binaryAcceptorDataSource, map);
                dispatchWorker.start();
            }
            else
                logger.log(Level.WARNING, "BinaryAcceptorDispatcher.dispatch: unable to find 'dom document acceptor data source': " + id);
        }
    }

    public boolean register(String id, BinaryAcceptorDataSource binaryAcceptorDataSource)
    {
        synchronized (_syncObject)
        {
            logger.log(Level.FINE, "BinaryAcceptorDispatcher.register: " + id);

            if (! _binaryAcceptorDataSourceMap.containsKey(id))
            {
                _binaryAcceptorDataSourceMap.put(id, binaryAcceptorDataSource);
                return true;
            }
            else
                return false;
        }
    }

    public boolean unregister(String id)
    {
        synchronized (_syncObject)
        {
            logger.log(Level.FINE, "BinaryAcceptorDispatcher.unregister: " + id);

            return _binaryAcceptorDataSourceMap.remove(id) != null;
        }
    }

    private class DispatchWorker extends Thread
    {
        public DispatchWorker(BinaryAcceptorDataSource binaryAcceptorDataSource, Map<String, Object> fields)
        {
            _binaryAcceptorDataSource = binaryAcceptorDataSource;
            _fields                   = fields;
        }

        public void run()
        {
            logger.log(Level.FINE, "BinaryAcceptorDispatcher:DispatchWorker.run: start");
            _binaryAcceptorDataSource.dispatch(_fields);
            logger.log(Level.FINE, "BinaryAcceptorDispatcher:DispatchWorker.run: complete");
        }

        private BinaryAcceptorDataSource _binaryAcceptorDataSource;
        private Map<String, Object>      _fields;
    }

    private Object                                _syncObject;
    private Map<String, BinaryAcceptorDataSource> _binaryAcceptorDataSourceMap;
}
