/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.binaryservice;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.InitialContext;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataSource;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PostActivated;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;
import com.arjuna.databroker.data.jee.annotation.PreDeactivated;

public class BinaryAcceptorDataSource implements DataSource
{
    private static final Logger logger = Logger.getLogger(BinaryAcceptorDataSource.class.getName());

    public static final String ENDPOINTPATH_PROPERTYNAME = "Endpoint Path";

    public BinaryAcceptorDataSource()
    {
        logger.log(Level.FINE, "BinaryAcceptorDataSource");

        try
        {
            _binaryAcceptorDispatcher = (BinaryAcceptorDispatcher) new InitialContext().lookup("java:global/binaryservice-plugin-ear-1.0.0p1m1/binaryservice-plugin-1.0.0p1m1/BinaryAcceptorDispatcher");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorDispatcher: no binaryAcceptorDispatcher found", throwable);
        }
    }

    public BinaryAcceptorDataSource(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "BinaryAcceptorDataSource: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;

        try
        {
            _binaryAcceptorDispatcher = (BinaryAcceptorDispatcher) new InitialContext().lookup("java:global/binaryservice-plugin-ear-1.0.0p1m1/binaryservice-plugin-1.0.0p1m1/BinaryAcceptorDispatcher");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "BinaryAcceptorDispatcher: no binaryAcceptorDispatcher found", throwable);
        }
    }

    @PostCreated
    @PostRecovery
    @PostConfig
    public void configure()
    {
        _endpointPath = _properties.get(ENDPOINTPATH_PROPERTYNAME);
    }

    @PostActivated
    public void register()
    {
        if (_binaryAcceptorDispatcher != null)
            _binaryAcceptorDispatcher.register(_endpointPath, this);
        else
            logger.log(Level.WARNING, "BinaryAcceptorDataSource.register: no binaryAcceptorDispatcher");
    }

    @PreDeactivated
    public void unregister()
    {
        if (_binaryAcceptorDispatcher != null)
            _binaryAcceptorDispatcher.unregister(_endpointPath);
        else
            logger.log(Level.WARNING, "BinaryAcceptorDispatcher.unregister: no binaryAcceptorDispatcher");
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }
    
    public void dispatch(byte[] data)
    {
        logger.log(Level.FINE, "BinaryAcceptorDataSource.dispatch: on \"" + _endpointPath + "\" (length = " + data.length + ")");
        System.err.println("BinaryAcceptorDataSource.dispatch: on \"" + _endpointPath + "\" (length = " + data.length + ")");

        _dataProvider.produce(data);
    }
    
    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(byte[].class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == byte[].class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private String _endpointPath;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataProviderInjection
    private DataProvider<byte[]> _dataProvider;

    private BinaryAcceptorDispatcher _binaryAcceptorDispatcher;

}
