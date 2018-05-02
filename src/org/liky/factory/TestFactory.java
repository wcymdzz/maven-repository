package org.liky.factory;

import org.liky.dao.TestDAO;
import org.liky.dao.impl.TestDAOImpl;
import org.liky.dbc.DataBaseConnection;

public class TestFactory {
public static TestDAO getTestDAOInstance(DataBaseConnection dbc){
	return new TestDAOImpl(dbc);
}
}
