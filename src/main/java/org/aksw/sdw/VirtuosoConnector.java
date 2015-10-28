package org.aksw.sdw;

//import virtuoso.jdbc4.VirtuosoDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Chile on 3/16/2015.
 */
public class VirtuosoConnector {

    //private VirtuosoDataSource dSource = new VirtuosoDataSource();
    private Connection conn;
    private String graphUri;
    private Statement batch;

    public VirtuosoConnector(final String host, final int port, final String username, final String password) throws SQLException {
/*        dSource.setPortNumber(port);
        dSource.setServerName(host);
        conn = dSource.getConnection(username, password);
        conn.setAutoCommit(true);
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT); //for faster execution*/
    }

    private boolean graphExists(String uri) throws SQLException {
/*        Statement s= conn.createStatement();
        ResultSet set = s.executeQuery("SELECT 1 " +
                "FROM DB.DBA.RDF_EXPLICITLY_CREATED_GRAPH " +
                "WHERE TRIM(ID_TO_IRI(REC_GRAPH_IID)) LIKE TRIM('" + uri + "')");
        if(set.next())
            return true;
        else*/
            return false;
    }

    private void createGraph(String uri) throws SQLException {
/*        Statement s= conn.createStatement();
        s.execute("SPARQL CREATE GRAPH <" + uri + ">");
        s.close();*/
    }

    public void initializeGraphLoading(String graphUri) throws SQLException {
/*        this.graphUri = graphUri;
        if(!graphExists(graphUri)) //not!
            createGraph(graphUri);
        Statement stmt = conn.createStatement();
        stmt.execute("log_enable (2)");  //do not log this! - for faster execution
        stmt.close();*/
    }

    public void finalizeGraphLoading() throws SQLException {
/*        Statement stmt = conn.createStatement();
        stmt.execute("checkpoint");
        stmt.execute("log_enable (1)");  //do not log this! - for faster execution
        stmt.close();*/
    }

    public void addTTL(String n3ttl) throws SQLException {
/*        Statement stmt = conn.createStatement();
        stmt.execute("TTLP ('" + n3ttl + "', '', '" + graphUri + "', 17)");
        stmt.close();*/
    }

    public void newBatch() throws SQLException {
//        batch = conn.createStatement();
    }

    public void addBatch(String sql) throws SQLException {
  //      if(batch == null)
  //          return; //TODO
  //      batch.addBatch(sql);
    }
}
