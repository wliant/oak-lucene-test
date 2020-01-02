package org.codeaffectionado.training;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.jcr.LoginException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder.IndexRule;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class App 
{
	private static NodeStore nodestore;
	private static Repository repository;
	
	private static void init() throws InvalidFileStoreVersionException, IOException {
    	LuceneIndexProvider provider = new LuceneIndexProvider();
    	FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("repository")).build();
    	nodestore = SegmentNodeStoreBuilders.builder(fs).build();
        repository = new Jcr(new Oak(nodestore))
        		.withAsyncIndexing("async", 3)
        		.with(new LuceneIndexEditorProvider())
        		.with((QueryIndexProvider) provider)
        		.with((Observer)provider)
        		.withAsyncIndexing()
        		.createRepository();
	}
	
	private static void createLuceneIndex() throws RepositoryException {
		Session session = createAdminSession();
		Node indexesNode = session.getRootNode().getNode("oak:index");
		IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
		IndexRule indexRules = idxBuilder.indexRule("nt:unstructured");
		indexRules.sync();
		indexRules.property("name").analyzed().nodeScopeIndex();
		idxBuilder.async("async");
		idxBuilder.includedPaths("/");
		Node documentIndex = indexesNode.addNode("lucene", "oak:QueryIndexDefinition");
		idxBuilder.build(documentIndex);
		session.save();
		session.logout();
		
	}
	
	private static void createTestData() throws LoginException, RepositoryException {
		Session session = createAdminSession();
		Node test = session.getRootNode().addNode("test");
		test.setProperty("name", "foo");
		session.save();
		session.logout();
	}
	
	private static Session createAdminSession() throws LoginException, RepositoryException {
		return repository.login(getCred());
	}
	
	private static SimpleCredentials getCred() {
		return new SimpleCredentials("admin", "admin".toCharArray());
	}
	
	private static void performQuery() throws Exception {
		final Session session = createAdminSession();
		TimeUnit.MICROSECONDS.sleep(5);
		
		QueryManager qm = session.getWorkspace().getQueryManager();
		final Query q = qm.createQuery("select * from [nt:base] where contains(., 'foo')", Query.JCR_SQL2);
		
		new RetryLoop(new RetryLoop.Condition() {
			public String getDescription() {
				return "Full text query";
			}
			
			public boolean isTrue() throws Exception {
				QueryResult r = q.execute();
				return r.getNodes().hasNext();
			}
		}, 20, 500);
	}
    public static void main( String[] args ) throws Exception
    {
    	init();
    	createLuceneIndex();
    	createTestData();
    	performQuery();
    	
    }
    
    private static class RetryLoop {
    	private final long timeout;
    	
    	static public interface Condition {
    		String getDescription();
    		boolean isTrue() throws Exception;
    	}
    	
    	public RetryLoop(Condition c, int timeoutSeconds, int intervalBetweenTriesMsec) throws Exception {
    		timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;
    		while(System.currentTimeMillis() < timeout) {
    			try {
    				if(c.isTrue()) {
    					return ;
    				}
    			} catch(AssertionError ae) {
    			} catch(Exception e) {
    			}
    			
    			try {
    				Thread.sleep(intervalBetweenTriesMsec);
    			} catch(InterruptedException ignore) {
    				
    			}
    			
    			
    		}
    		throw new Exception("timeout occur");
    	}
    }
}
