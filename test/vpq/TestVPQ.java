package vpq;

import static org.junit.Assert.assertTrue;

import org.dminus.vpq.VariablyPersistentQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVPQ  
{
	VariablyPersistentQueue<String> vpq;
	
	@Before
	public void before( )
	{
		vpq = new VariablyPersistentQueue<String>();
		vpq.setMaxInMemoryQueueDepth( 10 );
		vpq.setPersistencePath( "/tmp/queue" );
	}
	
	@Test
	public void testBufferOverrun( )
	{
		
		for( int i=0; i < 10; i++ )
		{
			vpq.add( "message " + i );
		}
		
		assertTrue( vpq.getFileBufferSize() == 0 );
		assertTrue( vpq.getSize() == 10 );
		
		vpq.poll();
		
		assertTrue( vpq.getSize() == 9 );
		
		vpq.add( "message 11" );
		vpq.add( "message 12" );
		
		try {
			Thread.sleep( 1000 );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		assertTrue( vpq.getSize() == 11 );
		assertTrue( vpq.getFileBufferSize() == 1 );
		
	}
	
	@After
	public void after( )
	{
		vpq.dumpFillBufferQueue();
	}
}
