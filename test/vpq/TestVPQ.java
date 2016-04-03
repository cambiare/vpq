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
		
		sleep( 100 );
		
		assertTrue( vpq.getSize() == 11 );
		assertTrue( vpq.getFileBufferSize() == 1 );
		
		System.out.println( "SIZE: " + vpq.getSize() );
		
		vpq.poll();
		
		sleep( 100 );
		
		System.out.println( "SIZE: " + vpq.getSize() );
		
		assertTrue( vpq.getSize() == 10 );
		assertTrue( vpq.getFileBufferSize() == 0 );
		
	}
	
	private void sleep( int msec )
	{
		try {
			Thread.sleep( msec );
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@After
	public void after( )
	{
		vpq.dumpFillBufferQueue();
	}
}
