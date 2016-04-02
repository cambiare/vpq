package org.dminus.vpq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.impl.Log4JLogger;

public class VariablyPersistentQueue<T> 
{
	private static final Log4JLogger	logger = new Log4JLogger();
	private static final int			BACKFILL_WAIT = 1000; // 1 sec
	
	private String		persistencePath;
	private int			maxInMemoryQueueDepth;
	
	private int			size;
	private int			firstFile;
	private int			lastFile;
	
	private boolean		continueBuffering;

	
	private LinkedBlockingQueue<T>		queue;
	private LinkedBlockingQueue<T>		bufferQueue;
	
	public VariablyPersistentQueue( )
	{
		size = 0;
		this.queue = new LinkedBlockingQueue<T>();
		this.bufferQueue = new LinkedBlockingQueue<T>();
		
		this.firstFile = 0;
		this.lastFile = 0;
		
		this.continueBuffering = true;
		
		new Thread( new QueueOverrunBufferWatcher() ).start();
		new Thread( new QueueBufferBackfillWatcher() ).start();
	}

	public int getFileBufferSize( )
	{
		return lastFile - firstFile;
	}
	
	public int getSize( )
	{
		return size;
	}
	
	public void add( T obj )
	{
		if( size < maxInMemoryQueueDepth )
		{
			queue.add( obj );
		} else {
			bufferQueue.add( obj );
		}
		
		size++;
	}
	
	public T poll( )
	{
		T obj = queue.poll();
		size--;
		
		return obj;
	}
	
	public T peek( )
	{
		return queue.peek();
	}
	
	public String getPersistencePath() {
		return persistencePath;
	}

	public void setPersistencePath(String persistencePath) {
		this.persistencePath = persistencePath;
	}

	public int getMaxInMemoryQueueDepth() {
		return maxInMemoryQueueDepth;
	}

	public void setMaxInMemoryQueueDepth(int maxInMemoryQueueDepth) {
		this.maxInMemoryQueueDepth = maxInMemoryQueueDepth;
	}

	class QueueOverrunBufferWatcher implements Runnable
	{
		public void run() 
		{
			while( continueBuffering )
			{
				T obj;
				try {
					obj = bufferQueue.poll( 1000, TimeUnit.DAYS );
				} catch (InterruptedException e1) {
					logger.error( e1 );
					return;
				}
				
				File f = new File( getPersistencePath() + File.separator + "vpq-buffer-" + lastFile );
				ObjectOutputStream out;
				try {
					out = new ObjectOutputStream( new FileOutputStream( f ) );
					out.writeObject( obj );
					out.close();
					lastFile++;
				} catch (FileNotFoundException e) {
					logger.error( "failed to open buffer file", e );
				} catch (IOException e) {
					logger.error( "failed to write to buffer file", e );
				}
			}
		}
	}
	
	public void dumpFillBufferQueue( )
	{
		firstFile = lastFile = 0;
		
		File dir = new File( persistencePath );
		for( File f : dir.listFiles() )
		{
			f.delete();
		}
	}
	
	class QueueBufferBackfillWatcher implements Runnable
	{
		public void run() 
		{
			while( continueBuffering )
			{
				int nBufRemove = maxInMemoryQueueDepth - size;
				int bufSize = lastFile - firstFile;
				
				nBufRemove = ( nBufRemove < bufSize ) ? nBufRemove : bufSize;
				
				for( int i=0; i < nBufRemove; i++ )
				{
					int fnum = i + firstFile;
					File f = new File( persistencePath + File.separator + "vpq-buffer-" + fnum );
					try {
						ObjectInputStream in = new ObjectInputStream( new FileInputStream( f ) );
						@SuppressWarnings("unchecked")
						T obj = (T)in.readObject();
						in.close();
						add( obj );
						firstFile++;
					} catch (FileNotFoundException e) {
						logger.error( "failed to open buffer file", e );
					} catch (IOException e) {
						logger.error( "failed to read buffer file", e );
					} catch (ClassNotFoundException e) {
						logger.error( e );
					}
				}
				
				if( firstFile == lastFile )
				{
					firstFile = 0;
					lastFile = 0;
				}
				
				try {
					Thread.sleep( BACKFILL_WAIT );
				} catch (InterruptedException e) {
					logger.error( e );
					continueBuffering = false;
				}
			}
		}
	}
	
	
}
