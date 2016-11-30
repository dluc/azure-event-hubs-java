/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus;

import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  Contract for all client entities with Open-Close/Abort state m/c
 *  main-purpose: closeAll related entities
 *  Internal-class
 */
public abstract class ClientEntity
{
    private final String clientId;
    private final Object syncClose;
    private final ClientEntity parent;

    private CompletableFuture<Void> closeTask;
    private boolean isClosing;
    private boolean isClosed;

	protected static boolean executorsAreInternal;
	protected static Consumer<Runnable> onStartLongLivedThread = null;
    protected static ScheduledExecutorService transientExecutor = null;
    
	private static final HashSet<String> references = new HashSet<String>();
	private static final Object syncReferences = new Object();
	
	private static final Logger TRACE_LOGGER = Logger.getLogger(ClientConstants.SERVICEBUS_CLIENT_TRACE);

    protected ClientEntity(final String clientId, final ClientEntity parent, final Consumer<Runnable> onStartLongLivedThread,
    		final ScheduledExecutorService transientExecutor)
    {
    	this(clientId, parent);
        
		if ((ClientEntity.transientExecutor != null) && !ClientEntity.transientExecutor.isShutdown())
		{
			// Executors have already been initialized and are operational.
			if (!(((onStartLongLivedThread == null) && (transientExecutor == null)) ||
				  ((onStartLongLivedThread == ClientEntity.onStartLongLivedThread) && (transientExecutor == ClientEntity.transientExecutor))))
			{
				throw new IllegalArgumentException(
						"After first client creation, onStartLongLivedThread and transientExecutor must be null or the same instances as previously passed in.");
			}
		}
		else
		{
			// Executors have not been initialized, or existing executors have been shut down and
			// need to be re-initialized. If the inputs are both null, use our own executors. If the inputs are both
			// not null, use those. Mixed is not allowed.
			if ((onStartLongLivedThread != null) && (transientExecutor != null))
			{
				ClientEntity.onStartLongLivedThread = onStartLongLivedThread;
				ClientEntity.transientExecutor = transientExecutor;
				ClientEntity.executorsAreInternal = false;
			}
			else if ((onStartLongLivedThread == null) && (transientExecutor == null))
			{
				ClientEntity.onStartLongLivedThread = new Consumer<Runnable>()
				{
					@Override
					public void accept(Runnable threadRunnable)
					{
						(new Thread(threadRunnable)).start();
					}
				};
				
				int corePoolSize = Math.max(Runtime.getRuntime().availableProcessors(), 4);
				TRACE_LOGGER.log(Level.FINE, String.format(Locale.US, "Starting internal transientExecutor with coreThreadPoolSize: %s", corePoolSize));
				ClientEntity.transientExecutor = Executors.newScheduledThreadPool(corePoolSize);
				ClientEntity.executorsAreInternal = true;
			}
			else
			{
				throw new IllegalArgumentException("onStartLongLivedThread and transientExecutor must both be non-null or both be null.");
			}
		}
		
		addReference();
    }

    protected ClientEntity(final String clientId, final ClientEntity parent, final boolean usesExecutors)
    {
    	this(clientId, parent);
        
        if (usesExecutors)
        {
        	addReference();
        }
    }

    private ClientEntity(final String clientId, final ClientEntity parent)
    {
        this.clientId = clientId;
        this.parent = parent;
        
        this.syncClose = new Object();
    }
    
    private void addReference()
    {
        if (ClientEntity.executorsAreInternal)
        {
        	synchronized (ClientEntity.syncReferences)
        	{
        		ClientEntity.references.add(this.clientId);
        	}
        }
    }
    
    // It is safe to call this from a thread running in transientExecutor because shutdown does
    // not stop currently executing tasks, just prevents any more from being scheduled. Existing
    // tasks run to completion.
    //
    // It is also safe to call this from an instance which was created with usesExecutors==false.
    // In that case, references.remove() will return false and no actions will be taken.
    private void removeReference()
    {
    	if (ClientEntity.executorsAreInternal)
    	{
    		synchronized (ClientEntity.syncReferences)
    		{
    			if (ClientEntity.references.remove(this.clientId) && (ClientEntity.references.size() == 0))
    			{
    				TRACE_LOGGER.log(Level.FINE, "Shutting down internal transientExecutor threadpool");
    				ClientEntity.transientExecutor.shutdown();
    			}
    		}
    	}
    }
    
    protected abstract CompletableFuture<Void> onClose();

    public String getClientId()
    {
        return this.clientId;
    }

    boolean getIsClosed()
    {
        final boolean isParentClosed = this.parent != null && this.parent.getIsClosed();
        synchronized (this.syncClose)
        {
            return isParentClosed || this.isClosed;
        }
    }

    // returns true even if the Parent is (being) Closed
    boolean getIsClosingOrClosed()
    {
        final boolean isParentClosingOrClosed = this.parent != null && this.parent.getIsClosingOrClosed();
        synchronized (this.syncClose)
        {
            return isParentClosingOrClosed || this.isClosing || this.isClosed;
        }
    }

    // used to force close when entity is faulted
    protected final void setClosed()
    {
        synchronized (this.syncClose)
        {
            this.isClosed = true;
        }
    }

    public final CompletableFuture<Void> close()
    {
        synchronized (this.syncClose)
        {
            if (this.isClosed || this.isClosing)
                return this.closeTask == null ? CompletableFuture.completedFuture(null) : this.closeTask;

            this.isClosing = true;
        }

        this.closeTask = this.onClose().thenRunAsync(new Runnable()
            {
                @Override
                public void run()
                {
                    synchronized (ClientEntity.this.syncClose)
                    {
                        ClientEntity.this.isClosing = false;
                        ClientEntity.this.isClosed = true;
                    }
                    
                    removeReference();
                }
            },
            ClientEntity.transientExecutor);

        return this.closeTask;
    }

    public final void closeSync() throws ServiceBusException
    {
        try
        {
            this.close().get();
        }
        catch (InterruptedException|ExecutionException exception)
        {
            if (exception instanceof InterruptedException)
            {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            Throwable throwable = exception.getCause();
            if (throwable != null)
            {
                if (throwable instanceof RuntimeException)
                {
                    throw (RuntimeException)throwable;
                }

                if (throwable instanceof ServiceBusException)
                {
                    throw (ServiceBusException)throwable;
                }

                throw new ServiceBusException(true, throwable);
            }
        }
    }

    protected final void throwIfClosed(Throwable cause)
    {
        if (this.getIsClosingOrClosed())
        {
            throw new IllegalStateException(String.format(Locale.US, "Operation not allowed after the %s instance is Closed.", this.getClass().getName()), cause);
        }
    }
}
