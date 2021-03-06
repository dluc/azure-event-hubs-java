/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.exceptioncontracts;

import java.time.Duration;

import org.junit.After;
import org.junit.Test;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import com.microsoft.azure.servicebus.AuthorizationFailedException;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;
import com.microsoft.azure.servicebus.SharedAccessSignatureTokenProvider;

public class SecurityExceptionsTest extends ApiTestBase
{
	final static String PARTITION_ID = "0"; 
	EventHubClient ehClient;
	
	@Test (expected = AuthorizationFailedException.class)
	public void testEventHubClientUnAuthorizedAccessKeyName() throws Throwable
	{
		final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				"---------------wrongkey------------",
				correctConnectionString.getSasKey());
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.sendSync(new EventData("Test Message".getBytes()));
	}
	
	@Test (expected = AuthorizationFailedException.class)
	public void testEventHubClientUnAuthorizedAccessKey() throws Throwable
	{
		final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				correctConnectionString.getSasKeyName(),
				"--------------wrongvalue-----------");
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.sendSync(new EventData("Test Message".getBytes()));
	}
        
        @Test (expected = ServiceBusException.class)
	public void testEventHubClientInvalidAccessToken() throws Throwable
	{
                final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				"--------------invalidtoken-------------");
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.sendSync(new EventData("Test Message".getBytes()));
	}
        
        @Test (expected = IllegalArgumentException.class)
	public void testEventHubClientNullAccessToken() throws Throwable
	{
                final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				null);
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.sendSync(new EventData("Test Message".getBytes()));
	}
        
        @Test (expected = AuthorizationFailedException.class)
	public void testEventHubClientUnAuthorizedAccessToken() throws Throwable
	{
                final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final String wrongToken = SharedAccessSignatureTokenProvider.generateSharedAccessSignature(
                        "wrongkey",
                        correctConnectionString.getSasKey(),
                        String.format("amqps://%s/%s", correctConnectionString.getEndpoint().getHost(), correctConnectionString.getEntityPath()),
                        Duration.ofSeconds(10));
                final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				wrongToken);
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.sendSync(new EventData("Test Message".getBytes()));
	}
        
        @Test (expected = AuthorizationFailedException.class)
	public void testEventHubClientWrongResourceInAccessToken() throws Throwable
	{
                final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final String wrongToken = SharedAccessSignatureTokenProvider.generateSharedAccessSignature(
                        correctConnectionString.getSasKeyName(),
                        correctConnectionString.getSasKey(),
                        "----------wrongresource-----------",
                        Duration.ofSeconds(10));
                final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				wrongToken);
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.sendSync(new EventData("Test Message".getBytes()));
	}
	
	@Test (expected = AuthorizationFailedException.class)
	public void testUnAuthorizedAccessSenderCreation() throws Throwable
	{
		final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				"---------------wrongkey------------",
				correctConnectionString.getSasKey());
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.createPartitionSenderSync(PARTITION_ID);
	}
	
	@Test (expected = AuthorizationFailedException.class)
	public void testUnAuthorizedAccessReceiverCreation() throws Throwable
	{
		final ConnectionStringBuilder correctConnectionString = TestContext.getConnectionString();
		final ConnectionStringBuilder connectionString = new ConnectionStringBuilder(
				correctConnectionString.getEndpoint(),
				correctConnectionString.getEntityPath(),
				"---------------wrongkey------------",
				correctConnectionString.getSasKey());
		
		ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
		ehClient.createReceiverSync(TestContext.getConsumerGroupName(), PARTITION_ID, PartitionReceiver.START_OF_STREAM);
	}
	
	@After
	public void cleanup() throws ServiceBusException
	{
		ehClient.closeSync();
	}
}