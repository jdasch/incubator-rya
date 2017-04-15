package org.apache.rya.periodic.notification.application;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.recovery.PeriodicNotificationProvider;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;

import org.junit.Assert;

public class PeriodicNotificationProviderIT extends AccumuloExportITBase {

    @Test
    public void testProvider() throws MalformedQueryException, InterruptedException {
        
        String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n
        
        BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        PeriodicNotificationCoordinatorExecutor coord = new PeriodicNotificationCoordinatorExecutor(2, notifications);
        PeriodicNotificationProvider provider = new PeriodicNotificationProvider();
        CreatePcj pcj = new CreatePcj();
        
        String id = null;
        try(FluoClient fluo = new FluoClientImpl(getFluoConfiguration())) {
            id = pcj.createPcj(sparql, fluo);
            provider.processRegisteredNotifications(coord, fluo.newSnapshot());
        }
        
        TimestampedNotification notification = notifications.take();
        Assert.assertEquals(5000, notification.getInitialDelay());
        Assert.assertEquals(15000, notification.getPeriod());
        Assert.assertEquals(TimeUnit.MILLISECONDS, notification.getTimeUnit());
        Assert.assertEquals(id, notification.getId());
        
    }
    
}
