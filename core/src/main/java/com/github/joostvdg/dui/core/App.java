package com.github.joostvdg.dui.core;

import com.github.joostvdg.dui.logging.LogLevel;
import com.github.joostvdg.dui.logging.Logger;
import com.github.joostvdg.dui.api.ProtocolConstants;
import com.github.joostvdg.dui.server.api.DuiServer;
import com.github.joostvdg.dui.server.api.DuiServerFactory;

import java.util.Random;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

    public static void main(String[] args) {
        ServiceLoader<Logger> loggers = ServiceLoader.load(Logger.class);
        Logger logger = loggers.findFirst().isPresent() ? loggers.findFirst().get() : null;
        if (logger == null) {
            System.err.println("Did not find any loggers, quiting");
            System.exit(1);
        }
        logger.start(LogLevel.INFO);

        int pseudoRandom1 = new Random().nextInt(ProtocolConstants.POTENTIAL_SERVER_NAMES.length -1);
        int pseudoRandom2 = new Random().nextInt(ProtocolConstants.POTENTIAL_SERVER_NAMES.length -1);
        int pseudoRandom3 = new Random().nextInt(ProtocolConstants.POTENTIAL_SERVER_NAMES.length -1);
        DuiServer serverSimpleA = DuiServerFactory.newServerSimple(ProtocolConstants.EXTERNAL_COMMUNICATION_PORT_A, ProtocolConstants.POTENTIAL_SERVER_NAMES[pseudoRandom1], logger);
        DuiServer serverSimpleB = DuiServerFactory.newServerSimple(ProtocolConstants.EXTERNAL_COMMUNICATION_PORT_B, ProtocolConstants.POTENTIAL_SERVER_NAMES[pseudoRandom2], logger);
        DuiServer serverSimpleC= DuiServerFactory.newServerSimple(ProtocolConstants.EXTERNAL_COMMUNICATION_PORT_C, ProtocolConstants.POTENTIAL_SERVER_NAMES[pseudoRandom3], logger);

        serverSimpleA.logMembership();
        serverSimpleB.logMembership();
        serverSimpleC.logMembership();

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(serverSimpleA::startServer);
        executorService.submit(serverSimpleB::startServer);
        executorService.submit(serverSimpleC::startServer);

        for(int i = 0; i < 15; i++){
            try {
                Thread.sleep(5000);
                serverSimpleA.logMembership();
                Thread.sleep(2000);
                serverSimpleB.logMembership();
                if (i == 5) {
                    serverSimpleC.stopServer();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
