import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.runtime.Network;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.logging.Logger;

public class EmbeddedMongo {

    public static final int PORT = 12345;



    public EmbeddedMongo() throws IOException {

        // the following logging setup makes the logging a lot less
        // verbose
        // TODO: figure out how to control it further
        Logger logger = Logger.getLogger(getClass().getName());
        IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaultsWithLogger(Command.MongoD, logger)
                .build();
        MongodStarter starter = MongodStarter.getInstance(runtimeConfig);

        // use instead of logging code above
        //MongodStarter starter = MongodStarter.getDefaultInstance();


        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(PORT, Network.localhostIsIPv6()))
                .build();



        _mongodExecutable = starter.prepare(mongodConfig);
        MongodProcess mongod = _mongodExecutable.start();
        /*
        MongoClient mongo = new MongoClient("localhost", port);
        DB db = mongo.getDB("test");
        DBCollection col = db.createCollection("testCol", new BasicDBObject());
        col.save(new BasicDBObject("testDoc", new Date()));
        */
    }

    public int getPort() { return PORT; }

    public void stop() {
        if (_mongodExecutable != null)
            _mongodExecutable.stop();
    }

    private MongodExecutable _mongodExecutable;

}
