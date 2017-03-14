package com.bazaarvoice.emopoller.tools;

import com.bazaarvoice.emopoller.util.HashUtil;
import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

public class HashKeyCommand extends Command {
    public HashKeyCommand() {
        super("hash-key", "hashes... keys");
    }

    @Override public void configure(final Subparser subparser) {
        subparser.addArgument("-k", "--key")
            .dest("key")
            .type(String.class)
            .required(true)
            .help("The key to hash");
    }

    @Override public void run(final Bootstrap<?> bootstrap, final Namespace namespace) throws Exception {
        final String toHash = namespace.get("key");
        System.out.println(HashUtil.argon2hash(toHash));
    }
}
