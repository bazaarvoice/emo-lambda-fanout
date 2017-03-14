package com.bazaarvoice.emopoller.util;

import de.mkammerer.argon2.Argon2;
import de.mkammerer.argon2.Argon2Factory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class HashUtil {
    private HashUtil() {}

    private static final Argon2 argon2 = Argon2Factory.create();

    public static String argon2hash(final String toHash) {
        // generates salt and uses UTF-8 encoding for toHash and then wipes the password array
        return argon2.hash(2, 65536, 1, toHash);
    }

    public static boolean verifyArgon2(final String hash, final String toVerify) {
        return argon2.verify(hash, toVerify);
    }

    @Deprecated
    public static String sha512hash_base64(final String toHash) {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Shouldn't happen");
        }
        final byte[] digest = md.digest(toHash.getBytes());
        return new String(Base64.getEncoder().encode(digest));
    }

    public static String sha512hash_base16(final String toHash) {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Shouldn't happen");
        }
        final byte[] digest = md.digest(toHash.getBytes());
        return String.format("%0128x", new BigInteger(1, digest));
    }
}
