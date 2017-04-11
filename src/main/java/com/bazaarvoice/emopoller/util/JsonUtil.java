package com.bazaarvoice.emopoller.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Stream;

public class JsonUtil {
    private JsonUtil() {} // uninstantiable

    private static final JsonNodeFactory factory = JsonNodeFactory.instance;
    private static final ObjectMapper mapper = new ObjectMapper();

    private static JsonNode coerce(Object arg) {
        if (arg == null) {
            return factory.nullNode();
        } else if (arg instanceof JsonNode) {
            return (JsonNode) arg;
        } else if (arg instanceof String) {
            return factory.textNode((String) arg);
        } else if (arg instanceof Number) {
            if (arg instanceof Byte) {
                return factory.numberNode((Byte) arg);
            } else if (arg instanceof Short) {
                return factory.numberNode((Short) arg);
            } else if (arg instanceof Integer) {
                return factory.numberNode((Integer) arg);
            } else if (arg instanceof Long) {
                return factory.numberNode((Long) arg);
            } else if (arg instanceof Float) {
                return factory.numberNode((Float) arg);
            } else if (arg instanceof Double) {
                return factory.numberNode((Double) arg);
            } else {
                throw new IllegalArgumentException("Unknown numeric type: " + arg.getClass().getCanonicalName());
            }
        } else if (arg instanceof Boolean) {
            return factory.booleanNode((Boolean) arg);
        } else {
            throw new IllegalArgumentException("Didn't know what to do with " + arg.getClass().getCanonicalName() + ". Did you mean to use convert instead?");
        }
    }

    // the mapper=========================================
    public static ObjectMapper mapper() { return mapper; }

    public static JsonNode toTree(Object arg) {
        return mapper().convertValue(arg, JsonNode.class);
    }

    // mini-dsl ==========================================
    public static ObjectNode obj(Object... args) {
        Preconditions.checkArgument(args.length % 2 == 0, "arguments must be key-value pairs");
        final ObjectNode objectNode = factory.objectNode();
        for (int i = 0; i < args.length; i += 2) {
            Preconditions.checkArgument(args[i] instanceof String, "keys must be strings");
            objectNode.set((String) args[i], coerce(args[i + 1]));
        }
        return objectNode;
    }

    public static ArrayNode arr(Stream<JsonNode> args) {
        final ArrayNode arrayNode = factory.arrayNode();
        args.forEachOrdered(arrayNode::add);
        return arrayNode;
    }

    public static ArrayNode arr(Object... args) {
        final ArrayNode arrayNode = factory.arrayNode();
        for (Object arg : args) {
            arrayNode.add(coerce(arg));
        }
        return arrayNode;
    }

    public static NullNode nul() {return factory.nullNode();}
}
