package com.example.crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Encrypts selected fields in a JSON document without recursion to avoid stack growth.
 */
public final class KeyScanPiiEncryptor {
    private final Encryptor encryptor;
    private final Set<String> piiKeys;
    private final Pattern keyPattern;
    private final ObjectMapper mapper;

    public KeyScanPiiEncryptor(Encryptor encryptor, Collection<String> piiKeys, Pattern keyPattern) {
        this(encryptor, piiKeys, keyPattern, new ObjectMapper());
    }

    public KeyScanPiiEncryptor(
            Encryptor encryptor, Collection<String> piiKeys, Pattern keyPattern, ObjectMapper mapper) {
        this.encryptor = Objects.requireNonNull(encryptor, "encryptor");
        this.keyPattern = keyPattern;
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.piiKeys = new HashSet<>();
        if (piiKeys != null) {
            for (String key : piiKeys) {
                if (key != null && !key.isBlank()) {
                    this.piiKeys.add(key.toLowerCase(Locale.ROOT));
                }
            }
        }
    }

    public byte[] encrypt(byte[] jsonDocument, String documentId) {
        Objects.requireNonNull(jsonDocument, "jsonDocument");
        Objects.requireNonNull(documentId, "documentId");
        try {
            JsonNode root = mapper.readTree(jsonDocument);
            if (root == null) {
                return jsonDocument;
            }

            boolean mutated = scanAndEncrypt(root, documentId);
            return mutated ? mapper.writeValueAsBytes(root) : jsonDocument;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON input", e);
        }
    }

    private boolean scanAndEncrypt(JsonNode root, String documentId) throws IOException {
        boolean mutated = false;
        Deque<JsonNode> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            JsonNode current = stack.pop();
            if (current.isObject()) {
                ObjectNode objectNode = (ObjectNode) current;
                List<String> fieldNames = new ArrayList<>();
                objectNode.fieldNames().forEachRemaining(fieldNames::add);
                for (String fieldName : fieldNames) {
                    JsonNode child = objectNode.get(fieldName);
                    if (child == null) {
                        continue;
                    }
                    if (shouldEncrypt(fieldName, child)) {
                        ObjectNode encryptedNode = createEncryptedNode(child, documentId);
                        objectNode.set(fieldName, encryptedNode);
                        mutated = true;
                    } else if (child.isContainerNode()) {
                        stack.push(child);
                    }
                }
            } else if (current.isArray()) {
                ArrayNode arrayNode = (ArrayNode) current;
                for (JsonNode child : arrayNode) {
                    if (child.isContainerNode()) {
                        stack.push(child);
                    }
                }
            }
        }
        return mutated;
    }

    private boolean shouldEncrypt(String fieldName, JsonNode node) {
        if (node.isObject() && node.has("_enc")) {
            return false;
        }
        if (fieldName == null) {
            return false;
        }
        String lower = fieldName.toLowerCase(Locale.ROOT);
        if (piiKeys.contains(lower)) {
            return true;
        }
        return keyPattern != null && keyPattern.matcher(fieldName).matches();
    }

    private ObjectNode createEncryptedNode(JsonNode originalValue, String documentId) throws IOException {
        byte[] serialized = mapper.writeValueAsBytes(originalValue);
        Encryptor.EncryptionEnvelope envelope = encryptor.encrypt(serialized, documentId);
        ObjectNode wrapper = mapper.createObjectNode();
        wrapper.put("v", 1);
        ObjectNode encNode = wrapper.putObject("_enc");
        encNode.put("alg", envelope.alg());
        encNode.put("kid", envelope.kid());
        encNode.put("iv", envelope.iv());
        encNode.put("ct", envelope.ct());
        return wrapper;
    }
}
