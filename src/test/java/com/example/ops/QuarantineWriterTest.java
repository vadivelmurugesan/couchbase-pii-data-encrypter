package com.example.ops;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QuarantineWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void writesQuarantineFile_withSafeFilename_andExpectedFields() throws Exception {
        QuarantineWriter writer = new QuarantineWriter(tempDir);

        String docId = "user::1/../bad";
        String stage = "GET/UPSERT:weird stage";
        Exception error = new IllegalArgumentException("something failed");

        Path out = writer.write(docId, stage, error);
        assertTrue(Files.exists(out));

        String filename = out.getFileName().toString();
        assertTrue(filename.matches("\\d+-[A-Za-z0-9_-]+-[0-9a-f]{16}(-\\d+)?\\.txt"), filename);

        String content = Files.readString(out);
        assertTrue(content.contains("docId=" + docId));
        assertTrue(content.contains("stage=" + stage));
        assertTrue(content.contains("exceptionClass="));
        assertTrue(content.contains("exceptionMessageSha256="));
        assertFalse(content.isBlank());
        assertNotNull(content);
    }

    @Test
    void doesNotIncludeDocumentBodyOrPiiValues() throws Exception {
        QuarantineWriter writer = new QuarantineWriter(tempDir);

        String docId = "doc-1";
        String stage = "ENCRYPT";
        String documentBody = "{\"ssn\":\"123-45-6789\",\"name\":\"bob\"}";
        Exception error = new RuntimeException("failed while processing " + documentBody);

        Path out = writer.write(docId, stage, error);
        String content = Files.readString(out);

        assertFalse(content.contains(documentBody));
        assertFalse(content.contains("123-45-6789"));
        assertTrue(content.contains("exceptionMessageSha256="));
    }
}

