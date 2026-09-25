package com.cs324a1.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JobParserTest {

    @TempDir
    Path tempDir;

    @Test
    void parseNumberListAcceptsMixedSeparators() {
        assertEquals(List.of(2, 4, 5, 11), JobParser.parseNumberList("2, 4 5\n11"));
    }

    @Test
    void parseNumberListAcceptsNegativeAndSemicolonSeparatedValues() {
        assertEquals(List.of(-8, -2, 0, 7), JobParser.parseNumberList("-8;-2, 0;7"));
    }

    @Test
    void parseNumberListRejectsNullOrBlankInput() {
        assertThrows(IllegalArgumentException.class, () -> JobParser.parseNumberList(null));
        assertThrows(IllegalArgumentException.class, () -> JobParser.parseNumberList("   \n  "));
    }

    @Test
    void parseNumberListRejectsNonNumericToken() {
        assertThrows(IllegalArgumentException.class, () -> JobParser.parseNumberList("2, four, 5"));
    }

    @Test
    void loadNumberListReadsSingleColumnWithHeader() throws IOException {
        Path file = write("value\n2\n4\n5\n");
        assertEquals(List.of(2, 4, 5), JobParser.loadNumberList(file.toFile()));
    }

    @Test
    void loadNumberListReadsSingleColumnWithoutHeader() throws IOException {
        Path file = write("7\n8\n9\n");
        assertEquals(List.of(7, 8, 9), JobParser.loadNumberList(file.toFile()));
    }

    @Test
    void loadNumberListReadsMultipleValuesPerLine() throws IOException {
        Path file = write("value\n1, 2, 3\n4 5\n6;7\n");
        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), JobParser.loadNumberList(file.toFile()));
    }

    @Test
    void loadNumberListSkipsCommentsAndBlankLines() throws IOException {
        Path file = write("# sample input\nvalue\n\n2\n\n# mid-file note\n4\n\n");
        assertEquals(List.of(2, 4), JobParser.loadNumberList(file.toFile()));
    }

    @Test
    void loadNumberListRejectsNonNumericValue() throws IOException {
        Path file = write("value\n2\nnope\n");
        assertThrows(IOException.class, () -> JobParser.loadNumberList(file.toFile()));
    }

    @Test
    void loadNumberListRejectsFileWithoutNumbers() throws IOException {
        Path file = write("value\n");
        assertThrows(IOException.class, () -> JobParser.loadNumberList(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeReadsRangeWithHeader() throws IOException {
        Path file = write("start,end\n1,1000\n");
        assertArrayEquals(new int[]{1, 1000}, JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeReadsRangeWithoutHeader() throws IOException {
        Path file = write("2, 10\n");
        assertArrayEquals(new int[]{2, 10}, JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeSkipsCommentsAndBlankLines() throws IOException {
        Path file = write("# range file\nstart end\n\n1 1000\n");
        assertArrayEquals(new int[]{1, 1000}, JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeAcceptsEqualEndpoints() throws IOException {
        Path file = write("7,7\n");
        assertArrayEquals(new int[]{7, 7}, JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeRejectsReversedRange() throws IOException {
        Path file = write("1000,1\n");
        assertThrows(IOException.class, () -> JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeRejectsMissingEndValue() throws IOException {
        Path file = write("1000\n");
        assertThrows(IOException.class, () -> JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeRejectsNonNumericValue() throws IOException {
        Path file = write("1,ten\n");
        assertThrows(IOException.class, () -> JobParser.loadPrimeSumRange(file.toFile()));
    }

    @Test
    void loadPrimeSumRangeRejectsFileWithoutRange() throws IOException {
        Path file = write("start,end\n");
        assertThrows(IOException.class, () -> JobParser.loadPrimeSumRange(file.toFile()));
    }

    private Path write(String content) throws IOException {
        Path file = Files.createTempFile(tempDir, "job-input", ".csv");
        Files.writeString(file, content);
        return file;
    }
}
