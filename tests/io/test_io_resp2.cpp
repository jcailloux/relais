#include <catch2/catch_test_macros.hpp>
#include <jcailloux/relais/io/redis/RespWriter.h>
#include <jcailloux/relais/io/redis/RespParser.h>
#include <string>
#include <string_view>

using namespace jcailloux::relais::io;

// =============================================================================
// RespWriter tests
// =============================================================================

TEST_CASE("RespWriter simple command", "[resp2][writer]") {
    RespWriter w;
    const char* argv[] = {"PING"};
    size_t argvlen[] = {4};
    w.writeCommand(1, argv, argvlen);

    REQUIRE(std::string_view(w.data(), w.size()) == "*1\r\n$4\r\nPING\r\n");
}

TEST_CASE("RespWriter multi-arg command", "[resp2][writer]") {
    RespWriter w;
    const char* argv[] = {"SET", "key", "value"};
    size_t argvlen[] = {3, 3, 5};
    w.writeCommand(3, argv, argvlen);

    REQUIRE(std::string_view(w.data(), w.size()) ==
        "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
}

TEST_CASE("RespWriter binary data (NUL and CRLF)", "[resp2][writer]") {
    RespWriter w;
    // Data containing NUL byte and \r\n
    const char data[] = "he\0l\r\nlo";
    const char* argv[] = {"SET", "key", data};
    size_t argvlen[] = {3, 3, 8};
    w.writeCommand(3, argv, argvlen);

    // Verify the length-prefixed data is correct (binary safe)
    auto result = std::string(w.data(), w.size());
    REQUIRE(result.find("$8\r\n") != std::string::npos);
    // *3\r\n + $3\r\nSET\r\n + $3\r\nkey\r\n + $8\r\n<8 bytes>\r\n = 4+9+9+14 = 36
    REQUIRE(result.size() == 36);
}

TEST_CASE("RespWriter consume advances read position", "[resp2][writer]") {
    RespWriter w;
    const char* argv[] = {"PING"};
    size_t argvlen[] = {4};
    w.writeCommand(1, argv, argvlen);

    auto total = w.size();
    REQUIRE(total > 0);

    w.consume(5);
    REQUIRE(w.size() == total - 5);

    w.consume(w.size());
    REQUIRE(w.empty());
}

TEST_CASE("RespWriter multiple commands", "[resp2][writer]") {
    RespWriter w;

    const char* argv1[] = {"SET", "a", "1"};
    size_t argvlen1[] = {3, 1, 1};
    w.writeCommand(3, argv1, argvlen1);

    const char* argv2[] = {"GET", "a"};
    size_t argvlen2[] = {3, 1};
    w.writeCommand(2, argv2, argvlen2);

    auto result = std::string_view(w.data(), w.size());
    REQUIRE(result.starts_with("*3\r\n"));
    REQUIRE(result.find("*2\r\n") != std::string_view::npos);
}

TEST_CASE("RespWriter empty value", "[resp2][writer]") {
    RespWriter w;
    const char* argv[] = {"SET", "key", ""};
    size_t argvlen[] = {3, 3, 0};
    w.writeCommand(3, argv, argvlen);

    auto result = std::string_view(w.data(), w.size());
    REQUIRE(result.find("$0\r\n\r\n") != std::string_view::npos);
}

// =============================================================================
// RespParser — Simple String
// =============================================================================

TEST_CASE("RespParser simple string", "[resp2][parser]") {
    RespParser p;
    std::string data = "+OK\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::SimpleString);
    REQUIRE(p.getString(p.root()) == "OK");
}

// =============================================================================
// RespParser — Error
// =============================================================================

TEST_CASE("RespParser error", "[resp2][parser]") {
    RespParser p;
    std::string data = "-ERR unknown command\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Error);
    REQUIRE(p.getString(p.root()) == "ERR unknown command");
}

// =============================================================================
// RespParser — Integer
// =============================================================================

TEST_CASE("RespParser positive integer", "[resp2][parser]") {
    RespParser p;
    std::string data = ":1000\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Integer);
    REQUIRE(p.root().integer == 1000);
}

TEST_CASE("RespParser negative integer", "[resp2][parser]") {
    RespParser p;
    std::string data = ":-42\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Integer);
    REQUIRE(p.root().integer == -42);
}

TEST_CASE("RespParser zero integer", "[resp2][parser]") {
    RespParser p;
    std::string data = ":0\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().integer == 0);
}

// =============================================================================
// RespParser — Bulk String
// =============================================================================

TEST_CASE("RespParser bulk string", "[resp2][parser]") {
    RespParser p;
    std::string data = "$5\r\nhello\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::BulkString);
    REQUIRE(p.getString(p.root()) == "hello");
}

TEST_CASE("RespParser nil bulk string", "[resp2][parser]") {
    RespParser p;
    std::string data = "$-1\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Nil);
}

TEST_CASE("RespParser empty bulk string", "[resp2][parser]") {
    RespParser p;
    std::string data = "$0\r\n\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::BulkString);
    REQUIRE(p.getString(p.root()) == "");
    REQUIRE(p.root().str_len == 0);
}

TEST_CASE("RespParser bulk string with embedded CRLF", "[resp2][parser]") {
    RespParser p;
    // "he\r\nllo" = 7 bytes
    std::string data = "$7\r\nhe\r\nllo\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::BulkString);
    REQUIRE(p.getString(p.root()) == "he\r\nllo");
}

// =============================================================================
// RespParser — Array
// =============================================================================

TEST_CASE("RespParser simple array", "[resp2][parser]") {
    RespParser p;
    std::string data = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Array);
    REQUIRE(p.root().array_count == 2);

    auto& e0 = p.arrayElement(p.root(), 0);
    auto& e1 = p.arrayElement(p.root(), 1);
    REQUIRE(e0.type == RespValue::Type::BulkString);
    REQUIRE(p.getString(e0) == "foo");
    REQUIRE(p.getString(e1) == "bar");
}

TEST_CASE("RespParser empty array", "[resp2][parser]") {
    RespParser p;
    std::string data = "*0\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Array);
    REQUIRE(p.root().array_count == 0);
}

TEST_CASE("RespParser nil array", "[resp2][parser]") {
    RespParser p;
    std::string data = "*-1\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Nil);
}

TEST_CASE("RespParser nested array (SCAN response)", "[resp2][parser]") {
    RespParser p;
    // SCAN returns: *2\r\n$1\r\n0\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n
    std::string data = "*2\r\n$1\r\n0\r\n*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::Array);
    REQUIRE(p.root().array_count == 2);

    // First element: cursor "0"
    auto& cursor = p.arrayElement(p.root(), 0);
    REQUIRE(cursor.type == RespValue::Type::BulkString);
    REQUIRE(p.getString(cursor) == "0");

    // Second element: nested array of keys
    auto& keys = p.arrayElement(p.root(), 1);
    REQUIRE(keys.type == RespValue::Type::Array);
    REQUIRE(keys.array_count == 3);
    REQUIRE(p.getString(p.arrayElement(keys, 0)) == "key1");
    REQUIRE(p.getString(p.arrayElement(keys, 1)) == "key2");
    REQUIRE(p.getString(p.arrayElement(keys, 2)) == "key3");
}

TEST_CASE("RespParser mixed type array", "[resp2][parser]") {
    RespParser p;
    // Array with: integer, bulk string, nil, simple string
    std::string data = "*4\r\n:42\r\n$5\r\nhello\r\n$-1\r\n+OK\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().array_count == 4);

    auto& e0 = p.arrayElement(p.root(), 0);
    auto& e1 = p.arrayElement(p.root(), 1);
    auto& e2 = p.arrayElement(p.root(), 2);
    auto& e3 = p.arrayElement(p.root(), 3);

    REQUIRE(e0.type == RespValue::Type::Integer);
    REQUIRE(e0.integer == 42);
    REQUIRE(e1.type == RespValue::Type::BulkString);
    REQUIRE(p.getString(e1) == "hello");
    REQUIRE(e2.type == RespValue::Type::Nil);
    REQUIRE(e3.type == RespValue::Type::SimpleString);
    REQUIRE(p.getString(e3) == "OK");
}

// =============================================================================
// RespParser — Incremental parsing
// =============================================================================

TEST_CASE("RespParser incomplete data returns 0", "[resp2][parser]") {
    RespParser p;

    // Incomplete simple string (no \r\n)
    std::string data = "+OK";
    REQUIRE(p.parse(data.data(), data.size()) == 0);

    // Incomplete bulk string (header only)
    data = "$5\r\n";
    REQUIRE(p.parse(data.data(), data.size()) == 0);

    // Incomplete bulk string (partial data)
    data = "$5\r\nhel";
    REQUIRE(p.parse(data.data(), data.size()) == 0);

    // Incomplete bulk string (data but no trailing \r\n)
    data = "$5\r\nhello";
    REQUIRE(p.parse(data.data(), data.size()) == 0);

    // Incomplete array (header only)
    data = "*2\r\n";
    REQUIRE(p.parse(data.data(), data.size()) == 0);

    // Incomplete array (partial elements)
    data = "*2\r\n$3\r\nfoo\r\n";
    REQUIRE(p.parse(data.data(), data.size()) == 0);
}

TEST_CASE("RespParser byte-by-byte incremental parsing", "[resp2][parser]") {
    RespParser p;
    std::string full = "$5\r\nhello\r\n";

    // Feed one byte at a time until complete
    for (size_t len = 1; len < full.size(); ++len) {
        REQUIRE(p.parse(full.data(), len) == 0);
    }

    // Complete data
    auto consumed = p.parse(full.data(), full.size());
    REQUIRE(consumed == full.size());
    REQUIRE(p.getString(p.root()) == "hello");
}

TEST_CASE("RespParser extra data after complete response", "[resp2][parser]") {
    RespParser p;
    // Two responses concatenated
    std::string data = "+OK\r\n+NEXT\r\n";
    auto consumed = p.parse(data.data(), data.size());

    // Should only consume the first response
    REQUIRE(consumed == 5); // "+OK\r\n"
    REQUIRE(p.getString(p.root()) == "OK");
}

// =============================================================================
// RespParser — EVAL response (Lua script)
// =============================================================================

TEST_CASE("RespParser EVAL integer response", "[resp2][parser]") {
    RespParser p;
    std::string data = ":1\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed > 0);
    REQUIRE(p.root().type == RespValue::Type::Integer);
    REQUIRE(p.root().integer == 1);
}

TEST_CASE("RespParser EVAL array response", "[resp2][parser]") {
    RespParser p;
    // Lua table returned as array
    std::string data = "*2\r\n$3\r\nfoo\r\n:42\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed > 0);
    REQUIRE(p.root().type == RespValue::Type::Array);
    REQUIRE(p.root().array_count == 2);
}

// =============================================================================
// RespParser — Edge cases
// =============================================================================

TEST_CASE("RespParser empty input", "[resp2][parser]") {
    RespParser p;
    REQUIRE(p.parse("", 0) == 0);
}

TEST_CASE("RespParser large bulk string", "[resp2][parser]") {
    RespParser p;
    std::string payload(10000, 'x');
    std::string data = "$10000\r\n" + payload + "\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::BulkString);
    REQUIRE(p.getString(p.root()).size() == 10000);
}

TEST_CASE("RespParser bulk string with NUL bytes", "[resp2][parser]") {
    RespParser p;
    // Binary data with NUL
    std::string payload = "he";
    payload += '\0';
    payload += "lo";
    std::string data = "$5\r\n" + payload + "\r\n";
    auto consumed = p.parse(data.data(), data.size());

    REQUIRE(consumed == data.size());
    REQUIRE(p.root().type == RespValue::Type::BulkString);
    auto sv = p.getString(p.root());
    REQUIRE(sv.size() == 5);
    REQUIRE(sv[2] == '\0');
}
