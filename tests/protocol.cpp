// The client against a small fake Cassandra on loopback, speaking just enough of protocol v4:
// STARTUP (with a login when it has a password), QUERY with bound values, Rows results of
// every type the client decodes, paging, errors, EVENT frames in between and dropped
// connections, so the framing and the decoding are checked without a real server.

#include <cassandra.h>
#include <sc_test.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <bit>
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {
    std::string be(const std::uint64_t value, const int bytes) {
        std::string out;
        for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) out += static_cast<char>((value >> shift) & 0xFF);
        return out;
    }
    std::string short_(const std::uint16_t v) { return be(v, 2); }
    std::string int_(const std::int32_t v) { return be(static_cast<std::uint32_t>(v), 4); }
    std::string string_(const std::string &text) { return short_(static_cast<std::uint16_t>(text.size())) + text; }
    std::string bytes_(const std::optional<std::string> &data) {
        return data ? int_(static_cast<std::int32_t>(data->size())) + *data : int_(-1);
    }
    std::uint64_t read_be(const std::string &data, const std::size_t at, const int bytes) {
        std::uint64_t v = 0;
        for (int i = 0; i < bytes; ++i) v = (v << 8) | static_cast<std::uint8_t>(data[at + i]);
        return v;
    }

    struct column {
        std::string name;
        std::uint16_t type;
        std::string option_extra; // nested type options, for collections
    };

    // A Rows result: global table spec ks.t, the columns, then the rows' cells.
    std::string rows_result(const std::vector<column> &columns, const std::vector<std::vector<std::optional<std::string>>> &rows,
                            const std::optional<std::string> &paging_state = std::nullopt) {
        std::string body = int_(2);
        body += int_(0x0001 | (paging_state ? 0x0002 : 0));
        body += int_(static_cast<std::int32_t>(columns.size()));
        if (paging_state) body += bytes_(*paging_state);
        body += string_("ks") + string_("t");
        for (const auto &each: columns) body += string_(each.name) + short_(each.type) + each.option_extra;
        body += int_(static_cast<std::int32_t>(rows.size()));
        for (const auto &row: rows)
            for (const auto &cell: row) body += bytes_(cell);
        return body;
    }

    class fake_cassandra {
    public:
        explicit fake_cassandra(std::string password = {}) : password_(std::move(password)) {
            listener_ = socket(AF_INET, SOCK_STREAM, 0);
            const int yes = 1;
            setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            if (bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof address) != 0 || listen(listener_, 16) != 0) {
                throw std::runtime_error("fake Cassandra: can't listen");
            }
            socklen_t size = sizeof address;
            getsockname(listener_, reinterpret_cast<sockaddr *>(&address), &size);
            port_ = ntohs(address.sin_port);
            thread_ = std::thread{[this] { serve(); }};
        }

        ~fake_cassandra() {
            stopping_ = true;
            shutdown(listener_, SHUT_RDWR);
            close(listener_);
            thread_.join();
        }

        [[nodiscard]] int port() const { return port_; }
        std::atomic<int> connections{0};
        std::atomic<bool> drop_next_query{false};
        std::atomic<bool> event_first{false};
        std::mutex seen_mutex;
        std::vector<std::string> queries;           // what came, in order
        std::vector<std::vector<std::optional<std::string>>> values; // each query's bound values
        std::vector<int> consistencies;

    private:
        void serve() {
            while (!stopping_) {
                pollfd ready{listener_, POLLIN, 0};
                if (poll(&ready, 1, 100) <= 0) continue;
                const int client = accept(listener_, nullptr, nullptr);
                if (client < 0) continue;
                ++connections;
                talk(client);
                close(client);
            }
        }

        static bool read_exact(const int fd, std::string &into, const std::size_t count) {
            into.assign(count, '\0');
            std::size_t got = 0;
            while (got < count) {
                const auto n = recv(fd, into.data() + got, count - got, 0);
                if (n <= 0) return false;
                got += static_cast<std::size_t>(n);
            }
            return true;
        }

        static void reply(const int fd, const std::uint16_t stream, const std::uint8_t opcode, const std::string &body) {
            const std::string frame = std::string{static_cast<char>(0x84), '\0'} + short_(stream) + static_cast<char>(opcode) +
                                      int_(static_cast<std::int32_t>(body.size())) + body;
            (void) send(fd, frame.data(), frame.size(), 0);
        }

        void talk(const int fd) {
            std::string head, body;
            while (read_exact(fd, head, 9)) {
                const auto stream = static_cast<std::uint16_t>(read_be(head, 2, 2));
                const auto opcode = static_cast<std::uint8_t>(head[4]);
                if (!read_exact(fd, body, read_be(head, 5, 4))) return;
                if (head[0] != 0x04) return reply(fd, stream, 0x00, int_(0x000A) + string_("Invalid or unsupported protocol version"));

                if (opcode == 0x01) { // STARTUP
                    if (password_.empty()) reply(fd, stream, 0x02, {});
                    else reply(fd, stream, 0x03, string_("org.apache.cassandra.auth.PasswordAuthenticator"));
                } else if (opcode == 0x0F) { // AUTH_RESPONSE: [bytes] \0user\0password
                    const auto token = body.substr(4);
                    if (token == std::string{'\0'} + "tester" + '\0' + password_) reply(fd, stream, 0x10, bytes_(std::nullopt));
                    else reply(fd, stream, 0x00, int_(0x0100) + string_("Provided username and/or password are incorrect"));
                } else if (opcode == 0x07) { // QUERY
                    std::size_t at = 0;
                    const auto length = read_be(body, at, 4);
                    const auto cql = body.substr(4, length);
                    at = 4 + length;
                    const auto consistency = static_cast<int>(read_be(body, at, 2));
                    const auto flags = static_cast<std::uint8_t>(body[at + 2]);
                    at += 3;
                    std::vector<std::optional<std::string>> bound;
                    if (flags & 0x01) {
                        auto n = read_be(body, at, 2);
                        at += 2;
                        for (; n > 0; --n) {
                            const auto size = static_cast<std::int32_t>(read_be(body, at, 4));
                            at += 4;
                            if (size < 0) {
                                bound.emplace_back(std::nullopt);
                            } else {
                                bound.emplace_back(body.substr(at, size));
                                at += size;
                            }
                        }
                    }
                    std::optional<std::string> paging_state;
                    if (flags & 0x04) at += 4; // page size
                    if (flags & 0x08) {
                        const auto size = read_be(body, at, 4);
                        paging_state = body.substr(at + 4, size);
                    }
                    if (cql != "SELECT release_version FROM system.local") {
                        const std::lock_guard lock{seen_mutex};
                        queries.push_back(cql);
                        values.push_back(bound);
                        consistencies.push_back(consistency);
                    }
                    if (cql != "SELECT release_version FROM system.local" && drop_next_query.exchange(false)) return; // hang up
                    if (event_first.exchange(false)) {
                        reply(fd, 0xFFFF, 0x0C, string_("STATUS_CHANGE") + string_("UP")); // stream -1, unasked for
                    }
                    answer(fd, stream, cql, bound, paging_state);
                } else {
                    reply(fd, stream, 0x00, int_(0x000A) + string_("unexpected opcode"));
                }
            }
        }

        static void answer(const int fd, const std::uint16_t stream, const std::string &cql,
                           const std::vector<std::optional<std::string>> &bound, const std::optional<std::string> &paging_state) {
            if (cql == "SELECT release_version FROM system.local") {
                return reply(fd, stream, 0x08, rows_result({{"release_version", 0x000D}}, {{std::string{"4.0.12"}}}));
            }
            if (cql.starts_with("USE ")) return reply(fd, stream, 0x08, int_(3) + string_("vehicle_data"));
            if (cql == "insert") return reply(fd, stream, 0x08, int_(1)); // Void
            if (cql == "bad") return reply(fd, stream, 0x00, int_(0x2000) + string_("line 1:0 no viable alternative at input 'bad'"));
            if (cql == "types") {
                const std::string uuid("\x12\x3e\x45\x67\xe8\x9b\x12\xd3\xa4\x56\x42\x66\x14\x17\x40\x00", 16);
                return reply(fd, stream, 0x08, rows_result(
                    {{"a", 0x0001}, {"big", 0x0002}, {"blob", 0x0003}, {"yes", 0x0004}, {"count", 0x0005}, {"d", 0x0007},
                     {"f", 0x0008}, {"i", 0x0009}, {"at", 0x000B}, {"id", 0x000C}, {"name", 0x000D}, {"tid", 0x000F},
                     {"ip", 0x0010}, {"day", 0x0011}, {"tod", 0x0012}, {"small", 0x0013}, {"tiny", 0x0014},
                     {"tags", 0x0020, short_(0x000D)}, {"scores", 0x0021, short_(0x000D) + short_(0x0009)}, {"missing", 0x0009}},
                    {{std::string{"plain"}, be(static_cast<std::uint64_t>(-5), 8), std::string{"\x01\x02", 2}, std::string{"\x01"},
                      be(7, 8), be(std::bit_cast<std::uint64_t>(-26.126786), 8), be(std::bit_cast<std::uint32_t>(1.5f), 4),
                      be(static_cast<std::uint32_t>(-274), 4), be(1792457577000ULL, 8), uuid, std::string{"tëxt"}, uuid,
                      std::string{"\x0a\x00\x00\x01", 4}, be((1ULL << 31) + 20746, 4), be(3600000000000ULL, 8),
                      be(static_cast<std::uint16_t>(-2), 2), std::string{"\xfe"}, std::string{"\x00\x00\x00\x00", 4}, std::string{"x"},
                      std::nullopt}}));
            }
            if (cql == "echo") { // the bound values back, as the types the test bound them as
                const std::vector<std::uint16_t> types{0x0009, 0x0002, 0x0007, 0x0008, 0x000D, 0x000B, 0x0004, 0x0003, 0x0009};
                std::vector<column> columns;
                for (std::size_t i = 0; i < bound.size(); ++i) columns.push_back({"v" + std::to_string(i), types.at(i)});
                return reply(fd, stream, 0x08, rows_result(columns, {bound}));
            }
            if (cql == "paged") {
                if (!paging_state) return reply(fd, stream, 0x08, rows_result({{"n", 0x0009}}, {{be(1, 4)}, {be(2, 4)}}, "page-2"));
                if (*paging_state == "page-2") return reply(fd, stream, 0x08, rows_result({{"n", 0x0009}}, {{be(3, 4)}}));
            }
            reply(fd, stream, 0x00, int_(0x2200) + string_("unknown query " + cql));
        }

        int listener_ = -1;
        int port_ = 0;
        std::string password_;
        std::atomic<bool> stopping_{false};
        std::thread thread_;
    };

    std::string hex(const std::string &bytes) {
        static const char digits[] = "0123456789abcdef";
        std::string out;
        for (const char c: bytes) {
            out += digits[static_cast<std::uint8_t>(c) >> 4];
            out += digits[static_cast<std::uint8_t>(c) & 0xF];
        }
        return out;
    }
}

int main() {
    using std::string;

    SECTION("Connecting: STARTUP, then the server's version");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}};
        CHECK_EQ(db.release_version(), string{"4.0.12"});
        CHECK_EQ(db.connected_to().port, server.port());
        CHECK_EQ(server.connections.load(), 1);
    }

    SECTION("Every type the client decodes, and the ones that arrive as blobs");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}};
        const auto rows = db.query("types");
        CHECK_EQ(rows.size(), std::size_t{1});
        CHECK_EQ(rows.columns().size(), std::size_t{20});
        CHECK_EQ(rows.columns()[0].keyspace, string{"ks"});
        CHECK_EQ(rows.columns()[0].table, string{"t"});
        CHECK(rows.columns()[17].type == sc::cql_type::list);
        CHECK_EQ(string{sc::cql_type_name(rows.columns()[10].type)}, string{"text"});
        const auto &row = rows[0];
        CHECK_EQ(row["a"].as<string>(), string{"plain"});
        CHECK_EQ(row["big"].as<std::int64_t>(), std::int64_t{-5});
        CHECK(row["blob"].as<sc::cql_blob>() == (sc::cql_blob{std::byte{1}, std::byte{2}}));
        CHECK_EQ(row["blob"].text(), string{"0x0102"});
        CHECK(row["yes"].as<bool>());
        CHECK_EQ(row["count"].as<std::int64_t>(), std::int64_t{7});
        CHECK_EQ(row["d"].as<double>(), -26.126786);
        CHECK_EQ(row["f"].as<float>(), 1.5f);
        CHECK_EQ(row["f"].as<double>(), 1.5); // a float widens
        CHECK_EQ(row["d"].text(), string{"-26.126786"}); // the shortest text that reads back the same
        CHECK_EQ(row["i"].as<std::int32_t>(), std::int32_t{-274});
        CHECK_EQ(row["i"].as<std::int64_t>(), std::int64_t{-274}); // an int widens
        CHECK(row["at"].as<sc::cql_timestamp>() == sc::cql_timestamp{std::chrono::milliseconds{1792457577000}});
        CHECK_EQ(row["at"].text(), string{"2026-10-20T00:52:57.000Z"});
        CHECK_EQ(row["id"].as<string>(), string{"123e4567-e89b-12d3-a456-426614174000"});
        CHECK_EQ(row["name"].as<string>(), string{"tëxt"});
        CHECK_EQ(row["tid"].as<string>(), string{"123e4567-e89b-12d3-a456-426614174000"});
        CHECK_EQ(row["ip"].as<string>(), string{"10.0.0.1"});
        CHECK_EQ(row["day"].as<string>(), string{"2026-10-20"});
        CHECK_EQ(row["tod"].as<std::int64_t>(), std::int64_t{3600000000000}); // 01:00, in nanoseconds
        CHECK_EQ(row["small"].as<std::int32_t>(), std::int32_t{-2});
        CHECK_EQ(row["tiny"].as<std::int32_t>(), std::int32_t{-2});
        CHECK_EQ(row["tags"].as<sc::cql_blob>().size(), std::size_t{4}); // a list: its bytes
        CHECK_EQ(row["scores"].as<sc::cql_blob>().size(), std::size_t{1}); // the map's nested types were read past
        CHECK(row["missing"].is_null());
        CHECK_EQ(row["missing"].text(), string{"null"});
        CHECK_THROWS_AS((void) row["nope"], std::out_of_range);
        CHECK_THROWS_AS((void) row["name"].as<std::int32_t>(), std::bad_variant_access);
    }

    SECTION("Bound values go as their C++ types say, and come back the same");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}};
        const auto at = std::chrono::system_clock::time_point{std::chrono::milliseconds{1792457577123}} + 456us;
        const std::vector<sc::cql_value> values{10, std::int64_t{389}, 2.5, 0.25f, "text", at, true,
                                                sc::cql_blob{std::byte{0xAB}}, nullptr};
        const auto rows = db.query("echo", values, sc::cql_consistency::local_quorum);
        {
            const std::lock_guard lock{server.seen_mutex};
            const auto &sent = server.values.back();
            CHECK_EQ(sent.size(), std::size_t{9});
            CHECK_EQ(hex(*sent[0]), string{"0000000a"});         // int: 4 bytes
            CHECK_EQ(hex(*sent[1]), string{"0000000000000185"}); // bigint: 8 bytes
            CHECK_EQ(hex(*sent[5]), string{"000001a156cc12a3"}); // timestamp: milliseconds, the rest dropped
            CHECK(!sent[8]);                                      // null: -1
            CHECK_EQ(server.consistencies.back(), 0x0006);
        }
        const auto &row = rows[0];
        CHECK_EQ(row[0].as<std::int32_t>(), 10);
        CHECK_EQ(row[1].as<std::int64_t>(), std::int64_t{389});
        CHECK_EQ(row[2].as<double>(), 2.5);
        CHECK_EQ(row[3].as<float>(), 0.25f);
        CHECK_EQ(row[4].as<string>(), string{"text"});
        CHECK(row[5].as<sc::cql_timestamp>() == sc::cql_timestamp{std::chrono::milliseconds{1792457577123}});
        CHECK(row[6].as<bool>());
        CHECK(row[7].as<sc::cql_blob>() == sc::cql_blob{std::byte{0xAB}});
        CHECK(row[8].is_null());
        CHECK(sc::cql_value{"x"} == sc::cql_value{string{"x"}});
    }

    SECTION("A result over several pages comes back whole");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}};
        const auto rows = db.query("paged");
        CHECK_EQ(rows.size(), std::size_t{3});
        CHECK_EQ(rows[2]["n"].as<std::int32_t>(), 3);
        const std::lock_guard lock{server.seen_mutex};
        CHECK_EQ(server.queries.size(), std::size_t{2}); // the second asked for page-2
    }

    SECTION("Nothing to return, an error, and EVENT frames in between");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}};
        const auto none = db.query("insert");
        CHECK(none.empty());
        CHECK(none.columns().empty());
        try {
            (void) db.query("bad");
            CHECK_MSG(false, "an error reply should throw");
        } catch (const sc::cql_error &error) {
            CHECK_EQ(error.code, 0x2000);
            CHECK(string{error.what()}.find("no viable alternative") != string::npos);
        }
        server.event_first = true;
        CHECK_EQ(db.query("paged").size(), std::size_t{3}); // the EVENT on stream -1 is skipped
        CHECK_EQ(server.connections.load(), 1);              // an error doesn't cost the connection
    }

    SECTION("A dropped connection is replaced once, transparently");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}};
        server.drop_next_query = true;
        CHECK_EQ(db.query("paged").size(), std::size_t{3});
        CHECK_EQ(server.connections.load(), 2);
    }

    SECTION("A keyspace is USE'd after connecting");
    {
        fake_cassandra server;
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}, {.keyspace = "vehicle_data"}};
        const std::lock_guard lock{server.seen_mutex};
        CHECK_EQ(server.queries.front(), string{"USE \"vehicle_data\""});
    }

    SECTION("A login, when the server asks for one");
    {
        fake_cassandra server{"secret"};
        {
            sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", server.port()}}, {.user = "tester", .password = "secret"}};
            CHECK_EQ(db.release_version(), string{"4.0.12"});
        } // the fake serves one connection at a time
        CHECK_THROWS_AS((sc::cassandra{sc::ip_endpoints{{"127.0.0.1", server.port()}}, {.user = "tester", .password = "wrong"}}),
                        sc::cql_error);
        CHECK_THROWS_AS((sc::cassandra{sc::ip_endpoints{{"127.0.0.1", server.port()}}}), sc::cql_error); // none configured
    }

    SECTION("No contact point answering: cassandra_unavailable, naming each");
    {
        int port = 0;
        { fake_cassandra gone; port = gone.port(); } // a port nothing listens on any more
        try {
            sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", port}}, {.connect_timeout = 500ms}};
            CHECK_MSG(false, "connecting should fail");
        } catch (const sc::cassandra_unavailable &error) {
            CHECK(string{error.what()}.find("127.0.0.1:" + std::to_string(port)) != string::npos);
        }
        CHECK_THROWS_AS(sc::cassandra{sc::ip_endpoints{}}, std::invalid_argument);
    }

    SECTION("The first contact point that answers, a missing port meaning 9042");
    {
        fake_cassandra server;
        int dead = 0;
        { fake_cassandra gone; dead = gone.port(); }
        sc::cassandra db{sc::ip_endpoints{{"127.0.0.1", dead}, {"127.0.0.1", server.port()}}, {.connect_timeout = 500ms}};
        CHECK_EQ(db.connected_to().port, server.port());
    }

    TEST_SUMMARY();
}
