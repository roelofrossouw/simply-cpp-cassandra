// Against a real Cassandra, when SC_CASSANDRA_TEST_HOSTS names one (host[:port], ';'-separated);
// skipped otherwise. Reads system tables only, so any server will do.

#include <cassandra.h>
#include <sc_test.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

int main() {
    using std::string;
    const char *hosts = std::getenv("SC_CASSANDRA_TEST_HOSTS");
    if (!hosts || !*hosts) {
        std::cout << "   (SC_CASSANDRA_TEST_HOSTS isn't set: no server to test against)" << std::endl;
        return 0;
    }
    sc::cassandra db{hosts};

    SECTION("Connected, and the server's version");
    CHECK(!db.release_version().empty());

    SECTION("The keyspaces, typed");
    {
        const auto rows = db.query("select keyspace_name, durable_writes from system_schema.keyspaces");
        CHECK(!rows.empty());
        CHECK(rows.columns()[1].type == sc::cql_type::boolean);
        bool system = false;
        for (const auto &row: rows) system = system || row["keyspace_name"].as<string>() == "system";
        CHECK(system);
    }

    SECTION("Bound values: text, and an int for LIMIT");
    {
        const auto rows = db.query("select table_name from system_schema.tables where keyspace_name = ? limit ?", {"system", 3});
        CHECK_EQ(rows.size(), std::size_t{3});
    }

    SECTION("A timestamp from the server, near the client's clock");
    {
        const auto rows = db.query("select toTimestamp(now()) as at from system.local");
        CHECK(rows.columns()[0].type == sc::cql_type::timestamp);
        const auto difference = rows[0]["at"].as<sc::cql_timestamp>() - std::chrono::system_clock::now();
        CHECK(std::chrono::abs(difference) < std::chrono::minutes{5});
    }

    SECTION("A syntax error is a cql_error with its code");
    {
        try {
            (void) db.query("selec nothing");
            CHECK_MSG(false, "should throw");
        } catch (const sc::cql_error &error) {
            CHECK_EQ(error.code, 0x2000);
        }
        CHECK_EQ(db.query("select release_version from system.local").size(), std::size_t{1}); // and the connection stays
    }

    TEST_SUMMARY();
}
