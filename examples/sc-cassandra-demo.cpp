// Runs a CQL query and prints its rows as a table: the columns with their types, then each row.
// Without a query it shows the server's version and its keyspaces.
//
//     sc-cassandra-demo --hosts vms:20001 "select vehicle_id, positioning_time, speed
//         from vehicle_data.device_locus where vehicle_id = 389 limit 5"

#include <cassandra.h>
#include <params.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

namespace {
    void print(const sc::cql_rows &rows) {
        const auto &columns = rows.columns();
        std::vector<std::size_t> widths;
        std::vector<std::string> heads;
        for (const auto &column: columns) {
            heads.push_back(column.name + " (" + std::string{sc::cql_type_name(column.type)} + ")");
            widths.push_back(heads.back().size());
        }
        std::vector<std::vector<std::string>> cells;
        for (const auto &row: rows) {
            auto &line = cells.emplace_back();
            for (std::size_t i = 0; i < row.size(); ++i) {
                line.push_back(row[i].text());
                widths[i] = std::max(widths[i], line.back().size());
            }
        }
        const auto show = [&](const std::vector<std::string> &values) {
            for (std::size_t i = 0; i < values.size(); ++i) {
                std::cout << values[i];
                if (i + 1 < values.size()) std::cout << std::string(widths[i] - values[i].size() + 2, ' ');
            }
            std::cout << '\n';
        };
        show(heads);
        for (const auto &line: cells) show(line);
        std::cout << rows.size() << (rows.size() == 1 ? " row\n" : " rows\n");
    }
}

int main(int argc, char **argv) {
    const sc::params args{argc, argv, {
        {"name", "sc-cassandra-demo"},
        {"description", "Runs a CQL query against Cassandra and prints the rows with their columns' types. Without a "
                        "query, the server's version and its keyspaces."},
        {"options", {
            {"hosts", {{"short", "H"}, {"value", "HOSTS"}, {"default", "127.0.0.1:9042"},
                       {"help", "contact points, host[:port] separated by ';' (9042 without a port)"}}},
            {"user", {{"short", "u"}, {"value", "NAME"}, {"default", ""}, {"help", "a login, for PasswordAuthenticator"}}},
            {"password", {{"short", "p"}, {"value", "TEXT"}, {"default", ""}, {"help", "the login's password"}}},
            {"keyspace", {{"short", "k"}, {"value", "NAME"}, {"default", ""}, {"help", "USE this keyspace first"}}}}},
        {"positional", {
            {"query", {{"default", ""}, {"help", "the CQL to run"}}}}}}};

    try {
        // [readme]
        sc::cassandra db{args.as<std::string>("hosts"), {
            .user = args.as<std::string>("user"),
            .password = args.as<std::string>("password"),
            .keyspace = args.as<std::string>("keyspace"),
        }};
        std::cout << "Cassandra " << db.release_version() << " at " << db.connected_to().to_string() << "\n\n";
        const auto cql = args.as<std::string>("query");
        print(cql.empty() ? db.query("select keyspace_name, durable_writes from system_schema.keyspaces") : db.query(cql));
        // [/readme]
    } catch (const std::exception &error) {
        std::cerr << "sc-cassandra-demo: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
