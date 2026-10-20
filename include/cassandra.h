#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ip_endpoints.h>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

// A small Cassandra client: CQL queries with bound values over the native protocol (v4), straight
// over a socket, with no driver underneath. Rows come back with their columns' types.
//
//     sc::cassandra db{"vms:20001"};
//     const auto rows = db.query("select positioning_time, speed from vehicle_data.device_locus "
//                                "where vehicle_id = ? and positioning_time <= ? limit ?",
//                                {std::int64_t{389}, std::chrono::system_clock::now(), 10});
//     for (const auto &row: rows) row["speed"].as<double>();
namespace sc {
    // How many replicas must answer (CQL's consistency levels, in the protocol's numbering).
    enum class cql_consistency : std::uint16_t {
        any = 0x0000, one = 0x0001, two = 0x0002, three = 0x0003, quorum = 0x0004, all = 0x0005,
        local_quorum = 0x0006, each_quorum = 0x0007, serial = 0x0008, local_serial = 0x0009, local_one = 0x000A,
    };

    // A column's type, in the protocol's numbering. Types this client doesn't decode (decimal,
    // varint, duration, collections, user types, tuples, custom) arrive as their bytes, a blob.
    enum class cql_type : std::uint16_t {
        custom = 0x0000, ascii = 0x0001, bigint = 0x0002, blob = 0x0003, boolean = 0x0004, counter = 0x0005,
        decimal = 0x0006, double_ = 0x0007, float_ = 0x0008, int_ = 0x0009, timestamp = 0x000B, uuid = 0x000C,
        varchar = 0x000D, varint = 0x000E, timeuuid = 0x000F, inet = 0x0010, date = 0x0011, time = 0x0012,
        smallint = 0x0013, tinyint = 0x0014, duration = 0x0015, list = 0x0020, map = 0x0021, set = 0x0022,
        udt = 0x0030, tuple = 0x0031,
    };

    // The name CQL gives a type: "bigint", "text" (varchar), "list" ...
    std::string_view cql_type_name(cql_type type);

    using cql_timestamp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
    using cql_blob = std::vector<std::byte>;

    // A value: what a column holds, or what a ? in a query is bound to. The C++ type decides how a
    // bound value is sent: std::int32_t for int (and LIMIT ?), std::int64_t for bigint and counter,
    // double, float, bool, std::string for text and ascii (and uuid, inet and date as text),
    // a time_point for timestamp (to the millisecond), cql_blob for blob. Columns arrive the same
    // way: smallint and tinyint as std::int32_t, time (nanoseconds since midnight) as std::int64_t,
    // uuid and timeuuid as "8-4-4-4-12" text, inet as "10.0.0.1" or "::1", date as "2026-10-20".
    class cql_value {
    public:
        using variant = std::variant<std::monostate, bool, std::int32_t, std::int64_t, float, double, std::string,
                                     cql_timestamp, cql_blob>;

        cql_value() = default; // null
        cql_value(std::nullptr_t) {
        }
        cql_value(const bool value) : value_(value) {
        }
        cql_value(const std::int32_t value) : value_(value) {
        }
        cql_value(const std::int64_t value) : value_(value) {
        }
        cql_value(const float value) : value_(value) {
        }
        cql_value(const double value) : value_(value) {
        }
        cql_value(std::string value) : value_(std::move(value)) {
        }
        cql_value(const char *value) : value_(std::string{value}) {
        }
        cql_value(const std::string_view value) : value_(std::string{value}) {
        }
        cql_value(cql_blob value) : value_(std::move(value)) {
        }
        // Any system_clock time point, kept to the millisecond (a timestamp's resolution).
        template<typename Duration>
        cql_value(const std::chrono::time_point<std::chrono::system_clock, Duration> value)
            : value_(std::chrono::time_point_cast<std::chrono::milliseconds>(value)) {
        }

        [[nodiscard]] bool is_null() const { return std::holds_alternative<std::monostate>(value_); }
        [[nodiscard]] const variant &get() const { return value_; }

        // The value as T. Numbers convert between each other where nothing is lost from an
        // integer (an int as std::int64_t or double; a float as double); anything else throws
        // std::bad_variant_access, as does a null (check is_null() first).
        template<typename T>
        [[nodiscard]] T as() const;

        // The value as text, for showing it: numbers as written, a timestamp as ISO 8601 in UTC
        // ("2026-10-20T00:52:57.000Z"), a blob as 0x and hex, null as "null".
        [[nodiscard]] std::string text() const;

        bool operator==(const cql_value &) const = default;

    private:
        variant value_;
    };

    struct cql_column {
        std::string keyspace;
        std::string table;
        std::string name;
        cql_type type = cql_type::blob;
    };

    // One row of a result, its values in the columns' order.
    class cql_row {
    public:
        cql_row(std::shared_ptr<const std::vector<cql_column>> columns, std::vector<cql_value> values)
            : columns_(std::move(columns)), values_(std::move(values)) {
        }

        [[nodiscard]] std::size_t size() const { return values_.size(); }
        [[nodiscard]] const cql_value &operator[](const std::size_t index) const { return values_.at(index); }
        // By column name; throws std::out_of_range for a name the result doesn't have.
        [[nodiscard]] const cql_value &operator[](std::string_view name) const;
        [[nodiscard]] const std::vector<cql_value> &values() const { return values_; }
        [[nodiscard]] const std::vector<cql_column> &columns() const { return *columns_; }

    private:
        std::shared_ptr<const std::vector<cql_column>> columns_;
        std::vector<cql_value> values_;
    };

    // What a query returned: the rows of a SELECT (all pages of them), or none.
    class cql_rows {
    public:
        cql_rows() : columns_(std::make_shared<std::vector<cql_column>>()) {
        }
        cql_rows(std::shared_ptr<const std::vector<cql_column>> columns, std::vector<cql_row> rows)
            : columns_(std::move(columns)), rows_(std::move(rows)) {
        }

        [[nodiscard]] const std::vector<cql_column> &columns() const { return *columns_; }
        [[nodiscard]] std::size_t size() const { return rows_.size(); }
        [[nodiscard]] bool empty() const { return rows_.empty(); }
        [[nodiscard]] const cql_row &operator[](const std::size_t index) const { return rows_.at(index); }
        [[nodiscard]] auto begin() const { return rows_.begin(); }
        [[nodiscard]] auto end() const { return rows_.end(); }

    private:
        std::shared_ptr<const std::vector<cql_column>> columns_;
        std::vector<cql_row> rows_;
    };

    // Cassandra couldn't be reached: no contact point answered, or a connection broke or timed out
    // (after the one transparent retry).
    struct cassandra_unavailable : std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    // Cassandra answered with an error (a syntax error, an unknown table, a timeout on its side ...);
    // code is the protocol's error code (0x2000 syntax error, 0x2200 invalid, 0x1200 read timeout ...).
    struct cql_error : std::runtime_error {
        cql_error(const std::int32_t code, const std::string &message) : std::runtime_error(message), code(code) {
        }
        std::int32_t code;
    };

    struct cassandra_options {
        std::string user;     // with PasswordAuthenticator; empty: no login
        std::string password;
        std::string keyspace; // USE'd after connecting; empty: name keyspaces in the queries
        cql_consistency consistency = cql_consistency::one;
        std::chrono::milliseconds connect_timeout{2000};
        std::chrono::milliseconds request_timeout{10000}; // a reply's wait; 0: however long
    };

    // One connection to the first contact point that answers (a contact point without a port uses
    // 9042), shared by the
    // threads using it: queries take turns. A connection that broke is replaced once,
    // transparently, trying the contact points again.
    class cassandra {
    public:
        explicit cassandra(ip_endpoints contact_points);
        cassandra(ip_endpoints contact_points, cassandra_options options);
        ~cassandra();

        cassandra(const cassandra &) = delete;
        cassandra &operator=(const cassandra &) = delete;

        // Runs a CQL statement, its ? markers bound to values in order, with the options'
        // consistency (or the one given). Gives the rows of a SELECT, all its pages; an empty
        // result for anything else.
        cql_rows query(std::string_view cql, const std::vector<cql_value> &values = {}) const;
        cql_rows query(std::string_view cql, const std::vector<cql_value> &values, cql_consistency consistency) const;

        // The server's version (system.local's release_version), as read when connecting.
        [[nodiscard]] std::string release_version() const;
        // Where it's connected now.
        [[nodiscard]] ip_endpoint connected_to() const;

    private:
        class implementation;
        std::unique_ptr<implementation> implementation_;
    };

    template<typename T>
    T cql_value::as() const {
        if constexpr (std::is_same_v<T, std::int64_t>) {
            if (const auto *small = std::get_if<std::int32_t>(&value_)) return *small;
            return std::get<std::int64_t>(value_);
        } else if constexpr (std::is_same_v<T, double>) {
            if (const auto *single = std::get_if<float>(&value_)) return *single;
            if (const auto *small = std::get_if<std::int32_t>(&value_)) return *small;
            return std::get<double>(value_);
        } else {
            return std::get<T>(value_);
        }
    }
}
