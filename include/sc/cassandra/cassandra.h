#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <sc/core/ip_endpoints.h>
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
//     sc::cassandra::client db{"cassandra.example.com:9042"};
//     const auto rows = db.query("select ordered_at, total from shop.orders "
//                                "where customer_id = ? and ordered_at <= ? limit ?",
//                                {std::int64_t{389}, std::chrono::system_clock::now(), 10});
//     for (const auto &row: rows) row["total"].as<double>();
namespace sc::cassandra {
    // How many replicas must answer (CQL's consistency levels, in the protocol's numbering).
    enum class consistency_level : std::uint16_t {
        any = 0x0000, one = 0x0001, two = 0x0002, three = 0x0003, quorum = 0x0004, all = 0x0005,
        local_quorum = 0x0006, each_quorum = 0x0007, serial = 0x0008, local_serial = 0x0009, local_one = 0x000A,
    };

    // A column's type, in the protocol's numbering. Types this client doesn't decode (decimal,
    // varint, duration, collections, user types, tuples, custom) arrive as their bytes, a blob.
    enum class column_type : std::uint16_t {
        custom = 0x0000, ascii = 0x0001, bigint = 0x0002, blob = 0x0003, boolean = 0x0004, counter = 0x0005,
        decimal = 0x0006, double_ = 0x0007, float_ = 0x0008, int_ = 0x0009, timestamp = 0x000B, uuid = 0x000C,
        varchar = 0x000D, varint = 0x000E, timeuuid = 0x000F, inet = 0x0010, date = 0x0011, time = 0x0012,
        smallint = 0x0013, tinyint = 0x0014, duration = 0x0015, list = 0x0020, map = 0x0021, set = 0x0022,
        udt = 0x0030, tuple = 0x0031,
    };

    // The name CQL gives a type: "bigint", "text" (varchar), "list" ...
    std::string_view column_type_name(column_type type);

    using timestamp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
    using blob = std::vector<std::byte>;

    // A value: what a column holds, or what a ? in a query is bound to. The C++ type decides how a
    // bound value is sent: std::int32_t for int (and LIMIT ?), std::int64_t for bigint and counter,
    // double, float, bool, std::string for text and ascii (and uuid, inet and date as text),
    // a time_point for timestamp (to the millisecond), blob for blob. Columns arrive the same
    // way: smallint and tinyint as std::int32_t, time (nanoseconds since midnight) as std::int64_t,
    // uuid and timeuuid as "8-4-4-4-12" text, inet as "10.0.0.1" or "::1", date as "2026-10-20".
    class value {
    public:
        using variant = std::variant<std::monostate, bool, std::int32_t, std::int64_t, float, double, std::string,
                                     timestamp, blob>;

        value() = default; // null
        value(std::nullptr_t) {
        }
        value(const bool value) : value_(value) {
        }
        value(const std::int32_t value) : value_(value) {
        }
        value(const std::int64_t value) : value_(value) {
        }
        value(const float value) : value_(value) {
        }
        value(const double value) : value_(value) {
        }
        value(std::string value) : value_(std::move(value)) {
        }
        value(const char *value) : value_(std::string{value}) {
        }
        value(const std::string_view value) : value_(std::string{value}) {
        }
        value(blob value) : value_(std::move(value)) {
        }
        // Any system_clock time point, kept to the millisecond (a timestamp's resolution).
        template<typename Duration>
        value(const std::chrono::time_point<std::chrono::system_clock, Duration> value)
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

        bool operator==(const value &) const = default;

    private:
        variant value_;
    };

    struct column {
        std::string keyspace;
        std::string table;
        std::string name;
        column_type type = column_type::blob;
    };

    // Values in the columns' order.
    class row {
    public:
        row(std::shared_ptr<const std::vector<column>> columns, std::vector<value> values)
            : columns_(std::move(columns)), values_(std::move(values)) {
        }

        [[nodiscard]] std::size_t size() const { return values_.size(); }
        [[nodiscard]] const value &operator[](const std::size_t index) const { return values_.at(index); }
        // By column name; throws std::out_of_range for a name the result doesn't have.
        [[nodiscard]] const value &operator[](std::string_view name) const;
        [[nodiscard]] const std::vector<value> &values() const { return values_; }
        [[nodiscard]] const std::vector<column> &columns() const { return *columns_; }

    private:
        std::shared_ptr<const std::vector<column>> columns_;
        std::vector<value> values_;
    };

    // What a query returned: the rows of a SELECT (all pages of them), or none.
    class rows {
    public:
        rows() : columns_(std::make_shared<std::vector<column>>()) {
        }
        rows(std::shared_ptr<const std::vector<column>> columns, std::vector<row> rows)
            : columns_(std::move(columns)), rows_(std::move(rows)) {
        }

        [[nodiscard]] const std::vector<column> &columns() const { return *columns_; }
        [[nodiscard]] std::size_t size() const { return rows_.size(); }
        [[nodiscard]] bool empty() const { return rows_.empty(); }
        [[nodiscard]] const row &operator[](const std::size_t index) const { return rows_.at(index); }
        [[nodiscard]] auto begin() const { return rows_.begin(); }
        [[nodiscard]] auto end() const { return rows_.end(); }

    private:
        std::shared_ptr<const std::vector<column>> columns_;
        std::vector<row> rows_;
    };

    // Cassandra couldn't be reached: no contact point answered, or a connection broke or timed out
    // (after the one transparent retry).
    struct unavailable : std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    // Cassandra answered with an error (a syntax error, an unknown table, a timeout on its side ...);
    // code is the protocol's error code (0x2000 syntax error, 0x2200 invalid, 0x1200 read timeout ...).
    struct error : std::runtime_error {
        error(const std::int32_t code, const std::string &message) : std::runtime_error(message), code(code) {
        }
        std::int32_t code;
    };

    struct options {
        std::string user;     // with PasswordAuthenticator; empty: no login
        std::string password;
        std::string keyspace; // USE'd after connecting; empty: name keyspaces in the queries
        consistency_level consistency = consistency_level::one;
        std::chrono::milliseconds connect_timeout{2000};
        std::chrono::milliseconds request_timeout{10000}; // a reply's wait; 0: however long
    };

    // One connection to the first contact point that answers (a contact point without a port uses
    // 9042), shared by the threads using it: queries take turns. A connection that broke is
    // replaced once, transparently, trying the contact points again.
    // A Cassandra connection (sc::cassandra until 3.0.0).
    class client {
    public:
        explicit client(ip_endpoints contact_points);
        client(ip_endpoints contact_points, cassandra::options settings);
        ~client();

        client(const client &) = delete;
        client &operator=(const client &) = delete;

        // Runs a CQL statement, its ? markers bound to values in order, with the options'
        // consistency (or the one given). Gives the rows of a SELECT, all its pages; an empty
        // result for anything else.
        rows query(std::string_view cql, const std::vector<value> &values = {}) const;
        rows query(std::string_view cql, const std::vector<value> &values, consistency_level consistency) const;

        // The server's version (system.local's release_version), as read when connecting.
        [[nodiscard]] std::string release_version() const;
        [[nodiscard]] ip_endpoint connected_to() const;

    private:
        class implementation;
        std::unique_ptr<implementation> implementation_;
    };

    template<typename T>
    T value::as() const {
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
