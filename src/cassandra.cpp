#include "cassandra.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cerrno>
#include <charconv>
#include <iterator>
#include <cstring>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <utility>

// The CQL native protocol, version 4: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec
namespace {
    constexpr std::uint8_t protocol_version = 0x04;
    constexpr std::uint8_t response_flag = 0x80;

    enum class opcode : std::uint8_t {
        error = 0x00, startup = 0x01, ready = 0x02, authenticate = 0x03, query = 0x07, result = 0x08,
        event = 0x0C, auth_challenge = 0x0E, auth_response = 0x0F, auth_success = 0x10,
    };

    enum class result_kind : std::int32_t { void_ = 1, rows = 2, set_keyspace = 3, prepared = 4, schema_change = 5 };

    // QUERY flags and Rows metadata flags.
    constexpr std::uint8_t query_values = 0x01, query_page_size = 0x04, query_paging_state = 0x08;
    constexpr std::int32_t rows_global_table_spec = 0x0001, rows_has_more_pages = 0x0002, rows_no_metadata = 0x0004;

    // A broken or timed-out connection, as opposed to Cassandra answering with an error.
    struct connection_lost : sc::cassandra_unavailable {
        using sc::cassandra_unavailable::cassandra_unavailable;
    };

    // Big-endian protocol notation, written.
    class writer {
    public:
        void byte(const std::uint8_t value) { bytes_.push_back(static_cast<char>(value)); }
        void short_(const std::uint16_t value) {
            byte(static_cast<std::uint8_t>(value >> 8));
            byte(static_cast<std::uint8_t>(value));
        }
        void int_(const std::int32_t value) { integer(value); }
        void long_(const std::int64_t value) { integer(value); }
        void raw(const std::string_view data) { bytes_.append(data); }
        void string(const std::string_view text) { // [string]: a short length
            if (text.size() > 0xFFFF) throw std::invalid_argument("CQL: a [string] over 65535 bytes");
            short_(static_cast<std::uint16_t>(text.size()));
            raw(text);
        }
        void long_string(const std::string_view text) { // [long string]: an int length
            int_(static_cast<std::int32_t>(text.size()));
            raw(text);
        }
        void bytes(const std::optional<std::string_view> data) { // [bytes]: -1 for null
            if (!data) return int_(-1);
            int_(static_cast<std::int32_t>(data->size()));
            raw(*data);
        }
        void string_map(const std::map<std::string, std::string> &map) {
            short_(static_cast<std::uint16_t>(map.size()));
            for (const auto &[key, value]: map) {
                string(key);
                string(value);
            }
        }
        [[nodiscard]] const std::string &data() const { return bytes_; }

    private:
        template<typename Integer>
        void integer(const Integer value) {
            const auto bits = static_cast<std::make_unsigned_t<Integer>>(value);
            for (int shift = 8 * (sizeof bits - 1); shift >= 0; shift -= 8) byte(static_cast<std::uint8_t>(bits >> shift));
        }

        std::string bytes_;
    };

    template<typename Integer>
    Integer big_endian(const std::string_view bytes) {
        std::uint64_t bits = 0;
        for (const char c: bytes) bits = (bits << 8) | static_cast<std::uint8_t>(c);
        return static_cast<Integer>(bits);
    }

    // Big-endian protocol notation, read; throws on running past the end.
    class reader {
    public:
        explicit reader(const std::string_view data) : data_(data) {
        }
        std::uint8_t byte() { return static_cast<std::uint8_t>(take(1)[0]); }
        std::uint16_t short_() { return big_endian<std::uint16_t>(take(2)); }
        std::int32_t int_() { return big_endian<std::int32_t>(take(4)); }
        std::string string() { return std::string{take(short_())}; }
        std::optional<std::string_view> bytes() {
            const auto length = int_();
            if (length < 0) return std::nullopt;
            return take(static_cast<std::size_t>(length));
        }
        std::string_view take(const std::size_t count) {
            if (count > data_.size() - position_) throw std::runtime_error("CQL: a reply shorter than it says");
            const auto part = data_.substr(position_, count);
            position_ += count;
            return part;
        }
        [[nodiscard]] bool at_end() const { return position_ == data_.size(); }

    private:
        std::string_view data_;
        std::size_t position_ = 0;
    };

    // A column type's [option], with what collections and user types nest consumed; the nested
    // types aren't kept, as those columns arrive as blobs.
    sc::cql_type read_type(reader &in) {
        const auto id = static_cast<sc::cql_type>(in.short_());
        switch (id) {
            case sc::cql_type::custom: (void) in.string(); break;
            case sc::cql_type::list:
            case sc::cql_type::set: (void) read_type(in); break;
            case sc::cql_type::map: (void) read_type(in); (void) read_type(in); break;
            case sc::cql_type::udt: {
                (void) in.string(); // keyspace
                (void) in.string(); // type name
                for (int fields = in.short_(); fields > 0; --fields) {
                    (void) in.string();
                    (void) read_type(in);
                }
                break;
            }
            case sc::cql_type::tuple:
                for (int elements = in.short_(); elements > 0; --elements) (void) read_type(in);
                break;
            default: break;
        }
        return id;
    }

    std::string uuid_text(const std::string_view bytes) {
        std::ostringstream out;
        out << std::hex << std::setfill('0');
        for (std::size_t i = 0; i < bytes.size(); ++i) {
            if (i == 4 || i == 6 || i == 8 || i == 10) out << '-';
            out << std::setw(2) << static_cast<int>(static_cast<std::uint8_t>(bytes[i]));
        }
        return out.str();
    }

    // Days since 1970-01-01 as "YYYY-MM-DD" (proleptic Gregorian, Howard Hinnant's civil_from_days:
    // older standard libraries lack std::chrono's calendar and std::format).
    std::string civil_date(std::int64_t days) {
        days += 719468;
        const std::int64_t era = (days >= 0 ? days : days - 146096) / 146097;
        const auto day_of_era = static_cast<std::uint64_t>(days - era * 146097);
        const auto year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
        const auto day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        const auto shifted_month = (5 * day_of_year + 2) / 153;
        const auto day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
        const auto month = shifted_month < 10 ? shifted_month + 3 : shifted_month - 9;
        const auto year = static_cast<std::int64_t>(year_of_era) + era * 400 + (month <= 2 ? 1 : 0);
        std::ostringstream out;
        out << std::setfill('0') << std::setw(4) << year << '-' << std::setw(2) << month << '-' << std::setw(2) << day;
        return out.str();
    }

    std::string date_text(const std::uint32_t days) { // 2^31 is 1970-01-01
        return civil_date(static_cast<std::int64_t>(days) - (std::int64_t{1} << 31));
    }

    sc::cql_value decode(const sc::cql_type type, const std::optional<std::string_view> cell) {
        using sc::cql_type;
        if (!cell) return {};
        const auto bytes = *cell;
        const auto needs = [&](const std::size_t size) {
            if (bytes.size() != size) {
                throw std::runtime_error("CQL: a " + std::string{sc::cql_type_name(type)} + " of " + std::to_string(bytes.size()) +
                                         " bytes");
            }
        };
        switch (type) {
            case cql_type::ascii:
            case cql_type::varchar: return std::string{bytes};
            case cql_type::boolean: needs(1); return bytes[0] != 0;
            case cql_type::int_: needs(4); return big_endian<std::int32_t>(bytes);
            case cql_type::smallint: needs(2); return static_cast<std::int32_t>(big_endian<std::int16_t>(bytes));
            case cql_type::tinyint: needs(1); return static_cast<std::int32_t>(static_cast<std::int8_t>(bytes[0]));
            case cql_type::bigint:
            case cql_type::counter:
            case cql_type::time: needs(8); return big_endian<std::int64_t>(bytes);
            case cql_type::float_: needs(4); return std::bit_cast<float>(big_endian<std::uint32_t>(bytes));
            case cql_type::double_: needs(8); return std::bit_cast<double>(big_endian<std::uint64_t>(bytes));
            case cql_type::timestamp:
                needs(8);
                return sc::cql_timestamp{std::chrono::milliseconds{big_endian<std::int64_t>(bytes)}};
            case cql_type::uuid:
            case cql_type::timeuuid: needs(16); return uuid_text(bytes);
            case cql_type::date: needs(4); return date_text(big_endian<std::uint32_t>(bytes));
            case cql_type::inet: {
                char text[INET6_ADDRSTRLEN]{};
                if (bytes.size() == 4 || bytes.size() == 16) {
                    inet_ntop(bytes.size() == 4 ? AF_INET : AF_INET6, bytes.data(), text, sizeof text);
                    return std::string{text};
                }
                break;
            }
            default: break;
        }
        sc::cql_blob blob(bytes.size());
        std::memcpy(blob.data(), bytes.data(), bytes.size());
        return blob;
    }

    // A bound value's [value] bytes, by its C++ type.
    std::optional<std::string> encode(const sc::cql_value &value) {
        writer out;
        const bool null = std::visit([&]<typename T>(const T &held) {
            if constexpr (std::is_same_v<T, std::monostate>) {
                return true;
            } else if constexpr (std::is_same_v<T, bool>) {
                out.byte(held ? 1 : 0);
            } else if constexpr (std::is_same_v<T, std::int32_t>) {
                out.int_(held);
            } else if constexpr (std::is_same_v<T, std::int64_t>) {
                out.long_(held);
            } else if constexpr (std::is_same_v<T, float>) {
                out.int_(std::bit_cast<std::int32_t>(held));
            } else if constexpr (std::is_same_v<T, double>) {
                out.long_(std::bit_cast<std::int64_t>(held));
            } else if constexpr (std::is_same_v<T, std::string>) {
                out.raw(held);
            } else if constexpr (std::is_same_v<T, sc::cql_timestamp>) {
                out.long_(held.time_since_epoch().count());
            } else if constexpr (std::is_same_v<T, sc::cql_blob>) {
                out.raw({reinterpret_cast<const char *>(held.data()), held.size()});
            }
            return false;
        }, value.get());
        if (null) return std::nullopt;
        return out.data();
    }

    // A blocking TCP connection with timeouts on connecting and on each reply.
    class connection {
    public:
        connection(const sc::ip_endpoint &endpoint, const std::chrono::milliseconds connect_timeout,
                   const std::chrono::milliseconds request_timeout)
            : request_timeout_(request_timeout) {
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            addrinfo *found = nullptr;
            const auto port = std::to_string(endpoint.port);
            if (const int error = getaddrinfo(endpoint.host.c_str(), port.c_str(), &hints, &found); error != 0) {
                throw connection_lost(endpoint.to_string() + ": " + gai_strerror(error));
            }
            std::string failure = "no address";
            for (const addrinfo *each = found; each && fd_ < 0; each = each->ai_next) {
                const int fd = socket(each->ai_family, each->ai_socktype, each->ai_protocol);
                if (fd < 0) continue;
                if (connect_within(fd, each->ai_addr, each->ai_addrlen, connect_timeout, failure)) {
                    fd_ = fd;
                } else {
                    close(fd);
                }
            }
            freeaddrinfo(found);
            if (fd_ < 0) throw connection_lost(endpoint.to_string() + ": " + failure);
            const int yes = 1;
            setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof yes);
#ifdef SO_NOSIGPIPE
            setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof yes);
#endif
        }

        ~connection() {
            if (fd_ >= 0) close(fd_);
        }

        connection(const connection &) = delete;
        connection &operator=(const connection &) = delete;

        void send(const std::string &data) const {
            std::size_t sent = 0;
            while (sent < data.size()) {
#ifdef MSG_NOSIGNAL
                const auto n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
#else
                const auto n = ::send(fd_, data.data() + sent, data.size() - sent, 0);
#endif
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) throw connection_lost(std::string{"sending: "} + std::strerror(errno));
                sent += static_cast<std::size_t>(n);
            }
        }

        std::string receive(const std::size_t count) const {
            std::string data(count, '\0');
            std::size_t got = 0;
            while (got < count) {
                if (request_timeout_.count() > 0) {
                    pollfd ready{fd_, POLLIN, 0};
                    const int polled = poll(&ready, 1, static_cast<int>(request_timeout_.count()));
                    if (polled < 0 && errno == EINTR) continue;
                    if (polled == 0) throw connection_lost("no reply within " + std::to_string(request_timeout_.count()) + " ms");
                    if (polled < 0) throw connection_lost(std::string{"waiting: "} + std::strerror(errno));
                }
                const auto n = recv(fd_, data.data() + got, count - got, 0);
                if (n < 0 && errno == EINTR) continue;
                if (n == 0) throw connection_lost("the server closed the connection");
                if (n < 0) throw connection_lost(std::string{"receiving: "} + std::strerror(errno));
                got += static_cast<std::size_t>(n);
            }
            return data;
        }

    private:
        static bool connect_within(const int fd, const sockaddr *address, const socklen_t length,
                                   const std::chrono::milliseconds timeout, std::string &failure) {
            const int flags = fcntl(fd, F_GETFL, 0);
            fcntl(fd, F_SETFL, flags | O_NONBLOCK);
            int result = connect(fd, address, length);
            if (result != 0 && errno == EINPROGRESS) {
                pollfd ready{fd, POLLOUT, 0};
                result = poll(&ready, 1, static_cast<int>(timeout.count()));
                if (result == 0) {
                    failure = "no connection within " + std::to_string(timeout.count()) + " ms";
                    return false;
                }
                int error = 0;
                socklen_t size = sizeof error;
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size);
                if (result < 0 || error != 0) {
                    failure = std::strerror(error ? error : errno);
                    return false;
                }
            } else if (result != 0) {
                failure = std::strerror(errno);
                return false;
            }
            fcntl(fd, F_SETFL, flags);
            return true;
        }

        int fd_ = -1;
        std::chrono::milliseconds request_timeout_;
    };

    struct frame {
        opcode code;
        std::string body;
    };
}

std::string_view sc::cql_type_name(const cql_type type) {
    switch (type) {
        case cql_type::custom: return "custom";
        case cql_type::ascii: return "ascii";
        case cql_type::bigint: return "bigint";
        case cql_type::blob: return "blob";
        case cql_type::boolean: return "boolean";
        case cql_type::counter: return "counter";
        case cql_type::decimal: return "decimal";
        case cql_type::double_: return "double";
        case cql_type::float_: return "float";
        case cql_type::int_: return "int";
        case cql_type::timestamp: return "timestamp";
        case cql_type::uuid: return "uuid";
        case cql_type::varchar: return "text";
        case cql_type::varint: return "varint";
        case cql_type::timeuuid: return "timeuuid";
        case cql_type::inet: return "inet";
        case cql_type::date: return "date";
        case cql_type::time: return "time";
        case cql_type::smallint: return "smallint";
        case cql_type::tinyint: return "tinyint";
        case cql_type::duration: return "duration";
        case cql_type::list: return "list";
        case cql_type::map: return "map";
        case cql_type::set: return "set";
        case cql_type::udt: return "udt";
        case cql_type::tuple: return "tuple";
    }
    return "unknown";
}

std::string sc::cql_value::text() const {
    return std::visit([]<typename T>(const T &held) -> std::string {
        if constexpr (std::is_same_v<T, std::monostate>) {
            return "null";
        } else if constexpr (std::is_same_v<T, bool>) {
            return held ? "true" : "false";
        } else if constexpr (std::is_same_v<T, std::string>) {
            return held;
        } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
            char text[64];
            const auto written = std::to_chars(std::begin(text), std::end(text), held); // the shortest that reads back the same
            return std::string(text, written.ptr);
        } else if constexpr (std::is_same_v<T, cql_timestamp>) {
            const auto ms = held.time_since_epoch().count();
            const auto days = (ms >= 0 ? ms : ms - 86399999) / 86400000;
            const auto of_day = ms - days * 86400000;
            std::ostringstream out;
            out << civil_date(days) << 'T' << std::setfill('0') << std::setw(2) << of_day / 3600000 << ':' << std::setw(2)
                << of_day / 60000 % 60 << ':' << std::setw(2) << of_day / 1000 % 60 << '.' << std::setw(3) << of_day % 1000 << 'Z';
            return out.str();
        } else if constexpr (std::is_same_v<T, cql_blob>) {
            std::ostringstream out;
            out << "0x" << std::hex << std::setfill('0');
            for (const auto b: held) out << std::setw(2) << static_cast<int>(b);
            return out.str();
        } else {
            return std::to_string(held);
        }
    }, value_);
}

const sc::cql_value &sc::cql_row::operator[](const std::string_view name) const {
    for (std::size_t i = 0; i < columns_->size(); ++i) {
        if ((*columns_)[i].name == name) return values_.at(i);
    }
    throw std::out_of_range("CQL: the result has no column " + std::string{name});
}

class sc::cassandra::implementation {
public:
    implementation(ip_endpoints contact_points, cassandra_options options)
        : contact_points_(std::move(contact_points)), options_(std::move(options)) {
        if (contact_points_.empty()) throw std::invalid_argument("Cassandra: no contact points");
        for (auto &each: contact_points_) {
            if (each.port == 0) each.port = 9042;
            if (each.host.empty() || each.port <= 0 || each.port > 65535) {
                throw std::invalid_argument("Cassandra contact points need a host and a port from 1 to 65535");
            }
        }
        const std::lock_guard lock{mutex_};
        connect();
    }

    cql_rows query(const std::string_view cql, const std::vector<cql_value> &values, const cql_consistency consistency) {
        const std::lock_guard lock{mutex_};
        try {
            if (!connection_) connect();
            return run(cql, values, consistency);
        } catch (const connection_lost &) {
            connection_.reset(); // once more on a new connection: it may only have gone stale
            connect();
            try {
                return run(cql, values, consistency);
            } catch (const connection_lost &) {
                connection_.reset();
                throw;
            }
        }
    }

    [[nodiscard]] cql_consistency consistency() const { return options_.consistency; }

    std::string release_version() const {
        const std::lock_guard lock{mutex_};
        return release_version_;
    }

    ip_endpoint connected_to() const {
        const std::lock_guard lock{mutex_};
        return connected_to_;
    }

private:
    // The first contact point that answers and takes the login; throws cassandra_unavailable
    // naming each failure, or the cql_error of a refused login.
    void connect() {
        std::string failures;
        for (const auto &endpoint: contact_points_) {
            try {
                connection_ = std::make_unique<connection>(endpoint, options_.connect_timeout, options_.request_timeout);
                start();
                connected_to_ = endpoint;
                if (!options_.keyspace.empty()) {
                    std::string quoted = options_.keyspace;
                    for (std::size_t at = quoted.find('"'); at != std::string::npos; at = quoted.find('"', at + 2)) quoted.insert(at, 1, '"');
                    (void) run("USE \"" + quoted + "\"", {}, options_.consistency);
                }
                const auto local = run("SELECT release_version FROM system.local", {}, cql_consistency::one);
                release_version_ = local.empty() || local[0][0].is_null() ? std::string{} : local[0][0].as<std::string>();
                return;
            } catch (const connection_lost &error) {
                connection_.reset();
                failures += (failures.empty() ? "" : "; ") + std::string{error.what()};
            } catch (...) {
                connection_.reset();
                throw;
            }
        }
        throw cassandra_unavailable("Cassandra: no contact point answered (" + failures + ")");
    }

    // STARTUP, and the login when the server asks for one.
    void start() {
        writer startup;
        startup.string_map({{"CQL_VERSION", "3.0.0"}});
        auto reply = request(opcode::startup, startup.data());
        if (reply.code == opcode::ready) return;
        if (reply.code != opcode::authenticate) throw std::runtime_error("CQL: an unexpected reply to STARTUP");
        if (options_.user.empty()) throw cql_error(0x0100, "Cassandra asks for a login, and none is configured");
        writer login; // SASL PLAIN, as PasswordAuthenticator takes it
        std::string token;
        token += '\0';
        token += options_.user;
        token += '\0';
        token += options_.password;
        login.bytes(token);
        reply = request(opcode::auth_response, login.data());
        if (reply.code != opcode::auth_success) throw std::runtime_error("CQL: an unexpected reply to the login");
    }

    cql_rows run(const std::string_view cql, const std::vector<cql_value> &values, const cql_consistency consistency) {
        std::vector<std::optional<std::string>> bound;
        bound.reserve(values.size());
        for (const auto &each: values) bound.push_back(encode(each));

        std::shared_ptr<std::vector<cql_column>> columns;
        std::vector<cql_row> rows;
        std::optional<std::string> paging_state;
        do {
            writer body;
            body.long_string(cql);
            body.short_(static_cast<std::uint16_t>(consistency));
            std::uint8_t flags = query_page_size;
            if (!bound.empty()) flags |= query_values;
            if (paging_state) flags |= query_paging_state;
            body.byte(flags);
            if (!bound.empty()) {
                body.short_(static_cast<std::uint16_t>(bound.size()));
                for (const auto &each: bound) {
                    if (each) body.bytes(std::string_view{*each});
                    else body.bytes(std::nullopt);
                }
            }
            body.int_(5000); // rows a page
            if (paging_state) body.bytes(std::string_view{*paging_state});

            const auto reply = request(opcode::query, body.data());
            if (reply.code != opcode::result) throw std::runtime_error("CQL: an unexpected reply to QUERY");
            reader in{reply.body};
            const auto kind = static_cast<result_kind>(in.int_());
            if (kind != result_kind::rows) return {};
            paging_state.reset();

            const auto meta = in.int_();
            const auto count = in.int_();
            if (meta & rows_has_more_pages) {
                if (const auto state = in.bytes()) paging_state = std::string{*state};
            }
            if (!(meta & rows_no_metadata)) {
                auto read = std::make_shared<std::vector<cql_column>>();
                std::string keyspace, table;
                if (meta & rows_global_table_spec) {
                    keyspace = in.string();
                    table = in.string();
                }
                for (int i = 0; i < count; ++i) {
                    cql_column column{keyspace, table, {}, cql_type::blob};
                    if (!(meta & rows_global_table_spec)) {
                        column.keyspace = in.string();
                        column.table = in.string();
                    }
                    column.name = in.string();
                    column.type = read_type(in);
                    read->push_back(std::move(column));
                }
                if (!columns) columns = std::move(read);
            }
            if (!columns) throw std::runtime_error("CQL: rows without their columns");
            for (auto remaining = in.int_(); remaining > 0; --remaining) {
                std::vector<cql_value> cells;
                cells.reserve(columns->size());
                for (const auto &column: *columns) cells.push_back(decode(column.type, in.bytes()));
                rows.emplace_back(columns, std::move(cells));
            }
        } while (paging_state);
        return {columns, std::move(rows)};
    }

    // Sends one request and waits for its reply (EVENT frames, unasked for, are skipped);
    // an ERROR reply is thrown as cql_error.
    frame request(const opcode code, const std::string &body) {
        stream_ = static_cast<std::int16_t>(stream_ >= 0x7FFF ? 1 : stream_ + 1);
        writer header;
        header.byte(protocol_version);
        header.byte(0); // no compression, no tracing
        header.short_(static_cast<std::uint16_t>(stream_));
        header.byte(static_cast<std::uint8_t>(code));
        header.int_(static_cast<std::int32_t>(body.size()));
        connection_->send(header.data() + body);

        while (true) {
            const auto head = connection_->receive(9);
            reader in{head};
            const auto version = in.byte();
            (void) in.byte(); // flags
            const auto stream = static_cast<std::int16_t>(in.short_());
            const auto reply_code = static_cast<opcode>(in.byte());
            const auto length = in.int_();
            if (version != (protocol_version | response_flag)) {
                throw connection_lost("CQL: the server speaks protocol version " + std::to_string(version & 0x7F) +
                                      ", not 4");
            }
            if (length < 0 || length > 256 * 1024 * 1024) throw connection_lost("CQL: a reply of " + std::to_string(length) + " bytes");
            auto reply_body = length ? connection_->receive(static_cast<std::size_t>(length)) : std::string{};
            if (stream != stream_) continue; // an EVENT (stream -1), or a late reply to something given up on
            if (reply_code == opcode::error) {
                reader error{reply_body};
                const auto error_code = error.int_();
                throw cql_error(error_code, "Cassandra: " + error.string());
            }
            return {reply_code, std::move(reply_body)};
        }
    }

    ip_endpoints contact_points_;
    cassandra_options options_;
    mutable std::mutex mutex_;
    std::unique_ptr<connection> connection_;
    std::int16_t stream_ = 0;
    std::string release_version_;
    ip_endpoint connected_to_;
};

sc::cassandra::cassandra(ip_endpoints contact_points) : cassandra(std::move(contact_points), cassandra_options{}) {
}

sc::cassandra::cassandra(ip_endpoints contact_points, cassandra_options options)
    : implementation_(std::make_unique<implementation>(std::move(contact_points), std::move(options))) {
}

sc::cassandra::~cassandra() = default;

sc::cql_rows sc::cassandra::query(const std::string_view cql, const std::vector<cql_value> &values) const {
    return implementation_->query(cql, values, implementation_->consistency());
}

sc::cql_rows sc::cassandra::query(const std::string_view cql, const std::vector<cql_value> &values,
                                  const cql_consistency consistency) const {
    return implementation_->query(cql, values, consistency);
}

std::string sc::cassandra::release_version() const {
    return implementation_->release_version();
}

sc::ip_endpoint sc::cassandra::connected_to() const {
    return implementation_->connected_to();
}
