// Validate one physical line at a time. A malformed quote cannot consume
// the next line, and a carriage return is always part of the field.
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {
bool split(std::string_view line, unsigned width,
           std::array<std::string_view, 7>& fields) {
    for (unsigned column = 0; column < width; ++column) {
        const auto comma = line.find(',');
        if ((comma == line.npos) != (column + 1 == width)) {
            return false;
        }
        auto field = line.substr(0, comma);
        if (field.size() >= 2 && field.front() == '"' && field.back() == '"') {
            field.remove_prefix(1);
            field.remove_suffix(1);
        }
        if (field.find('"') != field.npos) {
            return false;
        }
        fields[column] = field;
        if (comma != line.npos) {
            line.remove_prefix(comma + 1);
        }
    }
    return true;
}

std::optional<int64_t> unsigned_integer(std::string_view field) {
    if (field.empty() || field.front() < '0' || field.front() > '9') {
        return std::nullopt;
    }
    int64_t value;
    const auto end = field.data() + field.size();
    const auto result = std::from_chars(field.data(), end, value);
    if (result.ec != std::errc{} || result.ptr != end) {
        return std::nullopt;
    }
    return value;
}

int pair_at(std::string_view field, unsigned offset) {
    const int tens = field[offset] - '0';
    const int ones = field[offset + 1] - '0';
    if (tens < 0 || tens > 9 || ones < 0 || ones > 9) {
        return -1;
    }
    return tens * 10 + ones;
}

std::optional<int64_t> timestamp(std::string_view field) {
    if ((field.size() != 20 && field.size() != 25) || field[4] != '-') {
        return unsigned_integer(field);
    }
    if (field[7] != '-' || field[10] != 'T' || field[13] != ':' || field[16] != ':') {
        return std::nullopt;
    }
    const int century = pair_at(field, 0);
    const int short_year = pair_at(field, 2);
    const int month = pair_at(field, 5);
    const int day = pair_at(field, 8);
    const int hour = pair_at(field, 11);
    const int minute = pair_at(field, 14);
    const int second = pair_at(field, 17);
    if (century < 0 || short_year < 0 || month < 1 || month > 12 || day < 1 ||
        day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59) {
        return std::nullopt;
    }
    const auto date = std::chrono::year(century * 100 + short_year)
        / std::chrono::month(month) / std::chrono::day(day);
    if (!date.ok()) {
        return std::nullopt;
    }
    int offset = 0;
    if (field.size() == 20) {
        if (field[19] != 'Z') {
            return std::nullopt;
        }
    } else {
        const int offset_hour = pair_at(field, 20);
        const int offset_minute = pair_at(field, 23);
        if ((field[19] != '+' && field[19] != '-') || field[22] != ':' ||
            offset_hour < 0 || offset_hour > 23 || offset_minute < 0 || offset_minute > 59) {
            return std::nullopt;
        }
        offset = (field[19] == '+' ? 1 : -1) * (offset_hour * 60 + offset_minute);
    }
    return std::chrono::sys_days(date).time_since_epoch().count() * 86400000LL
        + ((hour * 60 + minute - offset) * 60 + second) * 1000LL;
}

void append_number(std::string& buffer, std::optional<int64_t> value) {
    if (value) {
        char digits[24];
        const auto result = std::to_chars(digits, digits + sizeof(digits), *value);
        buffer.append(digits, result.ptr);
    }
}

void append_field(std::string& buffer, std::string_view field) {
    // Quoting also preserves an initial UTF-8 BOM as data in a headerless shard.
    const bool quote = field.find('\r') != field.npos || field.starts_with("\xef\xbb\xbf");
    if (quote) {
        buffer.push_back('"');
    }
    buffer.append(field);
    if (quote) {
        buffer.push_back('"');
    }
}

void normalise(unsigned width, const char* source, const char* destination) {
    std::ifstream input(source, std::ios::binary);
    std::ofstream output(destination, std::ios::binary);
    if (!input || !output) {
        throw std::runtime_error("could not open input or output");
    }
    std::string line;
    std::string buffer;
    buffer.reserve(1 << 20);
    std::array<std::string_view, 7> fields;
    std::getline(input, line);  // All input files have a header.
    while (std::getline(input, line)) {
        if (!split(line, width, fields)) {
            continue;
        }
        for (unsigned column = 0; column < width; ++column) {
            if (column != 0) {
                buffer.push_back(',');
            }
            const auto field = fields[column];
            // Events have seven fields, clicks four; the other inputs are text.
            if ((width == 7 && (column == 2 || column == 3)) ||
                (width == 4 && column == 3)) {
                append_number(buffer, timestamp(field));
            } else if (width == 7 && column == 6) {
                if (fields[4] == "purchase") {
                    append_number(buffer, unsigned_integer(field));
                }
            } else {
                append_field(buffer, field);
            }
        }
        buffer.push_back('\n');
        if (buffer.size() >= (1 << 20)) {
            output.write(buffer.data(), buffer.size());
            buffer.clear();
        }
    }
    output.write(buffer.data(), buffer.size());
    output.close();
    if (input.bad() || !output) {
        throw std::runtime_error("failed reading input or writing output");
    }
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: normalise WIDTH SOURCE DESTINATION\n";
        return 1;
    }
    try {
        const auto width = std::stoul(argv[1]);
        if (width < 2 || width > 7) {
            throw std::runtime_error("invalid CSV width");
        }
        normalise(width, argv[2], argv[3]);
    } catch (const std::exception& error) {
        std::cerr << argv[2] << ": " << error.what() << '\n';
        return 1;
    }
}
