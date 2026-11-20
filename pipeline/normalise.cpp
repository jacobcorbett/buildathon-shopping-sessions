// Validate one physical line at a time. A malformed quote cannot consume
// the next line, and a carriage return is always part of the field.
#include <array>
#include <fstream>
#include <iostream>
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
            const bool quote = field.find('\r') != field.npos;
            if (quote) {
                buffer.push_back('"');
            }
            buffer.append(field);
            if (quote) {
                buffer.push_back('"');
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
